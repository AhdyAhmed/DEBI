"""
Mask Detection FastAPI — Live video via OpenCV (CPU-optimised)
"""
import os

# ── Silence TensorFlow noise before any TF import ────────────────────────────
os.environ["TF_CPP_MIN_LOG_LEVEL"]      = "3"
os.environ["TF_ENABLE_ONEDNN_OPTS"]     = "0"
os.environ["CUDA_VISIBLE_DEVICES"]      = "-1"   # force CPU, ignore GPU
os.environ["TF_FORCE_GPU_ALLOW_GROWTH"] = "false"

import io
import time
import threading
import logging
import warnings
warnings.filterwarnings("ignore")

import cv2
import numpy as np
import tensorflow as tf

# Keep TF CPU threads sane (tune to your core count)
tf.config.threading.set_intra_op_parallelism_threads(4)
tf.config.threading.set_inter_op_parallelism_threads(2)

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pathlib import Path

# ── Clean logger ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("mask_api")

# Suppress noisy third-party loggers
for noisy in ("absl", "tensorflow", "h5py", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.ERROR)

# ── Configuration ─────────────────────────────────────────────────────────────
IMG_SIZE   = (128, 128)
CLASSES    = ["with_mask", "without_mask"]
THRESHOLD  = 0.5
MODEL_PATH = "best_model.keras"

# ── Globals ───────────────────────────────────────────────────────────────────
model:  tf.keras.Model | None    = None
camera: cv2.VideoCapture | None  = None
camera_lock = threading.Lock()

# Warm-up flag — first predict() call triggers XLA compilation; we do it once
_model_warmed_up = False

# ── Face detector ─────────────────────────────────────────────────────────────
face_cascade = cv2.CascadeClassifier(
    cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
)


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global model, _model_warmed_up

    if Path(MODEL_PATH).exists():
        logger.info(f"Loading model → {MODEL_PATH}")
        model = tf.keras.models.load_model(MODEL_PATH, compile=False)

        # Warm-up: run one dummy inference so XLA compiles before the first
        # real request — eliminates the ~2 s stall on the first frame.
        logger.info("Warming up model on CPU …")
        dummy = np.zeros((1, *IMG_SIZE, 3), dtype="float32")
        model.predict(dummy, verbose=0)
        _model_warmed_up = True
        logger.info("Model ready ✓  (CPU inference, XLA compiled)")
    else:
        logger.warning(
            f"'{MODEL_PATH}' not found. "
            "Upload via POST /upload-model or place it next to main.py."
        )

    yield

    # ── Shutdown ──────────────────────────────────────────────────────────────
    if camera and camera.isOpened():
        camera.release()
        logger.info("Camera released ✓")


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Mask Detection API",
    description="Real-time face-mask detection — custom CNN, CPU inference, OpenCV stream.",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ══════════════════════════════════════════════════════════════════════════════
# Inference helpers
# ══════════════════════════════════════════════════════════════════════════════

def preprocess_face(face_bgr: np.ndarray) -> np.ndarray:
    """BGR crop → normalised float32 array ready for the model."""
    rgb     = cv2.cvtColor(face_bgr, cv2.COLOR_BGR2RGB)
    resized = cv2.resize(rgb, IMG_SIZE, interpolation=cv2.INTER_LINEAR)
    return (resized.astype("float32") / 255.0)


def predict_faces_batch(face_crops: list[np.ndarray]) -> list[tuple[str, float]]:
    """
    Run a single batched model.predict() for all detected faces in one frame.
    Much faster on CPU than calling predict() once per face.
    Returns list of (label, confidence) tuples.
    """
    if model is None or not face_crops:
        return []

    batch = np.stack([preprocess_face(c) for c in face_crops])   # (N, 128, 128, 3)
    probs = model.predict(batch, verbose=0).flatten()             # (N,)

    results = []
    for prob in probs:
        label      = CLASSES[int(prob > THRESHOLD)]
        confidence = float(prob) if label == "without_mask" else float(1.0 - prob)
        results.append((label, round(confidence, 4)))
    return results


def annotate_frame(frame: np.ndarray) -> tuple[np.ndarray, list[dict]]:
    """
    1. Detect faces with Haar cascade.
    2. Batch-predict all faces in one model call.
    3. Draw bounding boxes + labels.
    Returns (annotated_frame, detections_list).
    """
    gray  = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    faces = face_cascade.detectMultiScale(
        gray,
        scaleFactor=1.1,
        minNeighbors=5,
        minSize=(50, 50),
        flags=cv2.CASCADE_SCALE_IMAGE,
    )

    if not isinstance(faces, np.ndarray) or len(faces) == 0:
        return frame, []

    # ── Crop each face (with small padding) ───────────────────────────────────
    crops, coords = [], []
    for (x, y, w, h) in faces:
        pad = int(0.10 * min(w, h))
        x1  = max(0, x - pad)
        y1  = max(0, y - pad)
        x2  = min(frame.shape[1], x + w + pad)
        y2  = min(frame.shape[0], y + h + pad)
        crops.append(frame[y1:y2, x1:x2])
        coords.append((x1, y1, x2, y2))

    # ── One batched predict call ──────────────────────────────────────────────
    predictions = predict_faces_batch(crops)

    # ── Draw annotations ─────────────────────────────────────────────────────
    detections = []
    for (x1, y1, x2, y2), (label, conf) in zip(coords, predictions):
        color = (34, 197, 94) if label == "with_mask" else (59, 28, 255)
        tag   = f"MASK  {conf:.0%}" if label == "with_mask" else f"NO MASK  {conf:.0%}"

        # Box
        cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)

        # Label pill
        font       = cv2.FONT_HERSHEY_SIMPLEX
        font_scale = 0.60
        thickness  = 2
        (tw, th), baseline = cv2.getTextSize(tag, font, font_scale, thickness)
        pad_x, pad_y = 6, 4
        rx1 = x1
        ry1 = y1 - th - baseline - pad_y * 2
        rx2 = x1 + tw + pad_x * 2
        ry2 = y1
        # clamp if near top edge
        if ry1 < 0:
            ry1, ry2 = y2, y2 + th + baseline + pad_y * 2

        cv2.rectangle(frame, (rx1, ry1), (rx2, ry2), color, -1)
        cv2.putText(
            frame, tag,
            (rx1 + pad_x, ry2 - baseline - pad_y),
            font, font_scale, (255, 255, 255), thickness,
        )

        detections.append({
            "label":      label,
            "confidence": conf,
            "box":        {"x": x1, "y": y1, "w": x2 - x1, "h": y2 - y1},
        })

    return frame, detections


# ══════════════════════════════════════════════════════════════════════════════
# Camera helpers
# ══════════════════════════════════════════════════════════════════════════════

def get_camera() -> cv2.VideoCapture:
    global camera
    with camera_lock:
        if camera is None or not camera.isOpened():
            logger.info("Opening camera 0 …")
            cap = cv2.VideoCapture(0, cv2.CAP_DSHOW)   # CAP_DSHOW = faster on Windows
            if not cap.isOpened():
                cap = cv2.VideoCapture(0)               # fallback
            if not cap.isOpened():
                raise HTTPException(status_code=503, detail="Cannot open camera.")
            cap.set(cv2.CAP_PROP_FRAME_WIDTH,  640)
            cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
            cap.set(cv2.CAP_PROP_FPS, 30)
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)         # minimise latency
            camera = cap
            logger.info("Camera opened ✓")
        return camera


def release_camera_safe():
    global camera
    with camera_lock:
        if camera and camera.isOpened():
            camera.release()
            camera = None
            logger.info("Camera released.")


# ══════════════════════════════════════════════════════════════════════════════
# MJPEG stream generator
# ══════════════════════════════════════════════════════════════════════════════

def frame_generator():
    """Yield annotated JPEG frames as multipart MJPEG boundary stream."""
    cap = get_camera()

    fps_target   = 25          # cap output frame rate
    frame_period = 1.0 / fps_target
    t_last       = 0.0

    while True:
        # Throttle to fps_target
        now = time.perf_counter()
        if now - t_last < frame_period:
            time.sleep(0.001)
            continue
        t_last = now

        with camera_lock:
            ok, frame = cap.read()

        if not ok:
            logger.warning("Camera read failed — reconnecting …")
            time.sleep(0.5)
            release_camera_safe()
            try:
                cap = get_camera()
            except HTTPException:
                break
            continue

        annotated, _ = annotate_frame(frame)

        _, buf = cv2.imencode(
            ".jpg", annotated,
            [cv2.IMWRITE_JPEG_QUALITY, 80],
        )

        yield (
            b"--frame\r\n"
            b"Content-Type: image/jpeg\r\n\r\n"
            + buf.tobytes()
            + b"\r\n"
        )


# ══════════════════════════════════════════════════════════════════════════════
# Routes
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse, tags=["UI"])
async def dashboard():
    """Serve the live dashboard."""
    return HTMLResponse(Path("static/index.html").read_text(encoding="utf-8"))


@app.get("/video-feed", tags=["Stream"])
async def video_feed():
    """
    MJPEG stream of the annotated webcam feed.
    Embed with: `<img src="/video-feed" />`
    """
    return StreamingResponse(
        frame_generator(),
        media_type="multipart/x-mixed-replace; boundary=frame",
    )


@app.post("/predict/image", tags=["Inference"])
async def predict_image_annotated(file: UploadFile = File(...)):
    """Upload a JPEG/PNG → receive the annotated image."""
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded.")

    data  = await file.read()
    arr   = np.frombuffer(data, np.uint8)
    frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if frame is None:
        raise HTTPException(status_code=400, detail="Cannot decode image.")

    annotated, detections = annotate_frame(frame)
    _, buf = cv2.imencode(".jpg", annotated, [cv2.IMWRITE_JPEG_QUALITY, 92])

    return StreamingResponse(
        io.BytesIO(buf.tobytes()),
        media_type="image/jpeg",
        headers={"X-Detections": str(len(detections))},
    )





@app.get("/snapshot", tags=["Stream"])
async def snapshot():
    """Grab one annotated frame from the live camera and return it as JPEG."""
    cap = get_camera()
    with camera_lock:
        ok, frame = cap.read()
    if not ok:
        raise HTTPException(status_code=503, detail="Failed to capture frame.")

    annotated, detections = annotate_frame(frame)
    _, buf = cv2.imencode(".jpg", annotated, [cv2.IMWRITE_JPEG_QUALITY, 92])

    return StreamingResponse(
        io.BytesIO(buf.tobytes()),
        media_type="image/jpeg",
        headers={"X-Detections": str(len(detections))},
    )


@app.post("/camera/release", tags=["Camera"])
async def release_cam():
    """Manually release the webcam."""
    release_camera_safe()
    return {"status": "camera released"}


@app.post("/upload-model", tags=["Admin"])
async def upload_model(file: UploadFile = File(...)):
    """
    Hot-swap the Keras model at runtime — no server restart needed.
    Saves the file as `best_model.keras` and reloads it immediately.
    """
    global model, _model_warmed_up

    dest = Path(MODEL_PATH)
    dest.write_bytes(await file.read())
    logger.info(f"Model file saved → {dest} ({dest.stat().st_size / 1e6:.1f} MB)")

    model = tf.keras.models.load_model(str(dest), compile=False)

    logger.info("Warming up new model …")
    model.predict(np.zeros((1, *IMG_SIZE, 3), dtype="float32"), verbose=0)
    _model_warmed_up = True
    logger.info("New model ready ✓")

    return {"status": "model loaded", "path": str(dest)}


@app.get("/status", tags=["Admin"])
async def status():
    """Health check — returns model and camera state."""
    return {
        "model_loaded":   model is not None,
        "model_warmed_up": _model_warmed_up,
        "model_path":     MODEL_PATH,
        "img_size":       IMG_SIZE,
        "classes":        CLASSES,
        "threshold":      THRESHOLD,
        "camera_open":    camera is not None and camera.isOpened(),
        "device":         "CPU",
    }

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
#http://localhost:8000/video-feed