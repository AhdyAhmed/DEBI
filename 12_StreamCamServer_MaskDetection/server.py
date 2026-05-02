"""
Mask Detection Flask-SocketIO App
Receives webcam frames via WebSocket, runs mask detection, streams annotated frames back.
Saves every received frame to the ./frames/ directory.
"""
import os
import ssl
import time
import base64
import logging
import warnings
import threading

os.environ["TF_CPP_MIN_LOG_LEVEL"]      = "3"
os.environ["TF_ENABLE_ONEDNN_OPTS"]     = "0"
os.environ["CUDA_VISIBLE_DEVICES"]      = "-1"
os.environ["TF_FORCE_GPU_ALLOW_GROWTH"] = "false"

warnings.filterwarnings("ignore")

import cv2
import numpy as np
import tensorflow as tf

tf.config.threading.set_intra_op_parallelism_threads(4)
tf.config.threading.set_inter_op_parallelism_threads(2)

from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
from pathlib import Path
from werkzeug.utils import secure_filename

# ── Logger ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("mask_flask")

for noisy in ("absl", "tensorflow", "h5py", "urllib3", "werkzeug"):
    logging.getLogger(noisy).setLevel(logging.ERROR)

# ── Config ────────────────────────────────────────────────────────────────────
IMG_SIZE    = (128, 128)
CLASSES     = ["with_mask", "without_mask"]
THRESHOLD   = 0.5
MODEL_PATH  = "best_model.keras"
FRAMES_DIR  = Path("frames")

# ── Globals ───────────────────────────────────────────────────────────────────
model: tf.keras.Model | None = None
model_lock   = threading.Lock()
_model_ready = False

# ── Face detector ─────────────────────────────────────────────────────────────
face_cascade = cv2.CascadeClassifier(
    cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
)

# ── Flask & SocketIO ──────────────────────────────────────────────────────────
app = Flask(__name__)
app.config["SECRET_KEY"] = "mask-detection-secret"
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024  # 100 MB upload limit
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")


# ══════════════════════════════════════════════════════════════════════════════
# Model helpers
# ══════════════════════════════════════════════════════════════════════════════

def load_model_from_path(path: str) -> None:
    """Load (or hot-swap) the Keras model and warm it up."""
    global model, _model_ready
    logger.info(f"Loading model → {path}")
    m = tf.keras.models.load_model(path, compile=False)
    logger.info("Warming up model …")
    m.predict(np.zeros((1, *IMG_SIZE, 3), dtype="float32"), verbose=0)
    with model_lock:
        model        = m
        _model_ready = True
    logger.info("Model ready ✓  (CPU inference)")


def preprocess_face(face_bgr: np.ndarray) -> np.ndarray:
    rgb     = cv2.cvtColor(face_bgr, cv2.COLOR_BGR2RGB)
    resized = cv2.resize(rgb, IMG_SIZE, interpolation=cv2.INTER_LINEAR)
    return resized.astype("float32") / 255.0


def predict_faces_batch(face_crops: list[np.ndarray]) -> list[tuple[str, float]]:
    with model_lock:
        m = model
    if m is None or not face_crops:
        return []

    batch = np.stack([preprocess_face(c) for c in face_crops])
    probs = m.predict(batch, verbose=0).flatten()

    results = []
    for prob in probs:
        label      = CLASSES[int(prob > THRESHOLD)]
        confidence = float(prob) if label == "without_mask" else float(1.0 - prob)
        results.append((label, round(confidence, 4)))
    return results


def annotate_frame(frame: np.ndarray) -> tuple[np.ndarray, list[dict]]:
    gray  = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    faces = face_cascade.detectMultiScale(
        gray, scaleFactor=1.1, minNeighbors=5,
        minSize=(50, 50), flags=cv2.CASCADE_SCALE_IMAGE,
    )

    if not isinstance(faces, np.ndarray) or len(faces) == 0:
        return frame, []

    crops, coords = [], []
    for (x, y, w, h) in faces:
        pad = int(0.10 * min(w, h))
        x1  = max(0, x - pad)
        y1  = max(0, y - pad)
        x2  = min(frame.shape[1], x + w + pad)
        y2  = min(frame.shape[0], y + h + pad)
        crops.append(frame[y1:y2, x1:x2])
        coords.append((x1, y1, x2, y2))

    predictions = predict_faces_batch(crops)

    detections = []
    for (x1, y1, x2, y2), (label, conf) in zip(coords, predictions):
        color = (34, 197, 94) if label == "with_mask" else (59, 28, 255)
        tag   = f"MASK  {conf:.0%}" if label == "with_mask" else f"NO MASK  {conf:.0%}"

        cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)

        font       = cv2.FONT_HERSHEY_SIMPLEX
        font_scale = 0.60
        thickness  = 2
        (tw, th), baseline = cv2.getTextSize(tag, font, font_scale, thickness)
        pad_x, pad_y = 6, 4
        rx1 = x1
        ry1 = y1 - th - baseline - pad_y * 2
        rx2 = x1 + tw + pad_x * 2
        ry2 = y1
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
            "confidence": float(conf),
            "box":        {"x": int(x1), "y": int(y1), "w": int(x2 - x1), "h": int(y2 - y1)},
        })

    return frame, detections


# ══════════════════════════════════════════════════════════════════════════════
# Routes
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/status")
def status():
    with model_lock:
        loaded = model is not None
    return jsonify({
        "model_loaded":    loaded,
        "model_warmed_up": _model_ready,
        "model_path":      MODEL_PATH,
        "img_size":        IMG_SIZE,
        "classes":         CLASSES,
        "threshold":       THRESHOLD,
        "device":          "CPU",
    })


@app.route("/upload-model", methods=["POST"])
def upload_model():
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400
    f = request.files["file"]
    if f.filename == "":
        return jsonify({"error": "No file selected"}), 400

    dest = Path(MODEL_PATH)
    f.save(str(dest))
    logger.info(f"Model uploaded → {dest} ({dest.stat().st_size / 1e6:.1f} MB)")

    # Reload in background so the HTTP response isn't blocked by warm-up
    threading.Thread(target=load_model_from_path, args=(str(dest),), daemon=True).start()
    return jsonify({"status": "model loading", "path": str(dest)})


# ══════════════════════════════════════════════════════════════════════════════
# WebSocket events
# ══════════════════════════════════════════════════════════════════════════════

@socketio.on("connect")
def on_connect():
    logger.info(f"Client connected: {request.sid}")
    emit("status", {"model_loaded": _model_ready})


@socketio.on("disconnect")
def on_disconnect():
    logger.info(f"Client disconnected: {request.sid}")


@socketio.on("video_frame")
def handle_video_frame(data):
    """
    Receives a base64-encoded JPEG frame from the browser,
    runs mask detection, and emits back:
      - 'annotated_frame': base64 JPEG of the annotated result
      - 'detections':      list of detection dicts

    Also saves every raw decoded frame to ./frames/ as a JPEG.
    """
    logger.info(f"Frame received from {request.sid} — size: {len(data)} bytes")

    try:
        # ── Decode incoming frame ─────────────────────────────────────────────
        header, encoded = data.split(",", 1) if "," in data else ("", data)
        img_bytes = base64.b64decode(encoded)
        np_img    = np.frombuffer(img_bytes, dtype=np.uint8)
        frame     = cv2.imdecode(np_img, cv2.IMREAD_COLOR)

        if frame is None:
            logger.error("cv2.imdecode returned None — frame could not be decoded")
            emit("error", {"message": "Could not decode frame"})
            return

        logger.info(f"Frame decoded OK — shape: {frame.shape}")

        # ── Check model ───────────────────────────────────────────────────────
        with model_lock:
            m = model
        if m is None:
            logger.warning("Model not loaded — returning raw frame without inference")
            _, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
            b64_result = "data:image/jpeg;base64," + base64.b64encode(buf.tobytes()).decode()
            emit("annotated_frame", {"image": b64_result, "detections": [], "count": 0})
            return

        # ── Annotate ──────────────────────────────────────────────────────────
        annotated, detections = annotate_frame(frame)

        # ── Log detections ────────────────────────────────────────────────────
        if detections:
            summary = ", ".join(f"{d['label']} {d['confidence']:.0%}" for d in detections)
            logger.info(f"Detected {len(detections)} face(s): {summary}")
        else:
            logger.info("No faces detected in frame")

        # ── Save annotated frame to disk (bounding boxes + labels baked in) ──
        frame_path = FRAMES_DIR / f"frame_{time.time_ns()}.jpg"
        cv2.imwrite(str(frame_path), annotated, [cv2.IMWRITE_JPEG_QUALITY, 90])
        logger.debug(f"Saved annotated frame → {frame_path}")

        # ── Encode and emit back ──────────────────────────────────────────────
        _, buf     = cv2.imencode(".jpg", annotated, [cv2.IMWRITE_JPEG_QUALITY, 80])
        b64_result = "data:image/jpeg;base64," + base64.b64encode(buf.tobytes()).decode()

        emit("annotated_frame", {
            "image":      b64_result,
            "detections": detections,
            "count":      len(detections),
        })

    except Exception as e:
        logger.error(f"Frame processing error: {e}", exc_info=True)
        emit("error", {"message": str(e)})


# ══════════════════════════════════════════════════════════════════════════════
# Startup — create frames dir, load model if present
# ══════════════════════════════════════════════════════════════════════════════

def startup():
    FRAMES_DIR.mkdir(exist_ok=True)
    logger.info(f"Frame output directory → {FRAMES_DIR}/")

    if Path(MODEL_PATH).exists():
        load_model_from_path(MODEL_PATH)
    else:
        logger.warning(
            f"'{MODEL_PATH}' not found. "
            "Upload via POST /upload-model or place it next to main.py."
        )


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    startup()

    cert_file = "certificates/certificate.crt"
    key_file  = "certificates/private.key"

    use_ssl = Path(cert_file).exists() and Path(key_file).exists()

    if use_ssl:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(certfile=cert_file, keyfile=key_file)
        logger.info("Starting with SSL on https://0.0.0.0:5000")
        socketio.run(app, host="0.0.0.0", port=5000, ssl_context=ssl_context)
    else:
        logger.warning("SSL certificates not found — running without SSL on http://0.0.0.0:5000")
        socketio.run(app, host="0.0.0.0", port=5000)

#https://localhost:5000
