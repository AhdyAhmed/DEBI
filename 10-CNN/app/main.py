"""
Cats vs Dogs — FastAPI Inference App
=====================================
Run:  uvicorn main:app --reload --host 0.0.0.0 --port 8000
Docs: http://localhost:8000/docs
"""

import io
import time
import numpy as np
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from PIL import Image
import tensorflow as tf

# ── Config ────────────────────────────────────────────────────────────────────
# Always resolves to the same folder as main.py, regardless of where you
# launch uvicorn from (e.g. uvicorn catdog_api.main:app still works).
MODEL_PATH = Path(__file__).parent / "best_custom_cnn.keras"
IMG_SIZE   = 128                              # must match training size
CLASS_NAMES = ["cat", "dog"]

# ── GPU setup (use if available, fall back to CPU silently) ───────────────────
gpus = tf.config.list_physical_devices("GPU")
if gpus:
    for gpu in gpus:
        tf.config.experimental.set_memory_growth(gpu, True)

# ── Global model holder ───────────────────────────────────────────────────────
model: tf.keras.Model | None = None


# ── Lifespan: load model once at startup, release at shutdown ─────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global model
    if not MODEL_PATH.exists():
        raise RuntimeError(
            f"Model file not found: {MODEL_PATH.resolve()}\n"
            "Copy your 'best_custom_cnn.keras' next to main.py and restart."
        )
    print(f"Loading model from {MODEL_PATH} ...")
    model = tf.keras.models.load_model(str(MODEL_PATH))
    model.predict(np.zeros((1, IMG_SIZE, IMG_SIZE, 3)))  # warm-up inference
    print("Model ready ✅")
    yield
    print("Shutting down — releasing model.")
    del model


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="🐱🐶 Cats vs Dogs Classifier",
    description=(
        "Upload any image and the custom CNN will tell you "
        "whether it's a **cat** or a **dog**, with confidence score."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Helper ────────────────────────────────────────────────────────────────────
def preprocess_image(image_bytes: bytes) -> np.ndarray:
    """Decode bytes → PIL → resize → normalize → numpy batch."""
    try:
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    except Exception:
        raise HTTPException(status_code=422, detail="Could not decode image. Send a valid JPG/PNG/WEBP.")

    img = img.resize((IMG_SIZE, IMG_SIZE), Image.BILINEAR)
    arr = np.array(img, dtype=np.float32) / 255.0
    return np.expand_dims(arr, axis=0)          # (1, 128, 128, 3)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    """Health check — confirm the API is alive."""
    return {
        "status": "ok",
        "message": "Cats vs Dogs inference API is running.",
        "model_loaded": model is not None,
    }


@app.get("/info", tags=["Health"])
def info():
    """Return model and runtime info."""
    return {
        "model_path":   str(MODEL_PATH),
        "image_size":   f"{IMG_SIZE}x{IMG_SIZE}",
        "classes":      CLASS_NAMES,
        "gpu_available": len(gpus) > 0,
        "gpu_devices":  [g.name for g in gpus],
        "tf_version":   tf.__version__,
    }


@app.post("/predict", tags=["Inference"])
async def predict(file: UploadFile = File(..., description="JPG / PNG / WEBP image")):
    """
    Classify an uploaded image as **cat** or **dog**.

    Returns:
    - `label`       — predicted class name
    - `confidence`  — probability of the predicted class (0–1)
    - `probabilities` — raw scores for both classes
    - `inference_ms`  — server-side inference time in milliseconds
    """
    # Validate MIME type
    allowed = {"image/jpeg", "image/png", "image/webp", "image/jpg"}
    if file.content_type not in allowed:
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported file type '{file.content_type}'. Send JPG, PNG, or WEBP.",
        )

    image_bytes = await file.read()

    # Preprocess
    batch = preprocess_image(image_bytes)

    # Inference
    t0   = time.perf_counter()
    prob = float(model.predict(batch, verbose=0)[0][0])   # sigmoid output → dog probability
    ms   = round((time.perf_counter() - t0) * 1000, 2)

    cat_prob = round(1 - prob, 4)
    dog_prob = round(prob, 4)
    predicted_idx = int(prob > 0.5)
    confidence    = dog_prob if predicted_idx == 1 else cat_prob

    return JSONResponse({
        "label":        CLASS_NAMES[predicted_idx],
        "confidence":   round(confidence, 4),
        "probabilities": {
            "cat": cat_prob,
            "dog": dog_prob,
        },
        "inference_ms": ms,
        "filename":     file.filename,
    })


@app.post("/predict/batch", tags=["Inference"])
async def predict_batch(files: list[UploadFile] = File(..., description="Up to 16 images")):
    """
    Classify multiple images in one request (max 16).

    Returns a list of predictions in the same order as the uploaded files.
    """
    if len(files) > 16:
        raise HTTPException(status_code=400, detail="Maximum 16 images per batch request.")

    batches, filenames = [], []
    for f in files:
        raw = await f.read()
        batches.append(preprocess_image(raw)[0])    # strip batch dim
        filenames.append(f.filename)

    batch = np.stack(batches, axis=0)               # (N, 128, 128, 3)

    t0    = time.perf_counter()
    probs = model.predict(batch, verbose=0).flatten()
    ms    = round((time.perf_counter() - t0) * 1000, 2)

    results = []
    for fname, prob in zip(filenames, probs):
        prob      = float(prob)
        cat_prob  = round(1 - prob, 4)
        dog_prob  = round(prob, 4)
        pred_idx  = int(prob > 0.5)
        confidence = dog_prob if pred_idx == 1 else cat_prob
        results.append({
            "filename":   fname,
            "label":      CLASS_NAMES[pred_idx],
            "confidence": round(confidence, 4),
            "probabilities": {"cat": cat_prob, "dog": dog_prob},
        })

    return JSONResponse({"predictions": results, "total_inference_ms": ms})


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)