Source image verification uses the system tesseract-ocr and
tesseract-ocr-chi-sim packages (no Python OCR dependency).
Media preparation refuses unverifiable or contact-bearing sources and preserves
the entire accepted image. Cached decisions are keyed by source content and
SCRUB_REVISION. No source photo is cropped or repainted.
