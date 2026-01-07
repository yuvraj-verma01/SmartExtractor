from pdf2image import convert_from_path
import pytesseract
import cv2
import numpy as np
from pathlib import Path

# Explicit paths (safe even though PATH works)
TESSERACT_EXE = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
POPPLER_BIN = r"C:\Users\yuvra\AppData\Local\Programs\MiKTeX\miktex\bin\x64"


pytesseract.pytesseract.tesseract_cmd = TESSERACT_EXE


def preprocess(pil_img):
    img = np.array(pil_img)

    if img.ndim == 3:
        img = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)

    # Denoise
    img = cv2.fastNlMeansDenoising(img, h=30)

    # Adaptive binarization
    img = cv2.adaptiveThreshold(
        img,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        31,
        10,
    )
    return img


def ocr_pdf(pdf_path: Path, out_dir: Path) -> Path:
    pages = convert_from_path(
        pdf_path,
        dpi=300,
        poppler_path=POPPLER_BIN
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    chunks = []

    for i, page in enumerate(pages):
        img = preprocess(page)
        text = pytesseract.image_to_string(img, config="--psm 6")
        chunks.append(f"\n--- PAGE {i+1} ---\n{text}")

    out_file = out_dir / f"{pdf_path.stem}_ocr.txt"
    out_file.write_text("".join(chunks), encoding="utf-8")
    return out_file
