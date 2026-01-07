from pathlib import Path
from ocr.ocr_engine import ocr_pdf

if __name__ == "__main__":
    pdf = Path("data/input_pdfs/lease.pdf")
    out = ocr_pdf(pdf, Path("data/ocr_text"))
    print("OCR output written to:", out)
