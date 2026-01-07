# Lease extractor by Yuvraj Verma

Local-only app that wraps the existing extraction pipeline and forces a full human review on every field.

## Prerequisites
- Python 3.10+ with the pipeline dependencies installed.
- Optional: Ollama running locally with the model `qwen2.5:7b-instruct-q4_K_M` for the LLM fallback stage.

## Backend
From the repo root:

```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
pip install -r app\backend\requirements.txt
python -m uvicorn app.backend.main:app --reload --port 8000
```

If PowerShell blocks activation, either run:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Or skip activation and use the venv Python directly:

```bash
.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python -m pip install -r app\backend\requirements.txt
.\.venv\Scripts\python -m uvicorn app.backend.main:app --reload --port 8000
```

## Frontend
In a separate terminal:

```bash
cd app\frontend
npm install
npm run dev
```

Open `http://localhost:5173`.

## Usage
1. Create a job (upload a PDF or create an empty job).
2. Run the pipeline.
3. Review every field and mark each one reviewed.
4. Save changes as needed and add the job to Excel when review is complete.
5. Download JSON/XLSX.

## Notes
- Jobs are stored in `app/jobs/<job_id>/` and are gitignored.
- Excel exports are stored at `app/export/lease_jobs.xlsx`.
- The pipeline outputs are copied into the job's `stage1/`, `stage2/`, and `stage3/` folders unchanged.
- If Ollama or the model is unavailable, the UI shows an "LLM unavailable" banner and the job metadata records `llm_status="unavailable"`.
- To point the frontend at a different backend URL, set `VITE_API_URL`.
