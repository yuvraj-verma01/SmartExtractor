from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = APP_ROOT.parent
JOBS_ROOT = APP_ROOT / "jobs"
EXPORTS_ROOT = APP_ROOT / "export"


@dataclass(frozen=True)
class JobPaths:
    root: Path
    input_dir: Path
    stage1_dir: Path
    stage2_dir: Path
    stage3_dir: Path
    final_dir: Path
    export_dir: Path
    workspace_dir: Path
    job_meta_path: Path
    working_state_path: Path
    audit_log_path: Path

    @property
    def input_pdf(self) -> Path:
        return self.input_dir / "lease.pdf"

    @property
    def workspace_data(self) -> Path:
        return self.workspace_dir / "data"

    @property
    def workspace_outputs(self) -> Path:
        return self.workspace_data / "outputs"

    @property
    def workspace_ocr(self) -> Path:
        return self.workspace_data / "ocr_text"

    @property
    def workspace_inputs(self) -> Path:
        return self.workspace_data / "input_pdfs"


def job_paths(job_id: str) -> JobPaths:
    root = JOBS_ROOT / job_id
    return JobPaths(
        root=root,
        input_dir=root / "input",
        stage1_dir=root / "stage1",
        stage2_dir=root / "stage2",
        stage3_dir=root / "stage3",
        final_dir=root / "final",
        export_dir=root / "export",
        workspace_dir=root / "workspace",
        job_meta_path=root / "job_meta.json",
        working_state_path=root / "working_state.json",
        audit_log_path=root / "audit_log.jsonl",
    )
