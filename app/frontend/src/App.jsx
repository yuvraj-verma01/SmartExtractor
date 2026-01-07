import React, { useEffect, useMemo, useRef, useState } from "react";

const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8000";

const emptyValue = (value) => {
  if (value === null || value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
};

const formatDetail = (detail) => {
  if (!detail) return "";
  if (typeof detail === "string") return detail;
  try {
    return JSON.stringify(detail);
  } catch (err) {
    return String(detail);
  }
};

const readSuggestionValue = (suggestion) => {
  if (!suggestion || typeof suggestion !== "object") return null;
  if (suggestion.value !== undefined && suggestion.value !== null) {
    return suggestion.value;
  }
  return null;
};

export default function App() {
  const [jobs, setJobs] = useState([]);
  const [selectedJobId, setSelectedJobId] = useState(null);
  const [jobState, setJobState] = useState(null);
  const [fieldEdits, setFieldEdits] = useState({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [backendStatus, setBackendStatus] = useState("unknown");
  const [backendError, setBackendError] = useState(null);
  const [jobName, setJobName] = useState("");
  const [reviewMode, setReviewMode] = useState("grid");
  const [currentFieldIndex, setCurrentFieldIndex] = useState(0);
  const [llmModel, setLlmModel] = useState("");
  const [guidedJump, setGuidedJump] = useState("");
  const fileInputRef = useRef(null);
  const guidedInitRef = useRef({ jobId: null, mode: null });
  const guidedSkipInitRef = useRef(false);

  const markBackendOk = () => {
    setBackendStatus("ok");
    setBackendError(null);
  };

  const markBackendError = (err) => {
    setBackendStatus("error");
    setBackendError(err?.message || String(err));
  };

  const requestJson = async (url, options) => {
    let res;
    try {
      res = await fetch(url, options);
    } catch (err) {
      markBackendError(err);
      throw err;
    }

    markBackendOk();

    let data = {};
    try {
      data = await res.json();
    } catch (err) {
      data = {};
    }

    if (!res.ok) {
      const detail = formatDetail(data.detail);
      throw new Error(detail || `Request failed: ${res.status}`);
    }

    return data;
  };

  const loadJobs = async () => {
    try {
      const data = await requestJson(`${API_BASE}/jobs`);
      setJobs(data.jobs || []);
    } catch (err) {
      setJobs([]);
    }
  };

  const loadJobState = async (jobId) => {
    if (!jobId) return;
    try {
      const data = await requestJson(`${API_BASE}/jobs/${jobId}/state`);
      setJobState(data);
      const edits = {};
      Object.entries(data.working_state?.fields || {}).forEach(([field, info]) => {
        edits[field] = emptyValue(info.value);
      });
      setFieldEdits(edits);
    } catch (err) {
      setError(err.message);
    }
  };

  const selectedJob = useMemo(() => jobs.find((job) => job.id === selectedJobId), [jobs, selectedJobId]);
  const activeJob = jobState?.job || selectedJob || null;
  const workingFields = jobState?.working_state?.fields || {};
  const reviewProgress = jobState?.review_progress || { reviewed: 0, total: 0 };
  const llmStatus = jobState?.working_state?.llm_status;
  const fieldOrder = jobState?.schema?.length ? jobState.schema : Object.keys(workingFields);
  const finalExists = Boolean(jobState?.final_exists);
  const evidenceByField = jobState?.evidence_by_field || {};
  const workingUpdatedAt = jobState?.working_state?.updated_at;
  const excelExportedAt = activeJob?.excel_exported_at;

  const hasUnsavedChanges = useMemo(() => {
    if (!fieldOrder.length) return false;
    return fieldOrder.some((field) => {
      const currentValue = emptyValue(workingFields[field]?.value);
      const editedValue = fieldEdits[field] ?? "";
      return editedValue !== currentValue;
    });
  }, [fieldOrder, fieldEdits, workingFields]);

  const needsExcelUpdate = useMemo(() => {
    if (!workingUpdatedAt) {
      return Boolean(excelExportedAt);
    }
    if (!excelExportedAt) {
      return true;
    }
    const updated = Date.parse(workingUpdatedAt);
    const exported = Date.parse(excelExportedAt);
    if (Number.isNaN(updated) || Number.isNaN(exported)) {
      return false;
    }
    return updated > exported;
  }, [workingUpdatedAt, excelExportedAt]);

  const saveStatus = (() => {
    if (hasUnsavedChanges) {
      return { label: "Unsaved changes", tone: "warning" };
    }
    if (needsExcelUpdate) {
      return { label: "Saved locally, not added to Excel", tone: "pending" };
    }
    if (excelExportedAt) {
      return { label: "Excel up to date", tone: "ok" };
    }
    return { label: "Not added to Excel yet", tone: "pending" };
  })();

  useEffect(() => {
    const checkBackend = async () => {
      try {
        await requestJson(`${API_BASE}/health`);
      } catch (err) {
        markBackendError(err);
      }
    };
    checkBackend();
    loadJobs();
  }, []);

  useEffect(() => {
    if (selectedJobId) {
      loadJobState(selectedJobId);
    }
  }, [selectedJobId]);

  useEffect(() => {
    setGuidedJump("");
  }, [selectedJobId]);

  useEffect(() => {
    if (!activeJob) return;
    if (activeJob.llm_model) {
      setLlmModel(activeJob.llm_model);
    }
  }, [activeJob?.llm_model, selectedJobId]);

  useEffect(() => {
    if (!selectedJobId) return;
    if (activeJob?.status !== "running") return;
    const timer = setInterval(() => {
      loadJobs();
      loadJobState(selectedJobId);
    }, 2000);
    return () => clearInterval(timer);
  }, [selectedJobId, activeJob?.status]);

  useEffect(() => {
    if (reviewMode !== "guided") return;
    if (!fieldOrder.length) return;
    const last = guidedInitRef.current;
    const needsInit = last.jobId !== selectedJobId || last.mode !== reviewMode;
    if (!needsInit) return;
    if (guidedSkipInitRef.current) {
      guidedSkipInitRef.current = false;
      guidedInitRef.current = { jobId: selectedJobId, mode: reviewMode };
      return;
    }
    const firstUnreviewed = fieldOrder.findIndex((field) => {
      const info = workingFields[field];
      return info?.review?.status !== "reviewed";
    });
    setCurrentFieldIndex(firstUnreviewed >= 0 ? firstUnreviewed : 0);
    guidedInitRef.current = { jobId: selectedJobId, mode: reviewMode };
  }, [reviewMode, selectedJobId, fieldOrder.length]);

  const onCreateJobWithFile = async (file) => {
    if (!file) {
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const form = new FormData();
      form.append("file", file);
      if (jobName.trim()) {
        form.append("name", jobName.trim());
      }
      const data = await requestJson(`${API_BASE}/jobs`, {
        method: "POST",
        body: form
      });
      await loadJobs();
      const newId = data.job?.id;
      if (newId) {
        setSelectedJobId(newId);
      }
      setJobName("");
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const onCreateEmptyJob = async () => {
    setLoading(true);
    setError(null);
    try {
      const form = new FormData();
      if (jobName.trim()) {
        form.append("name", jobName.trim());
      }
      const data = await requestJson(`${API_BASE}/jobs`, {
        method: "POST",
        body: form
      });
      await loadJobs();
      const newId = data.job?.id;
      if (newId) {
        setSelectedJobId(newId);
      }
      setJobName("");
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const onRunPipeline = async () => {
    if (!selectedJobId) return;
    setLoading(true);
    setError(null);
    try {
      await requestJson(`${API_BASE}/jobs/${selectedJobId}/run`, { method: "POST" });
      await loadJobState(selectedJobId);
      await loadJobs();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const onRunStage = async (stage) => {
    if (!selectedJobId) return;
    setLoading(true);
    setError(null);
    try {
      const payload = {};
      if (stage === "stage3" && llmModel.trim()) {
        payload.model = llmModel.trim();
      }
      const options = { method: "POST" };
      if (Object.keys(payload).length) {
        options.headers = { "Content-Type": "application/json" };
        options.body = JSON.stringify(payload);
      }
      await requestJson(`${API_BASE}/jobs/${selectedJobId}/run_stage/${stage}`, options);
      await loadJobState(selectedJobId);
      await loadJobs();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const onFieldAction = async (field, action, value, source) => {
    if (!selectedJobId) return;
    setLoading(true);
    setError(null);
    let ok = false;
    try {
      await requestJson(`${API_BASE}/jobs/${selectedJobId}/field_action`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ field, action, value, source })
      });
      await loadJobState(selectedJobId);
      ok = true;
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
    return ok;
  };

  const onSaveChanges = async () => {
    if (!selectedJobId) return;
    if (!fieldOrder.length) return;
    const changes = {};
    fieldOrder.forEach((field) => {
      const currentValue = emptyValue(workingFields[field]?.value);
      const editedValue = fieldEdits[field] ?? "";
      if (editedValue !== currentValue) {
        changes[field] = editedValue;
      }
    });
    if (!Object.keys(changes).length) {
      return;
    }
    setLoading(true);
    setError(null);
    try {
      await requestJson(`${API_BASE}/jobs/${selectedJobId}/save`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ fields: changes })
      });
      await loadJobState(selectedJobId);
      await loadJobs();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const onAddToExcel = async () => {
    if (!selectedJobId) return;
    setLoading(true);
    setError(null);
    try {
      await requestJson(`${API_BASE}/jobs/${selectedJobId}/export_excel`, { method: "POST" });
      await loadJobState(selectedJobId);
      await loadJobs();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const downloadFile = async (path, filename) => {
    if (!selectedJobId) return;
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/jobs/${selectedJobId}/download/${path}`);
      if (!res.ok) {
        let detail = "";
        try {
          const data = await res.json();
          detail = formatDetail(data.detail);
        } catch (err) {
          detail = "";
        }
        throw new Error(detail || `Download failed (${res.status})`);
      }
      const blob = await res.blob();
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const onDeleteJob = async () => {
    if (!selectedJobId) return;
    if (!window.confirm("Delete this job and all its files? This cannot be undone.")) {
      return;
    }
    setLoading(true);
    setError(null);
    try {
      await requestJson(`${API_BASE}/jobs/${selectedJobId}`, { method: "DELETE" });
      setSelectedJobId(null);
      setJobState(null);
      await loadJobs();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const pipeline = activeJob?.pipeline || {};
  const isRunning = activeJob?.status === "running";
  const stageDetails = [
    { key: "stage1", label: "Stage 1" },
    { key: "stage2", label: "Stage 2" },
    { key: "stage3", label: "Stage 3" }
  ];

  const guidedField = fieldOrder[currentFieldIndex];
  const guidedInfo = guidedField ? workingFields[guidedField] : null;

  const moveToIndex = (index) => {
    if (!fieldOrder.length) return;
    const nextIndex = Math.min(Math.max(index, 0), fieldOrder.length - 1);
    setCurrentFieldIndex(nextIndex);
  };

  const moveToNextUnreviewed = (startIndex) => {
    if (!fieldOrder.length) return;
    const start = Math.min(Math.max(startIndex, 0), fieldOrder.length - 1);
    for (let i = start; i < fieldOrder.length; i += 1) {
      const info = workingFields[fieldOrder[i]];
      if (info?.review?.status !== "reviewed") {
        setCurrentFieldIndex(i);
        return;
      }
    }
    for (let i = 0; i < start; i += 1) {
      const info = workingFields[fieldOrder[i]];
      if (info?.review?.status !== "reviewed") {
        setCurrentFieldIndex(i);
        return;
      }
    }
  };

  const onGuidedAction = async (field, action, value, source) => {
    const ok = await onFieldAction(field, action, value, source);
    if (!ok) return;
    moveToNextUnreviewed(currentFieldIndex + 1);
  };

  const jumpToFieldNumber = () => {
    const parsed = Number.parseInt(guidedJump, 10);
    if (!Number.isFinite(parsed)) {
      return;
    }
    moveToIndex(parsed - 1);
  };

  return (
    <div className="app-shell">
      <header className="hero">
        <div>
          <p className="eyebrow">Lease Automation</p>
          <h1>SmartExtractor</h1>
          <p className="subtitle">
            An AI-powered smart and efficient lease data extraction and review tool.
          </p>
        </div>
        <div className="hero-actions">
          <input
            ref={fileInputRef}
            className="file-input"
            type="file"
            accept="application/pdf"
            onChange={(event) => {
              const file = event.target.files?.[0];
              if (file) {
                onCreateJobWithFile(file);
              }
              event.target.value = "";
            }}
            disabled={loading}
          />
          <input
            className="name-input"
            value={jobName}
            onChange={(event) => setJobName(event.target.value)}
            placeholder="Job name (optional)"
            disabled={loading}
          />
          <button
            className="upload"
            type="button"
            onClick={() => fileInputRef.current?.click()}
            disabled={loading}
          >
            Create job
          </button>
          <button className="ghost" onClick={onCreateEmptyJob} disabled={loading}>
            Create empty job
          </button>
        </div>
      </header>

      {backendStatus === "error" && (
        <div className="error-banner">
          Backend not reachable at {API_BASE}. Start `uvicorn` and refresh. ({backendError})
        </div>
      )}
      {error && <div className="error-banner">{error}</div>}

      <main className="layout">
        <aside className="panel">
          <div className="panel-header">
            <h2>Jobs</h2>
            <button
              className="ghost"
              onClick={() => {
                loadJobs();
                if (selectedJobId) {
                  loadJobState(selectedJobId);
                }
              }}
              disabled={loading}
            >
              Refresh
            </button>
          </div>
          <div className="job-list">
            {jobs.length === 0 && backendStatus !== "error" && <p className="muted">No jobs yet.</p>}
            {jobs.map((job, index) => (
              <button
                key={job.id}
                className={`job-card ${job.id === selectedJobId ? "active" : ""}`}
                style={{ animationDelay: `${index * 40}ms` }}
                onClick={() => setSelectedJobId(job.id)}
              >
                <div>
                  <p className="job-title">{job.name || job.id}</p>
                  {job.name && <p className="job-meta">ID: {job.id}</p>}
                  <p className="job-meta">Status: {job.status || "unknown"}</p>
                  <p className="job-meta">LLM: {job.llm_status || "unknown"}</p>
                </div>
                <span className="pill">{job.pipeline?.stage2?.status || "pending"}</span>
              </button>
            ))}
          </div>
        </aside>

        <section className="panel detail">
          {!activeJob && <p className="muted">Select a job to begin review.</p>}
          {activeJob && (
            <>
              <div className="detail-header">
                <div>
                  <h2>Job {activeJob.name || activeJob.id}</h2>
                  <p className="job-meta">Status: {activeJob.status || "unknown"}</p>
                  {activeJob.name && <p className="job-meta">ID: {activeJob.id}</p>}
                  {activeJob.excel_row && <p className="job-meta">Excel row: {activeJob.excel_row}</p>}
                  {activeJob.excel_exported_at && <p className="job-meta">Excel updated: {activeJob.excel_exported_at}</p>}
                </div>
                <div className="detail-actions">
                  <button className="primary" onClick={onRunPipeline} disabled={loading || isRunning}>
                    {isRunning ? "Running..." : "Run pipeline"}
                  </button>
                  <button
                    className="ghost"
                    onClick={onSaveChanges}
                    disabled={loading || isRunning || !hasUnsavedChanges}
                  >
                    Save changes
                  </button>
                  <button
                    className="ghost"
                    onClick={onAddToExcel}
                    disabled={
                      loading ||
                      isRunning ||
                      reviewProgress.reviewed !== reviewProgress.total ||
                      hasUnsavedChanges
                    }
                  >
                    Add to Excel
                  </button>
                  <button className="ghost" onClick={onDeleteJob} disabled={loading || isRunning}>
                    Delete job
                  </button>
                  <button
                    className="ghost"
                    onClick={() => downloadFile("working_json", "working_state.json")}
                    disabled={loading}
                  >
                    Download working JSON
                  </button>
                  <button
                    className="ghost"
                    onClick={() => downloadFile("final_json", "lease_final.json")}
                    disabled={loading || !finalExists}
                  >
                    Download JSON
                  </button>
                  <button
                    className="ghost"
                    onClick={() => downloadFile("xlsx", "lease_jobs.xlsx")}
                    disabled={loading || !activeJob?.excel_exported_at}
                  >
                    Download XLSX (all jobs)
                  </button>
                </div>
              </div>

              {!finalExists && (
                <p className="muted download-hint">
                  Excel export unlocks after all fields are reviewed. Working JSON is available anytime.
                </p>
              )}

              <div className={`save-status ${saveStatus.tone}`}>
                {saveStatus.label}
              </div>

              {isRunning && (
                <div className="banner running">
                  Pipeline running. Status updates every 2 seconds.
                </div>
              )}
              {activeJob?.last_error && (
                <div className="error-banner">
                  Last error: {activeJob.last_error}
                </div>
              )}

              <div className="status-panel">
                {stageDetails.map((stage) => {
                  const info = pipeline[stage.key] || {};
                  const status = info.status || "pending";
                  return (
                    <div className="status-row" key={stage.key}>
                      <span className={`status-tag ${status}`}>{stage.label}: {status}</span>
                      {info.message && <span className="status-message">{info.message}</span>}
                    </div>
                  );
                })}
              </div>

              <div className="stage-controls">
                <div className="stage-buttons">
                  <button className="ghost" onClick={() => onRunStage("stage1")} disabled={loading || isRunning}>
                    Run stage 1
                  </button>
                  <button className="ghost" onClick={() => onRunStage("stage2")} disabled={loading || isRunning}>
                    Run stage 2
                  </button>
                </div>
                <div className="stage-llm">
                  <input
                    className="name-input llm-model-input"
                    value={llmModel}
                    onChange={(event) => setLlmModel(event.target.value)}
                    placeholder="LLM model (full name)"
                    disabled={loading || isRunning}
                  />
                  <button className="ghost" onClick={() => onRunStage("stage3")} disabled={loading || isRunning}>
                    Run stage 3 (LLM)
                  </button>
                  <p className="job-meta">Current model: {activeJob?.llm_model || "default"}</p>
                </div>
              </div>

              <div className="review-toggle">
                <button
                  className={reviewMode === "guided" ? "primary" : "ghost"}
                  onClick={() => setReviewMode("guided")}
                  disabled={loading}
                >
                  Guided review
                </button>
                <button
                  className={reviewMode === "grid" ? "primary" : "ghost"}
                  onClick={() => setReviewMode("grid")}
                  disabled={loading}
                >
                  Grid view
                </button>
              </div>

              {reviewMode === "guided" && guidedField && guidedInfo && (
                <div className={`guided-panel ${guidedInfo.review?.status || "unreviewed"}`}>
                    <div className="guided-header">
                      <div>
                        <p className="field-label">{guidedField}</p>
                        <p className="field-meta">
                          Confidence: {guidedInfo.confidence === null || guidedInfo.confidence === undefined ? "--" : guidedInfo.confidence}
                        </p>
                      <p className="field-meta">
                        Field #{currentFieldIndex + 1} of {fieldOrder.length}
                      </p>
                      </div>
                      <div className="guided-header-actions">
                        <div className="guided-jump">
                          <input
                            type="number"
                            min="1"
                            max={fieldOrder.length}
                            value={guidedJump}
                            onChange={(event) => setGuidedJump(event.target.value)}
                            placeholder="#"
                            className="guided-jump-input"
                          />
                          <button className="ghost tiny" type="button" onClick={jumpToFieldNumber} disabled={loading}>
                            Go
                          </button>
                        </div>
                        <span className="status-dot" title={guidedInfo.review?.status || "unreviewed"} />
                      </div>
                    </div>

                  <div className="guided-body">
                    <div className="guided-input">
                      <input
                        className="field-input"
                        value={fieldEdits[guidedField] ?? ""}
                        placeholder="No value extracted"
                        onChange={(event) =>
                          setFieldEdits((prev) => ({
                            ...prev,
                            [guidedField]: event.target.value
                          }))
                        }
                      />
                      <div className="field-actions">
                        <button
                          className="ghost"
                          onClick={() => onGuidedAction(guidedField, "accept", fieldEdits[guidedField], "current")}
                          disabled={loading}
                        >
                          Accept
                        </button>
                        <button
                          className="ghost"
                          onClick={() => onGuidedAction(guidedField, "edit", fieldEdits[guidedField], "edited")}
                          disabled={loading}
                        >
                          Save Edit
                        </button>
                        <button
                          className="ghost"
                          onClick={() => onGuidedAction(guidedField, "clear", null, "cleared")}
                          disabled={loading}
                        >
                          Clear
                        </button>
                      </div>
                      <div className="guided-nav">
                        <button className="ghost" onClick={() => moveToIndex(currentFieldIndex - 1)} disabled={loading}>
                          Previous
                        </button>
                        <button className="ghost" onClick={() => moveToNextUnreviewed(currentFieldIndex + 1)} disabled={loading}>
                          Next unreviewed
                        </button>
                        <button className="ghost" onClick={() => moveToIndex(currentFieldIndex + 1)} disabled={loading}>
                          Next
                        </button>
                      </div>
                    </div>

                    <div className="guided-suggestions">
                      <p className="suggestion-title">Suggestions</p>
                      <p className="suggestion-body">
                        {(() => {
                          const derived = guidedInfo.suggestions?.derived || [];
                          const llm = guidedInfo.suggestions?.llm;
                          const derivedValue = derived.map(readSuggestionValue).find((v) => v !== null && v !== undefined);
                          const llmValue = readSuggestionValue(llm);
                          if (derivedValue !== undefined && derivedValue !== null) {
                            return `Derived: ${emptyValue(derivedValue)}`;
                          }
                          if (llmValue !== undefined && llmValue !== null) {
                            return `LLM: ${emptyValue(llmValue)}`;
                          }
                          return "No suggestions available.";
                        })()}
                      </p>
                      <p className="suggestion-title">Evidence</p>
                      {(() => {
                        const evidence = [...(evidenceByField[guidedField] || [])];
                        const llmQuote = guidedInfo.suggestions?.llm?.quote;
                        const llmPage = guidedInfo.suggestions?.llm?.page;
                        if (llmQuote) {
                          evidence.unshift({
                            text: llmQuote,
                            page: llmPage,
                            line_no: null,
                            source_field: "llm",
                            score: null
                          });
                        }
                        if (!evidence.length) {
                          return <p className="suggestion-body muted">No evidence snippets available.</p>;
                        }
                        return (
                          <div className="evidence-list">
                            {evidence.map((item, idx) => (
                              <div className="evidence-item" key={`${guidedField}-${idx}`}>
                                <p className="evidence-meta">
                                  Page: {item.page ?? "--"} | Line: {item.line_no ?? "--"} | Source: {item.source_field ?? "--"}
                                </p>
                                <p className="evidence-text">{item.text ?? item.snippet ?? ""}</p>
                              </div>
                            ))}
                          </div>
                        );
                      })()}
                    </div>
                  </div>

                </div>
              )}

              {reviewMode === "grid" && (
                <>
                  <div className="progress">
                    <div className="progress-bar">
                      <span style={{ width: `${(reviewProgress.reviewed / Math.max(reviewProgress.total, 1)) * 100}%` }} />
                    </div>
                    <p>
                      Reviewed {reviewProgress.reviewed} of {reviewProgress.total}
                    </p>
                  </div>

                  {llmStatus === "unavailable" && (
                    <div className="banner">
                      LLM unavailable. The pipeline skipped Ollama fallback for this job.
                    </div>
                  )}

                  <div className="field-grid">
                    {fieldOrder.map((field, index) => {
                      const info = workingFields[field];
                      if (!info) {
                        return null;
                      }
                      const reviewStatus = info.review?.status || "unreviewed";
                      const derived = info.suggestions?.derived || [];
                      const llm = info.suggestions?.llm;
                      const derivedValue = derived.map(readSuggestionValue).find((v) => v !== null && v !== undefined);
                      const llmValue = readSuggestionValue(llm);

                      return (
                        <div key={field} className={`field-card ${reviewStatus}`} style={{ animationDelay: `${index * 25}ms` }}>
                          <div className="field-header">
                            <div>
                              <p className="field-label">{field}</p>
                              <p className="field-meta">
                                Confidence: {info.confidence === null || info.confidence === undefined ? "--" : info.confidence}
                              </p>
                            </div>
                            <div className="field-header-actions">
                              <span className="field-number">#{index + 1}</span>
                              <button
                                className="ghost tiny"
                                type="button"
                                onClick={() => {
                                  guidedSkipInitRef.current = true;
                                  setReviewMode("guided");
                                  setCurrentFieldIndex(index);
                                }}
                                disabled={loading}
                              >
                                Guided
                              </button>
                              <span className="status-dot" title={reviewStatus} />
                            </div>
                          </div>

                          <input
                            className="field-input"
                            value={fieldEdits[field] ?? ""}
                            placeholder="No value extracted"
                            onChange={(event) =>
                              setFieldEdits((prev) => ({
                                ...prev,
                                [field]: event.target.value
                              }))
                            }
                          />

                          <div className="suggestions">
                            {derived.length > 0 && (
                              <div>
                                <p className="suggestion-title">Derived suggestions</p>
                                <p className="suggestion-body">
                                  {derivedValue !== undefined && derivedValue !== null
                                    ? emptyValue(derivedValue)
                                    : "See JSON for details"}
                                </p>
                              </div>
                            )}
                            {llm && (
                              <div>
                                <p className="suggestion-title">LLM suggestion</p>
                                <p className="suggestion-body">
                                  {llmValue !== undefined && llmValue !== null ? emptyValue(llmValue) : "See JSON for details"}
                                </p>
                              </div>
                            )}
                          </div>

                          <div className="field-actions">
                            <button
                              className="ghost"
                              onClick={() => onFieldAction(field, "accept", fieldEdits[field], "current")}
                              disabled={loading}
                            >
                              Accept
                            </button>
                            <button
                              className="ghost"
                              onClick={() => onFieldAction(field, "edit", fieldEdits[field], "edited")}
                              disabled={loading}
                            >
                              Save Edit
                            </button>
                            <button
                              className="ghost"
                              onClick={() => onFieldAction(field, "clear", null, "cleared")}
                              disabled={loading}
                            >
                              Clear
                            </button>
                            {derivedValue !== undefined && derivedValue !== null && (
                              <button
                                className="ghost"
                                onClick={() => onFieldAction(field, "accept", derivedValue, "derived")}
                                disabled={loading}
                              >
                                Use derived
                              </button>
                            )}
                            {llmValue !== undefined && llmValue !== null && (
                              <button
                                className="ghost"
                                onClick={() => onFieldAction(field, "accept", llmValue, "llm")}
                                disabled={loading}
                              >
                                Use LLM
                              </button>
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </>
              )}
            </>
          )}
        </section>
      </main>
    </div>
  );
}
