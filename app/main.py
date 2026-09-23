import asyncio
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import CrawlConfig, JobStatus, RenderMode
from .crawler import CrawlJob, DATA_DIR
from .exporter import csv_to_xlsx

app = FastAPI(title="Link Crawler")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

JOBS: dict[str, CrawlJob] = {}


class StartCrawlRequest(BaseModel):
    start_url: str
    max_urls: int = Field(5000, ge=1, le=50000)
    concurrency: int = Field(15, ge=1, le=100)
    same_domain_only: bool = True
    render_js: RenderMode = RenderMode.AUTO
    min_delay: float = Field(0.15, ge=0)
    max_delay: float = Field(0.6, ge=0)
    max_depth: Optional[int] = None
    respect_robots: bool = True
    check_external_status: bool = True


@app.get("/", response_class=HTMLResponse)
async def index():
    return (STATIC_DIR / "index.html").read_text(encoding="utf-8")


@app.post("/api/crawl/start")
async def start_crawl(req: StartCrawlRequest):
    if req.max_delay < req.min_delay:
        raise HTTPException(400, "max_delay must be >= min_delay")

    config = CrawlConfig(
        start_url=req.start_url,
        max_urls=req.max_urls,
        concurrency=req.concurrency,
        same_domain_only=req.same_domain_only,
        render_js=req.render_js,
        min_delay=req.min_delay,
        max_delay=req.max_delay,
        max_depth=req.max_depth,
        respect_robots=req.respect_robots,
        check_external_status=req.check_external_status,
    )
    job = CrawlJob(config)
    JOBS[job.id] = job
    asyncio.create_task(job.run())
    return {"job_id": job.id, "status": job.status.value}


@app.post("/api/crawl/stop/{job_id}")
async def stop_crawl(job_id: str):
    job = _get_job(job_id)
    job.request_stop()
    return {"job_id": job.id, "status": job.status.value}


@app.get("/api/crawl/status/{job_id}")
async def crawl_status(job_id: str):
    job = _get_job(job_id)
    return job.to_status_dict()


@app.get("/api/crawl/jobs")
async def list_jobs():
    return [job.to_status_dict() for job in JOBS.values()]


@app.get("/api/crawl/download/{job_id}")
async def download(job_id: str, fmt: str = "xlsx"):
    job = _get_job(job_id)
    if not job.csv_path.exists():
        raise HTTPException(404, "No results yet for this job")

    if fmt == "csv":
        return FileResponse(
            job.csv_path,
            filename=f"crawl_{job_id}_links.csv",
            media_type="text/csv",
        )
    if fmt == "xlsx":
        xlsx_path = DATA_DIR / f"{job_id}_edges.xlsx"
        if not xlsx_path.exists() or xlsx_path.stat().st_mtime < job.csv_path.stat().st_mtime:
            csv_to_xlsx(job.csv_path, xlsx_path)
        return FileResponse(
            xlsx_path,
            filename=f"crawl_{job_id}_links.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    raise HTTPException(400, "fmt must be 'csv' or 'xlsx'")


def _get_job(job_id: str) -> CrawlJob:
    job = JOBS.get(job_id)
    if job is None:
        raise HTTPException(404, "Unknown job_id")
    return job
