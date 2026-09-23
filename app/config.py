"""Shared constants and config/data models for the crawler."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

# Rotate through a handful of realistic desktop User-Agent strings so the
# crawler doesn't look like a single obvious bot to every server it hits.
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# File-like extensions we should record as links but never try to fetch &
# parse as HTML (binaries, media, archives, etc.)
NON_HTML_EXTENSIONS = {
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico", ".bmp",
    ".zip", ".rar", ".7z", ".tar", ".gz", ".mp3", ".mp4", ".avi", ".mov",
    ".wmv", ".mkv", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".css", ".js", ".json", ".xml", ".woff", ".woff2", ".ttf", ".eot",
    ".exe", ".dmg", ".apk", ".csv", ".rss",
}


class JobStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    COMPLETED = "completed"
    ERROR = "error"


class RenderMode(str, Enum):
    OFF = "off"        # never use a headless browser
    AUTO = "auto"       # try static fetch first, fall back to JS render if page looks empty/SPA
    ALWAYS = "always"   # always render every internal page with a headless browser


@dataclass
class CrawlConfig:
    start_url: str
    max_urls: int = 5000                 # max distinct internal pages to crawl
    concurrency: int = 15                # number of parallel workers
    same_domain_only: bool = True        # internal = same registered domain as start_url
    render_js: RenderMode = RenderMode.AUTO
    min_delay: float = 0.15              # seconds, randomized human-like delay floor
    max_delay: float = 0.6               # seconds, randomized human-like delay ceiling
    request_timeout: float = 15.0
    max_depth: Optional[int] = None      # None = unlimited
    respect_robots: bool = True
    check_external_status: bool = True   # HEAD/GET external links just to record status code
    js_render_timeout: float = 20.0


@dataclass
class Edge:
    from_url: str
    to_url: str
    link_text: str
    is_internal: bool
    status_code: Optional[int]
    content_type: str
    error: str
    rendered_with_js: bool
    depth: int
