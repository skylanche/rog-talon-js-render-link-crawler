"""Core crawl job: async worker pool that walks a site like a browser,
recording every link edge (from-page -> to-page) with status codes, and
streams results straight to a CSV file on disk so memory stays flat even
at tens of thousands of pages / millions of links.

Key design point: a target URL might be linked from many pages before it is
ever actually crawled. We don't want to silently drop those extra edges (a
naive version would only record the first discoverer). Instead each target
URL has a resolved-status cache entry once it's known (crawled, checked, or
explicitly given up on); edges discovered before that are buffered in
`pending_edges` keyed by target and flushed the moment the target resolves.
"""

from __future__ import annotations

import asyncio
import csv
import random
import time
import urllib.robotparser as robotparser
import uuid
from pathlib import Path
from typing import Optional
from urllib.parse import urldefrag, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from .config import (
    USER_AGENTS,
    NON_HTML_EXTENSIONS,
    JobStatus,
    RenderMode,
    CrawlConfig,
    Edge,
)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

CSV_FIELDS = [
    "from_url",
    "to_url",
    "link_text",
    "is_internal",
    "status_code",
    "content_type",
    "error",
    "rendered_with_js",
    "depth",
]


def normalize_url(url: str) -> str:
    """Strip fragments and trailing slashes (except root) so we don't
    treat http://x.com/a and http://x.com/a#section as different pages."""
    url, _frag = urldefrag(url)
    parsed = urlparse(url)
    path = parsed.path
    if len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    return parsed._replace(path=path).geturl()


def is_probably_html_link(url: str) -> bool:
    path = urlparse(url).path.lower()
    for ext in NON_HTML_EXTENSIONS:
        if path.endswith(ext):
            return False
    return True


class CrawlJob:
    def __init__(self, config: CrawlConfig):
        self.id = str(uuid.uuid4())[:8]
        self.config = config
        self.status = JobStatus.IDLE
        self.start_domain = urlparse(config.start_url).netloc

        self.queue: "asyncio.Queue[tuple[str, int]]" = asyncio.Queue()
        self.visited: set[str] = set()
        self.enqueued: set[str] = set()
        self.checked_targets: set[str] = set()  # external/asset URLs already had a check task fired

        # url -> {"status_code", "content_type", "error", "rendered_with_js"}
        self.url_status: dict[str, dict] = {}
        # url -> list of (from_url, link_text, depth, is_internal) awaiting resolution
        self.pending_edges: dict[str, list[tuple[str, str, int, bool]]] = {}

        self._stop_event = asyncio.Event()
        self.pages_crawled = 0
        self.links_found = 0
        self.errors = 0
        self.js_rendered_count = 0
        self.started_at: Optional[float] = None
        self.finished_at: Optional[float] = None
        self.last_error_message: str = ""

        self._robots_cache: dict[str, robotparser.RobotFileParser] = {}
        self._csv_path = DATA_DIR / f"{self.id}_edges.csv"
        self._csv_lock = asyncio.Lock()
        self._csv_file = None
        self._csv_writer = None
        self._state_lock = asyncio.Lock()

        self._browser = None
        self._playwright = None
        self._worker_tasks: list[asyncio.Task] = []
        self._check_tasks: list[asyncio.Task] = []

    # ---------- public API ----------

    def to_status_dict(self) -> dict:
        elapsed = None
        if self.started_at:
            end = self.finished_at or time.time()
            elapsed = round(end - self.started_at, 1)
        return {
            "job_id": self.id,
            "status": self.status.value,
            "start_url": self.config.start_url,
            "pages_crawled": self.pages_crawled,
            "links_found": self.links_found,
            "queue_size": self.queue.qsize(),
            "errors": self.errors,
            "js_rendered_count": self.js_rendered_count,
            "elapsed_seconds": elapsed,
            "max_urls": self.config.max_urls,
            "last_error_message": self.last_error_message,
        }

    def request_stop(self):
        self._stop_event.set()
        if self.status == JobStatus.RUNNING:
            self.status = JobStatus.STOPPING

    async def run(self):
        self.status = JobStatus.RUNNING
        self.started_at = time.time()
        self._csv_file = open(self._csv_path, "w", newline="", encoding="utf-8")
        self._csv_writer = csv.DictWriter(self._csv_file, fieldnames=CSV_FIELDS)
        self._csv_writer.writeheader()

        start = normalize_url(self.config.start_url)
        self.enqueued.add(start)
        await self.queue.put((start, 0))

        if self.config.render_js != RenderMode.OFF:
            await self._start_browser()

        try:
            async with httpx.AsyncClient(
                http2=True,
                follow_redirects=True,
                timeout=self.config.request_timeout,
                limits=httpx.Limits(max_connections=self.config.concurrency * 2),
            ) as client:
                self._worker_tasks = [
                    asyncio.create_task(self._worker(client))
                    for _ in range(self.config.concurrency)
                ]
                await asyncio.gather(*self._worker_tasks, return_exceptions=True)

                # Let in-flight background status checks (external links /
                # non-HTML assets) finish so their edges get flushed too.
                for _ in range(200):  # bounded wait, ~20s max
                    if not self._check_tasks or all(t.done() for t in self._check_tasks):
                        break
                    await asyncio.sleep(0.1)
                if self._check_tasks:
                    await asyncio.gather(*self._check_tasks, return_exceptions=True)

                # Anything still pending (e.g. a check task errored without
                # resolving, or the job was stopped mid-flight) gets flushed
                # with an explicit "unresolved" marker so no edge is silently lost.
                await self._flush_unresolved()
        finally:
            await self._stop_browser()
            if self._csv_file:
                self._csv_file.close()
            self.finished_at = time.time()
            if self.status == JobStatus.STOPPING:
                self.status = JobStatus.STOPPED
            elif self.status == JobStatus.RUNNING:
                self.status = JobStatus.COMPLETED

    # ---------- worker loop ----------

    async def _worker(self, client: httpx.AsyncClient):
        while True:
            if self._stop_event.is_set():
                return
            if self.pages_crawled >= self.config.max_urls:
                return
            try:
                url, depth = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                if self.queue.empty():
                    return
                continue

            if url in self.visited:
                self.queue.task_done()
                continue
            if self.config.max_depth is not None and depth > self.config.max_depth:
                self.queue.task_done()
                continue

            self.visited.add(url)
            self.pages_crawled += 1

            try:
                await self._crawl_page(client, url, depth)
            except Exception as exc:  # noqa: BLE001
                self.errors += 1
                self.last_error_message = f"{url}: {exc}"
                await self._resolve_status(url, None, "", str(exc)[:200], False)
            finally:
                self.queue.task_done()

            # human-like pacing
            await asyncio.sleep(random.uniform(self.config.min_delay, self.config.max_delay))

    # ---------- fetching & parsing ----------

    async def _crawl_page(self, client: httpx.AsyncClient, url: str, depth: int):
        if self.config.respect_robots and not self._robots_allowed(url):
            await self._resolve_status(url, None, "", "blocked by robots.txt", False)
            return

        html, status_code, content_type, rendered_with_js, error = await self._fetch(client, url)
        await self._resolve_status(url, status_code, content_type, error, rendered_with_js)

        if not html or status_code is None or status_code >= 400:
            return

        links = self._extract_links(html, url)
        seen_on_page = set()
        for link_url, link_text in links:
            link_url = normalize_url(link_url)
            pair = (url, link_url)
            if pair in seen_on_page:
                continue
            seen_on_page.add(pair)
            self.links_found += 1

            is_internal = self._is_internal(link_url)
            await self._record_edge_for_link(url, link_url, link_text, depth, is_internal)

            if is_internal and is_probably_html_link(link_url):
                should_enqueue = False
                over_budget = False
                async with self._state_lock:
                    if link_url not in self.enqueued:
                        if len(self.enqueued) < self.config.max_urls:
                            self.enqueued.add(link_url)
                            should_enqueue = True
                        else:
                            over_budget = True
                if should_enqueue:
                    await self.queue.put((link_url, depth + 1))
                elif over_budget:
                    await self._resolve_status(
                        link_url, None, "", "not crawled (max_urls limit reached)", False
                    )
            else:
                # external link, or a non-HTML internal asset (pdf/image/etc.) —
                # never queued for full crawling, just optionally status-checked.
                need_check = False
                async with self._state_lock:
                    if link_url not in self.checked_targets:
                        self.checked_targets.add(link_url)
                        need_check = True
                if need_check:
                    if (not is_internal) and not self.config.check_external_status:
                        await self._resolve_status(link_url, None, "", "not checked (disabled)", False)
                    else:
                        t = asyncio.create_task(self._check_status_only(client, link_url))
                        self._check_tasks.append(t)

    async def _check_status_only(self, client: httpx.AsyncClient, to_url: str):
        """Lightweight HEAD (falling back to GET) purely to resolve a status
        code for a URL we won't crawl further, then flush edges waiting on it."""
        status_code, content_type, error = None, "", ""
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            resp = await client.head(to_url, headers=headers, timeout=10.0)
            if resp.status_code >= 400 or resp.status_code == 405:
                resp = await client.get(to_url, headers=headers, timeout=10.0)
            status_code = resp.status_code
            content_type = resp.headers.get("content-type", "")
        except Exception as exc:  # noqa: BLE001
            error = str(exc)[:200]
        await self._resolve_status(to_url, status_code, content_type, error, False)

    async def _fetch(self, client: httpx.AsyncClient, url: str):
        """Returns (html, status_code, content_type, rendered_with_js, error)."""
        rendered_with_js = False
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            resp = await client.get(url, headers=headers)
            status_code = resp.status_code
            content_type = resp.headers.get("content-type", "")
            html = resp.text if "text/html" in content_type or content_type == "" else ""

            needs_js = self.config.render_js == RenderMode.ALWAYS
            if self.config.render_js == RenderMode.AUTO and html:
                needs_js = self._looks_like_spa(html)

            if needs_js and self._browser is not None:
                js_html = await self._render_with_browser(url)
                if js_html:
                    html = js_html
                    rendered_with_js = True
                    self.js_rendered_count += 1

            return html, status_code, content_type, rendered_with_js, ""
        except httpx.HTTPError as exc:
            return "", None, "", False, str(exc)[:200]
        except Exception as exc:  # noqa: BLE001
            return "", None, "", False, str(exc)[:200]

    @staticmethod
    def _looks_like_spa(html: str) -> bool:
        """Cheap heuristic: very little visible text but a root div/app
        container and script tags usually means client-side rendering."""
        text_len = len(BeautifulSoup(html, "lxml").get_text(strip=True))
        if text_len > 400:
            return False
        markers = ("id=\"root\"", "id=\"app\"", "ng-version", "__next", "data-reactroot")
        return text_len < 400 and (any(m in html for m in markers) or "<script" in html)

    async def _render_with_browser(self, url: str) -> Optional[str]:
        if self._browser is None:
            return None
        try:
            page = await self._browser.new_page(user_agent=random.choice(USER_AGENTS))
            try:
                await page.goto(url, timeout=self.config.js_render_timeout * 1000, wait_until="networkidle")
                content = await page.content()
                return content
            finally:
                await page.close()
        except Exception as exc:  # noqa: BLE001
            self.last_error_message = f"JS render failed for {url}: {exc}"
            return None

    async def _start_browser(self):
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            self.last_error_message = (
                "Playwright not installed; JS rendering disabled. "
                "Run: pip install playwright && playwright install chromium"
            )
            return
        try:
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=True)
        except Exception as exc:  # noqa: BLE001
            self.last_error_message = f"Could not launch headless browser: {exc}"
            self._browser = None

    async def _stop_browser(self):
        if self._browser is not None:
            await self._browser.close()
        if self._playwright is not None:
            await self._playwright.stop()

    # ---------- link extraction & classification ----------

    def _extract_links(self, html: str, base_url: str) -> list[tuple[str, str]]:
        soup = BeautifulSoup(html, "lxml")
        out = []
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if not href or href.startswith(("javascript:", "mailto:", "tel:", "#")):
                continue
            absolute = urljoin(base_url, href)
            if absolute.startswith(("http://", "https://")):
                text = tag.get_text(strip=True)[:150]
                out.append((absolute, text))
        return out

    def _is_internal(self, url: str) -> bool:
        if not self.config.same_domain_only:
            return True
        return urlparse(url).netloc == self.start_domain

    def _robots_allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        rp = self._robots_cache.get(origin)
        if rp is None:
            rp = robotparser.RobotFileParser()
            rp.set_url(urljoin(origin, "/robots.txt"))
            try:
                rp.read()
            except Exception:  # noqa: BLE001
                pass  # if robots.txt can't be read, default to allow
            self._robots_cache[origin] = rp
        try:
            return rp.can_fetch("*", url)
        except Exception:  # noqa: BLE001
            return True

    # ---------- edge resolution / output ----------

    async def _record_edge_for_link(self, from_url: str, to_url: str, link_text: str, depth: int, is_internal: bool):
        """Called the moment a link is discovered on a page. If the target's
        status is already known, write the edge immediately; otherwise buffer
        it until the target resolves (gets crawled, checked, or given up on)."""
        async with self._state_lock:
            cached = self.url_status.get(to_url)
            if cached is None:
                self.pending_edges.setdefault(to_url, []).append((from_url, link_text, depth, is_internal))
                return
        await self._write_edge(Edge(
            from_url=from_url, to_url=to_url, link_text=link_text, is_internal=is_internal,
            status_code=cached["status_code"], content_type=cached["content_type"],
            error=cached["error"], rendered_with_js=cached["rendered_with_js"], depth=depth,
        ))

    async def _resolve_status(self, url: str, status_code, content_type: str, error: str, rendered_with_js: bool):
        """Mark a URL's status as known and flush every edge that was
        waiting on it."""
        async with self._state_lock:
            self.url_status[url] = {
                "status_code": status_code,
                "content_type": content_type,
                "error": error,
                "rendered_with_js": rendered_with_js,
            }
            pending = self.pending_edges.pop(url, [])
        for from_url, link_text, depth, is_internal in pending:
            await self._write_edge(Edge(
                from_url=from_url, to_url=url, link_text=link_text, is_internal=is_internal,
                status_code=status_code, content_type=content_type, error=error,
                rendered_with_js=rendered_with_js, depth=depth,
            ))

    async def _flush_unresolved(self):
        """Safety net: if the crawl stopped early, resolve anything still
        pending as 'not crawled' so every discovered edge still ends up in
        the output instead of being silently dropped."""
        async with self._state_lock:
            remaining = list(self.pending_edges.items())
            self.pending_edges.clear()
        for to_url, edges in remaining:
            for from_url, link_text, depth, is_internal in edges:
                await self._write_edge(Edge(
                    from_url=from_url, to_url=to_url, link_text=link_text, is_internal=is_internal,
                    status_code=None, content_type="", error="not crawled (job stopped before reaching this URL)",
                    rendered_with_js=False, depth=depth,
                ))

    async def _write_edge(self, edge: Edge):
        async with self._csv_lock:
            self._csv_writer.writerow({
                "from_url": edge.from_url,
                "to_url": edge.to_url,
                "link_text": edge.link_text,
                "is_internal": edge.is_internal,
                "status_code": edge.status_code if edge.status_code is not None else "",
                "content_type": edge.content_type,
                "error": edge.error,
                "rendered_with_js": edge.rendered_with_js,
                "depth": edge.depth,
            })
            self._csv_file.flush()

    @property
    def csv_path(self) -> Path:
        return self._csv_path
