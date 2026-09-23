# Link Crawler

A FastAPI + Uvicorn tool that crawls a website like a human browser (rotating
user-agents, randomized delays, robots.txt-aware), records every link edge
(from-page → to-page, status code, internal/external), optionally renders
JS-heavy pages with a headless browser, and exports results to CSV/XLSX.
Start/Stop is controlled from a simple web UI.

## 1. Install

```bash
cd rog-talon-js-render-link-crawler
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Needed only if you want JS-rendered crawling (recommended to install anyway):
playwright install chromium
```

## 2. Run

```bash
uvicorn app.main:app --reload --port 8000
```

Open **http://localhost:8000** in your browser.

## 3. Use it

1. Enter the start URL (e.g. `https://example.com`).
2. Set **max pages** (up to 50,000), **concurrency** (parallel workers —
   15–30 is a good starting point; higher = faster but more load on the
   target site and your machine), and JS rendering mode:
   - **Off** — fastest, static HTML only (BeautifulSoup).
   - **Auto** — fetches static HTML first; if the page looks like a
     JS-only SPA (very little text, a `#root`/`#app` div, React/Angular/Next
     markers), it re-renders that one page with headless Chromium.
   - **Always** — every internal page is rendered with headless Chromium.
     Much slower and heavier; use for known SPA sites only.
3. Click **Start Crawl**. Progress (pages crawled, links found, queue size,
   errors, elapsed time) updates live.
4. Click **Stop** any time to halt the crawl — partial results are still
   downloadable.
5. When finished (or stopped), download **XLSX** or **CSV**.

## Output columns

| Column | Meaning |
|---|---|
| `from_url` | Page the link was found on |
| `to_url` | Link target |
| `link_text` | Anchor text |
| `is_internal` | Same domain as the start URL |
| `status_code` | HTTP status of `to_url` |
| `content_type` | Response `Content-Type` of `to_url` |
| `error` | Fetch error / robots.txt block, if any |
| `rendered_with_js` | Whether `to_url` needed headless-browser rendering |
| `depth` | Link distance (clicks) from the start URL |

## Performance & scale notes

- Results are streamed straight to a CSV file on disk as they're found
  (not held in memory), so it stays fast and stable even for 50,000 pages
  with millions of link edges. XLSX is generated from that CSV on demand,
  also streamed via openpyxl's write-only mode.
- Internal pages are fully fetched and parsed for further links. External
  links (and non-HTML internal assets like PDFs/images) get a lightweight
  `HEAD` (falling back to `GET`) request just to capture a status code —
  they are not crawled further.
- `min_delay`/`max_delay` add a small randomized pause per worker between
  requests, and the User-Agent is rotated, to behave more like a real
  browsing session rather than a hammering bot. Increase the delay or lower
  concurrency if you're crawling a site you don't control, to be a good
  citizen (and check that site's terms of service / robots.txt).
- `respect_robots.txt` is on by default — turn it off only for sites you
  own or have explicit permission to crawl aggressively.
- JS rendering is much slower per page (spins up a real Chromium tab) —
  use `auto` or `off` for large crawls, and reserve `always` for smaller,
  React/Vue/Angular-heavy sites.

## API (for scripting / integration)

- `POST /api/crawl/start` — body matches the form fields; returns `job_id`
- `POST /api/crawl/stop/{job_id}`
- `GET /api/crawl/status/{job_id}`
- `GET /api/crawl/download/{job_id}?fmt=xlsx|csv`
- `GET /api/crawl/jobs` — list all jobs in this server's memory

## Notes / limitations

- Jobs live in server memory (a Python dict); restarting the server clears
  the job list, though the CSV/XLSX files remain on disk under `data/`.
- This is single-process; for very high concurrency across many crawls
  simultaneously, run multiple Uvicorn workers behind a reverse proxy and
  give each its own `data/` volume, or move job state to Redis/a DB.
