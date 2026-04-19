#!/usr/bin/env python3
"""Crawl and download all PDF files from DOJ Epstein Data Set 12 pages.

The DOJ site can block non-browser traffic on paginated pages. This script supports:
- http: crawl listing pages with urllib (fast, may be blocked)
- browser: crawl listing pages with Playwright + local Edge/Chrome
- auto: try http first, then browser fallback
"""

from __future__ import annotations

import argparse
import html
import time
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urljoin, urlparse
from urllib.request import Request, urlopen

BASE_URL = "https://www.justice.gov/epstein/doj-disclosures/data-set-12-files"
DATASET_PAGE_PATH = "/epstein/doj-disclosures/data-set-12-files"
DEFAULT_OUTPUT_DIR = Path("data/raw_pdfs")
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) DataSet12Crawler/2.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
DOWNLOAD_HEADERS = {
    "User-Agent": REQUEST_HEADERS["User-Agent"],
    "Accept": "application/pdf,*/*;q=0.8",
}
DEFAULT_BROWSER_PATHS = (
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
)


class NonPdfDownloadError(RuntimeError):
    """Raised when a download request does not return a real PDF payload."""


class AnchorHrefParser(HTMLParser):
    """Collect href values from all anchor tags in an HTML page."""

    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.hrefs.append(html.unescape(value).strip())


def fetch_text(url: str, timeout: int = 60) -> str:
    request = Request(url, headers=REQUEST_HEADERS)
    with urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def extract_hrefs(page_html: str) -> list[str]:
    parser = AnchorHrefParser()
    parser.feed(page_html)
    return parser.hrefs


def build_page_url(page: int) -> str:
    return f"{BASE_URL}?page={page}"


def is_dataset_pagination_link(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc.endswith("justice.gov") and parsed.path.rstrip("/") == DATASET_PAGE_PATH


def discover_max_page(page_html: str, current_page_url: str) -> int:
    max_page = 0
    for href in extract_hrefs(page_html):
        absolute = urljoin(current_page_url, href)
        if not is_dataset_pagination_link(absolute):
            continue
        query = parse_qs(urlparse(absolute).query)
        if "page" not in query or not query["page"]:
            continue
        try:
            page_number = int(query["page"][0])
        except ValueError:
            continue
        max_page = max(max_page, page_number)
    return max_page


def extract_pdf_links_from_html(page_html: str, current_page_url: str) -> set[str]:
    pdf_urls: set[str] = set()
    for href in extract_hrefs(page_html):
        if not href:
            continue
        absolute = urljoin(current_page_url, href).split("#", 1)[0]
        if absolute.lower().split("?", 1)[0].endswith(".pdf"):
            pdf_urls.add(absolute)
    return pdf_urls


def crawl_pdf_links_via_http() -> list[str]:
    first_page_url = build_page_url(0)
    first_page_html = fetch_text(first_page_url)
    max_page = discover_max_page(first_page_html, first_page_url)

    all_pdf_links: set[str] = set()
    for page in range(max_page + 1):
        page_url = build_page_url(page)
        page_html = first_page_html if page == 0 else fetch_text(page_url)
        page_links = extract_pdf_links_from_html(page_html, page_url)
        all_pdf_links.update(page_links)
        print(f"[http page {page}] found {len(page_links)} pdf links")

    return sorted(all_pdf_links)


def find_browser_executable(preferred_path: str | None) -> str | None:
    if preferred_path and Path(preferred_path).exists():
        return preferred_path

    for candidate in DEFAULT_BROWSER_PATHS:
        if Path(candidate).exists():
            return candidate
    return None


def extract_pdf_links_from_browser(page: Any) -> set[str]:
    links = page.evaluate(
        """
() => {
  const anchors = Array.from(document.querySelectorAll("a[href]"));
  const urls = anchors
    .map((a) => a.href)
    .filter((href) => /\\.pdf(?:[?#]|$)/i.test(href));
  return Array.from(new Set(urls));
}
"""
    )
    if not isinstance(links, list):
        return set()

    cleaned: set[str] = set()
    for link in links:
        if isinstance(link, str) and link:
            cleaned.add(link.split("#", 1)[0])
    return cleaned


def discover_max_page_from_browser(page: Any) -> int:
    page_values = page.evaluate(
        """
() => {
  const values = new Set([0]);
  const anchors = Array.from(document.querySelectorAll("a[href*='page=']"));
  for (const anchor of anchors) {
    const href = anchor.getAttribute("href") || "";
    const match = href.match(/[?&]page=(\\d+)/);
    if (match) {
      values.add(parseInt(match[1], 10));
    }
  }
  return Array.from(values);
}
"""
    )
    if not isinstance(page_values, list):
        return 0

    max_page = 0
    for value in page_values:
        try:
            max_page = max(max_page, int(value))
        except (TypeError, ValueError):
            continue
    return max_page


def wait_for_pdf_links_in_browser(
    page: Any,
    timeout_seconds: float,
    previous_links: set[str] | None = None,
) -> set[str]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        links = extract_pdf_links_from_browser(page)
        if links and (previous_links is None or links != previous_links):
            return links
        page.wait_for_timeout(750)
    raise RuntimeError(f"No PDF links found in browser page: {page.url}")


def click_next_page_in_browser(page: Any) -> None:
    next_link = page.locator("nav[aria-label='Pagination'] a[aria-label='Next page']")
    if next_link.count() == 0:
        raise RuntimeError("Could not find next page link in browser mode.")

    next_link.first.click()
    try:
        page.wait_for_load_state("domcontentloaded", timeout=15_000)
    except Exception:
        # Some updates are done via AJAX and do not trigger full navigations.
        pass


def crawl_pdf_links_via_browser(
    *,
    headless: bool,
    browser_path: str | None,
    wait_seconds: float,
) -> list[str]:
    try:
        from playwright.sync_api import Error as PlaywrightError, sync_playwright
    except Exception as exc:
        raise RuntimeError(
            "Playwright is not available. Install it with: python -m pip install playwright"
        ) from exc

    executable_path = find_browser_executable(browser_path)
    launch_kwargs: dict[str, Any] = {"headless": headless}
    if executable_path:
        launch_kwargs["executable_path"] = executable_path

    all_pdf_links: set[str] = set()

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(**launch_kwargs)
            context = browser.new_context()
            page = context.new_page()
            page.set_default_timeout(90_000)

            page.goto(build_page_url(0), wait_until="domcontentloaded")
            page_links = wait_for_pdf_links_in_browser(page, wait_seconds)
            max_page = discover_max_page_from_browser(page)
            all_pdf_links.update(page_links)
            print(f"[browser page 0] found {len(page_links)} pdf links")

            for page_num in range(1, max_page + 1):
                previous_page_links = set(page_links)
                click_next_page_in_browser(page)
                page_links = wait_for_pdf_links_in_browser(
                    page,
                    wait_seconds,
                    previous_links=previous_page_links,
                )
                all_pdf_links.update(page_links)
                print(f"[browser page {page_num}] found {len(page_links)} pdf links")

            context.close()
            browser.close()

    except PlaywrightError as exc:
        raise RuntimeError(f"Browser crawl failed: {exc}") from exc

    return sorted(all_pdf_links)


def crawl_all_pdf_links(
    *,
    mode: str,
    headless: bool,
    browser_path: str | None,
    browser_wait_seconds: float,
) -> list[str]:
    if mode == "http":
        return crawl_pdf_links_via_http()
    if mode == "browser":
        return crawl_pdf_links_via_browser(
            headless=headless,
            browser_path=browser_path,
            wait_seconds=browser_wait_seconds,
        )

    try:
        return crawl_pdf_links_via_http()
    except Exception as exc:
        print(f"HTTP crawl failed ({exc}). Retrying with browser mode.")
        return crawl_pdf_links_via_browser(
            headless=headless,
            browser_path=browser_path,
            wait_seconds=browser_wait_seconds,
        )


def iter_download_jobs(links: Iterable[str], output_dir: Path) -> Iterable[tuple[str, Path]]:
    for link in links:
        parsed = urlparse(link)
        filename = Path(parsed.path).name
        if not filename:
            continue
        yield link, output_dir / filename


def bytes_look_like_pdf(payload: bytes) -> bool:
    return len(payload) >= 5 and payload[:5] == b"%PDF-"


def path_has_pdf_signature(path: Path) -> bool:
    try:
        with path.open("rb") as file_obj:
            return file_obj.read(5) == b"%PDF-"
    except OSError:
        return False


def looks_like_age_gate_html(payload: bytes) -> bool:
    snippet = payload[:4096].decode("utf-8", errors="ignore").lower()
    return "age-verify" in snippet and "18 years" in snippet


class BrowserDownloadSession:
    """Persistent browser-backed downloader used when direct HTTP gets age-gated."""

    def __init__(self, *, headless: bool, browser_path: str | None) -> None:
        self.headless = headless
        self.browser_path = browser_path
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None
        self._age_verified = False

    def start(self) -> "BrowserDownloadSession":
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            raise RuntimeError(
                "Playwright is not available. Install it with: python -m pip install playwright"
            ) from exc

        executable_path = find_browser_executable(self.browser_path)
        launch_kwargs: dict[str, Any] = {"headless": self.headless}
        if executable_path:
            launch_kwargs["executable_path"] = executable_path

        self._playwright = sync_playwright().start()
        try:
            self._browser = self._playwright.chromium.launch(**launch_kwargs)
            self._context = self._browser.new_context()
            self._page = self._context.new_page()
            self._page.set_default_timeout(90_000)
        except Exception:
            self.close()
            raise

        return self

    def close(self) -> None:
        if self._context is not None:
            self._context.close()
            self._context = None
        if self._browser is not None:
            self._browser.close()
            self._browser = None
        if self._playwright is not None:
            self._playwright.stop()
            self._playwright = None
        self._page = None
        self._age_verified = False

    def _ensure_age_verified(self, destination_url: str) -> None:
        if self._page is None:
            raise RuntimeError("Browser download session is not initialized.")
        if self._age_verified:
            return

        self._page.goto(destination_url, wait_until="domcontentloaded")
        if "age-verify" in self._page.url:
            yes_button = self._page.locator("#age-button-yes")
            if yes_button.count() == 0:
                raise RuntimeError("Age verification page loaded, but Yes button was not found.")
            yes_button.first.click()
            self._page.wait_for_timeout(750)

        self._age_verified = True

    def fetch_pdf_bytes(self, url: str, timeout: int = 120) -> bytes:
        if self._context is None:
            raise RuntimeError("Browser download session is not initialized.")

        self._ensure_age_verified(url)
        response = self._context.request.get(url, headers=DOWNLOAD_HEADERS, timeout=timeout * 1000)
        payload = response.body()
        if bytes_look_like_pdf(payload):
            return payload

        # Retry once after reapplying age verification in case the cookie expired.
        self._age_verified = False
        self._ensure_age_verified(url)
        response = self._context.request.get(url, headers=DOWNLOAD_HEADERS, timeout=timeout * 1000)
        payload = response.body()
        if bytes_look_like_pdf(payload):
            return payload

        content_type = response.headers.get("content-type", "")
        if looks_like_age_gate_html(payload):
            raise NonPdfDownloadError(
                f"Browser request still returned age verification HTML for {url}."
            )
        raise NonPdfDownloadError(
            f"Browser request returned non-PDF content-type={content_type!r} for {url}."
        )


def download_pdf_with_browser_session(
    url: str,
    destination: Path,
    overwrite: bool,
    browser_session: BrowserDownloadSession,
    timeout: int = 120,
) -> str:
    if destination.exists() and not overwrite and path_has_pdf_signature(destination):
        return "skipped"

    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = browser_session.fetch_pdf_bytes(url, timeout=timeout)
    with destination.open("wb") as file_obj:
        file_obj.write(payload)
    return "downloaded"


def download_pdf(url: str, destination: Path, overwrite: bool, timeout: int = 120) -> str:
    if destination.exists() and not overwrite:
        if path_has_pdf_signature(destination):
            return "skipped"
        print(f"Existing file is not a valid PDF and will be re-downloaded: {destination.name}")

    destination.parent.mkdir(parents=True, exist_ok=True)
    request = Request(url, headers=DOWNLOAD_HEADERS)
    with urlopen(request, timeout=timeout) as response:
        payload = response.read()
        content_type = response.headers.get("Content-Type", "")
        final_url = response.geturl()

    if not bytes_look_like_pdf(payload):
        if looks_like_age_gate_html(payload):
            raise NonPdfDownloadError(
                f"Age verification HTML was returned (final URL: {final_url})."
            )
        raise NonPdfDownloadError(
            f"Non-PDF response received (content-type={content_type!r}, final URL: {final_url})."
        )

    with destination.open("wb") as file_obj:
        file_obj.write(payload)
    return "downloaded"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl and download all PDF files from DOJ Epstein Data Set 12 pages."
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where PDF files will be saved (default: data/raw_pdfs)",
    )
    parser.add_argument(
        "--crawl-mode",
        choices=("auto", "http", "browser"),
        default="auto",
        help="How to crawl listing pages (default: auto)",
    )
    parser.add_argument(
        "--browser-path",
        default=None,
        help="Optional path to browser executable (Edge/Chrome).",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser in headless mode (disabled by default).",
    )
    parser.add_argument(
        "--browser-wait-seconds",
        type=float,
        default=30.0,
        help="Max seconds to wait for PDF links on each browser page (default: 30).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-download files even if they already exist.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only list discovered PDF links without downloading files.",
    )
    parser.add_argument(
        "--download-delay",
        type=float,
        default=0.0,
        help="Optional delay in seconds between downloads (default: 0).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)

    try:
        links = crawl_all_pdf_links(
            mode=args.crawl_mode,
            headless=args.headless,
            browser_path=args.browser_path,
            browser_wait_seconds=args.browser_wait_seconds,
        )
    except (HTTPError, URLError, TimeoutError, RuntimeError) as exc:
        print(f"Failed to crawl listing pages: {exc}")
        return 1

    print(f"Discovered {len(links)} unique PDF links.")

    if args.dry_run:
        for link in links:
            print(link)
        return 0

    downloaded = 0
    downloaded_via_browser_fallback = 0
    skipped = 0
    failed = 0
    browser_fallback_disabled = False
    browser_session: BrowserDownloadSession | None = None

    jobs = list(iter_download_jobs(links, output_dir))
    total = len(jobs)
    try:
        for index, (url, destination) in enumerate(jobs, start=1):
            try:
                status = download_pdf(url, destination, overwrite=args.overwrite)
                if status == "downloaded":
                    downloaded += 1
                    print(f"[{index}/{total}] downloaded {destination.name}")
                else:
                    skipped += 1
                    print(f"[{index}/{total}] skipped {destination.name} (already exists)")
            except NonPdfDownloadError as direct_exc:
                if browser_fallback_disabled:
                    failed += 1
                    print(f"[{index}/{total}] failed {destination.name}: {direct_exc}")
                    continue

                try:
                    if browser_session is None:
                        browser_session = BrowserDownloadSession(
                            headless=args.headless,
                            browser_path=args.browser_path,
                        ).start()
                        print("Direct download returned non-PDF content; browser fallback enabled.")

                    status = download_pdf_with_browser_session(
                        url,
                        destination,
                        overwrite=True,
                        browser_session=browser_session,
                        timeout=180,
                    )
                    if status == "downloaded":
                        downloaded += 1
                        downloaded_via_browser_fallback += 1
                        print(f"[{index}/{total}] downloaded {destination.name} (browser fallback)")
                    else:
                        skipped += 1
                        print(f"[{index}/{total}] skipped {destination.name} (already exists)")
                except (HTTPError, URLError, TimeoutError, RuntimeError) as fallback_exc:
                    if browser_session is None:
                        browser_fallback_disabled = True
                    failed += 1
                    print(
                        f"[{index}/{total}] failed {destination.name}: {direct_exc}; "
                        f"browser fallback error: {fallback_exc}"
                    )
            except (HTTPError, URLError, TimeoutError) as exc:
                failed += 1
                print(f"[{index}/{total}] failed {destination.name}: {exc}")

            if args.download_delay > 0 and index < total:
                time.sleep(args.download_delay)
    finally:
        if browser_session is not None:
            browser_session.close()

    print(
        "Done. "
        f"total={total}, downloaded={downloaded}, "
        f"browser_fallback_downloaded={downloaded_via_browser_fallback}, "
        f"skipped={skipped}, failed={failed}."
    )
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
