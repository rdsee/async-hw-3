import time
import json

import aiohttp
import asyncio
import logging

from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urldefrag


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)

logger = logging.getLogger(__name__)


def log_section(title: str) -> None:
    logger.info("")
    logger.info("=" * 78)
    logger.info(title)
    logger.info("=" * 78)


class HTMLParser:
    def __init__(self):
        pass

    async def parse_html(self, html: str, url: str) -> dict:
        soup = BeautifulSoup(html, "lxml")
        metadata = self.extract_metadata(soup)
        text = self.extract_text(soup)
        links = self.extract_links(soup, url)
        images = self.extract_images(soup)
        headings = self.extract_headings(soup)
        tables = self.extract_tables(soup)
        lists = self.extract_lists(soup)

        return {
            "url": url,
            "title": metadata["title"],
            "metadata": metadata,
            "text": text,
            "links": links,
            "images": images,
            "headings": headings,
            "tables": tables,
            "lists": lists
        }

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        links_tags = soup.find_all("a", href=True)
        links = []
        seen = set()

        for i in links_tags:
            link = i.get("href", "").strip()

            if not link:
                continue

            if link.startswith(("#", "mailto:", "javascript:", "tel:")):
                continue

            absolute_url = urljoin(base_url, link)
            absolute_url, _ = urldefrag(absolute_url)

            parsed_url = urlparse(absolute_url)

            if parsed_url.scheme not in ("http", "https"):
                continue

            if not parsed_url.netloc:
                continue

            if absolute_url not in seen:
                seen.add(absolute_url)
                links.append(absolute_url)

        return links

    def extract_text(self, soup: BeautifulSoup, selector: str = None) -> str:
        if selector:
            target = soup.select_one(selector)

            if target is None:
                logger.warning("Selector not found: %s", selector)
                return ""
        else:
            target = soup

        return target.get_text(" ", strip=True)

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        title_tag = soup.find("title")
        if title_tag is not None:
            title = title_tag.get_text(strip=True)
        else:
            logger.warning("Title not found")
            title = ""

        description_tag = soup.find("meta", attrs={"name":"description"})
        if description_tag is not None:
            description = description_tag.get("content", "")
        else:
            logger.warning("Description not found")
            description = ""

        keywords_tag = soup.find("meta", attrs={"name": "keywords"})
        if keywords_tag is not None:
            keywords = keywords_tag.get("content", "")
        else:
            logger.warning("Keyword not found")
            keywords = ""

        return {
            "title": title,
            "description": description,
            "keywords": keywords,
        }

    def extract_images(self, soup: BeautifulSoup) -> list[dict]:
        images = []
        images_tag = soup.find_all("img", src=True)

        if not images_tag:
            logger.warning("Image not found")

        for img in images_tag:
            src = img.get("src")
            alt = img.get("alt")

            if alt is None:
                alt = ""

            img_dict = {"src": src, "alt": alt}
            images.append(img_dict)

        return images

    def extract_headings(self, soup: BeautifulSoup) -> list[dict]:
        heading = []

        heading_tags = soup.find_all(["h1", "h2", "h3"])

        for head in heading_tags:
            text = head.get_text(" ", strip=True)

            head_dict = {
                "level": head.name,
                "text": text
            }

            heading.append(head_dict)
        return heading


    def extract_tables(self, soup: BeautifulSoup) -> list[dict]:
        tables = []

        for table_tag in soup.find_all("table"):
            headers = []
            rows = []

            for row_tag in table_tag.find_all("tr"):
                cell_tags = row_tag.find_all(["th", "td"])

                row_values = [
                    cell.get_text(" ", strip=True)
                    for cell in cell_tags
                ]

                if not row_values:
                    continue

                header_tags = row_tag.find_all("th")

                if header_tags and not headers:
                    headers = [
                        header.get_text(" ", strip=True)
                        for header in header_tags
                    ]
                else:
                    rows.append(row_values)

            tables.append({
                "headers": headers,
                "rows": rows,
            })

        return tables


    def extract_lists(self, soup: BeautifulSoup) -> list[dict]:
        lists = []
        list_tags = soup.find_all(["ul", "ol"])

        for list_tag in list_tags:
            items = []
            li = list_tag.find_all("li")

            for li_tag in li:
                text = li_tag.get_text(" ", strip=True)
                items.append(text)

            lists.append({
                "type": list_tag.name,
                "items": items
            })

        return lists

class CrawlerQueue:
    def __init__(self):
        self.queued_urls = set()
        self.queue = asyncio.PriorityQueue()
        self.counter = 0
        self.processed_urls = {}
        self.failed_urls = {}
        self.deep_urls = {}


    @staticmethod
    def normalize_url(url: str) -> str:
        parsed = urlparse(url)
        path = parsed.path or "/"
        return parsed._replace(path=path, fragment="").geturl()

    def add_url(self, url: str, priority: int = 0, depth: int = 0) -> bool:
        url = self.normalize_url(url)

        if url in self.queued_urls:
            return False

        self.queued_urls.add(url)
        self.counter += 1
        self.queue.put_nowait((priority, self.counter, url))
        self.deep_urls[url] = depth
        return True

    def deepness(self, url: str) -> int | None:
        return self.deep_urls.get(url)

    async def get_next(self) -> str | None:
        if self.queue.empty():
            return None

        priority, counter, url = await self.queue.get()
        return url


    def mark_processed(self, url: str) -> None:
        self.processed_urls[url] = True


    def mark_failed(self, url: str, error: str ) -> None:
        self.failed_urls[url] = error


    def get_stats(self) -> dict:
        return {
            "queued": self.queue.qsize(),
            "processed": len(self.processed_urls),
            "failed": len(self.failed_urls),
        }

class SemaphoreManager:
    def __init__(self, global_limit, domain_limit):
        self.global_limit = global_limit
        self.domain_limit = domain_limit
        self.domain_semaphores = {}
        self.active_tasks = 0

        self.semaphore = asyncio.Semaphore(global_limit)

    def get_domain_semaphore(self, url: str):
        domain = urlparse(url).hostname
        if domain in self.domain_semaphores:
            logger.info(f"Existing semaphore: {domain}")
            return self.domain_semaphores[domain]
        else:
            domain_semaphore = asyncio.Semaphore(self.domain_limit)
            self.domain_semaphores[domain] = domain_semaphore

        return self.domain_semaphores[domain]

    async def acquire(self, url):
        domain_semaphore = self.get_domain_semaphore(url)

        await self.semaphore.acquire()
        await domain_semaphore.acquire()

        self.active_tasks += 1

    def release(self, url):
        domain_semaphore = self.get_domain_semaphore(url)

        self.semaphore.release()
        domain_semaphore.release()

        self.active_tasks -= 1

class AsyncCrawler:
    def __init__(self, max_concurrent: int = 4, domain_limit: int = 2):
        self.max_concurrency = max_concurrent
        self.semaphore_manager = SemaphoreManager(max_concurrent, domain_limit)
        self.session = None
        self.parser = HTMLParser()
        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}


    async def fetch_url(self, url: str) -> str | None:
        if self.session is None:

            timeout = aiohttp.ClientTimeout(connect=10, sock_read=2)
            self.session = aiohttp.ClientSession(timeout=timeout)

        logger.info("[DOWNLOAD] WAITING  | %s", url)

        await self.semaphore_manager.acquire(url)
        logger.info("[DOWNLOAD] STARTED  | %s", url)

        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                text = await response.text()
                logger.info("[DOWNLOAD] FINISHED | %s", url)
                return text
        except asyncio.TimeoutError:
            logger.warning("[DOWNLOAD] TIMEOUT  | %s", url)
        except aiohttp.ClientResponseError:
            logger.error("[DOWNLOAD] HTTP ERROR | %s", url)
        except aiohttp.ClientError:
            logger.error("[DOWNLOAD] NETWORK ERROR | %s", url)
        finally:
            self.semaphore_manager.release(url)
        return None


    async def fetch_urls(self, urls: list[str]) -> dict[str, str | None]:
        result = await asyncio.gather(*(self.fetch_url(url) for url in urls))
        return dict(zip(urls, result))


    async def fetch_and_parse(self, url: str) -> dict | None:
        html = await self.fetch_url(url)

        if html is None:
            self.failed_urls[url] = "Failed to fetch"
            logger.warning("Failed to fetch %s", url)
            return None
        else:
            result = await self.parser.parse_html(html, url)
            return result

    async def crawl(
            self,
            start_urls: list[str],
            max_pages: int = 15,
            same_domain_only: bool = False,
            max_depth: int = 2,
            include_patterns: list[str] | None = None,
            exclude_patterns: list[str] | None = None,
    ):
        start = time.time()
        queue = CrawlerQueue()

        if include_patterns is None:
            include_patterns = []

        if exclude_patterns is None:
            exclude_patterns = []

        allowed_domains = {
            urlparse(url).hostname
            for url in start_urls
            if urlparse(url).hostname
        }

        for url in start_urls:
            queue.add_url(url, depth=0)

        results = {}

        async def worker(url: str):
            page = await self.fetch_and_parse(url)
            return page

        while len(results) < max_pages:
            batch_urls = []

            while len(batch_urls) < self.max_concurrency and len(results) + len(batch_urls) < max_pages:
                next_url = await queue.get_next()

                if next_url is None:
                    break

                if next_url in self.visited_urls:
                    continue

                self.visited_urls.add(next_url)
                batch_urls.append(next_url)

            if not batch_urls:
                break

            tasks = [
                asyncio.create_task(worker(url))
                for url in batch_urls
            ]

            pages = await asyncio.gather(*tasks)

            for url, page in zip(batch_urls, pages):
                current_depth = queue.deepness(url)

                if page is None:
                    continue

                results[url] = page
                queue.mark_processed(url)
                self.processed_urls[url] = page

                end = time.time()
                time_progress = end - start
                speed = len(results) / time_progress if time_progress > 0 else 0

                logger.info(f"[CRAWLER] FINISHED FOR {time_progress:.2f} seconds")
                logger.info(
                    f"[CRAWLER] CRAWLED {len(results)} PAGES. "
                    f"AVG SPEED: {speed:.2f} PAGES PER SECOND"
                )
                logger.info(f"[CRAWLER] {queue.get_stats()["queued"]} URLS ARE IN QUEUE")
                logger.info(f"[CRAWLER] QUANTITY OF ERRORS: {len(self.failed_urls)} \n")

                for link in results[url]["links"]:
                    if same_domain_only:
                        link_domain = urlparse(link).hostname
                        if link_domain not in allowed_domains:
                            continue

                    if any(pattern in link for pattern in exclude_patterns):
                        continue

                    if include_patterns and not any(pattern in link for pattern in include_patterns):
                        continue

                    if current_depth < max_depth:
                        queue.add_url(link, depth=current_depth + 1)
        return results



    async def close(self) -> None:
        if self.session is not None and not self.session.closed:
            await self.session.close()
            self.session = None


async def test_queue() -> None:
    queue = CrawlerQueue()

    assert queue.add_url("https://example.com/a", priority=1)
    assert queue.add_url("https://example.com/b", priority=0)
    assert not queue.add_url("https://example.com/a", priority=0)

    first = await queue.get_next()
    assert first == "https://example.com/b"
    assert queue.deepness(first) == 0
    queue.mark_processed(first)

    second = await queue.get_next()
    assert second == "https://example.com/a"
    queue.mark_failed(second, "test error")

    assert queue.get_stats() == {
        "queued": 0,
        "processed": 1,
        "failed": 1,
    }

    logger.info("[CHECK] CrawlerQueue: PASSED")


async def main():
    await test_queue()

    crawler = AsyncCrawler(max_concurrent=4, domain_limit=2)
    crawl_config = {
        "start_urls": ["https://www.python.org"],
        "max_pages": 15,
        "max_depth": 2,
        "same_domain_only": True,
        "include_patterns": None,
        "exclude_patterns": [],
    }

    try:
        crawl_results = await crawler.crawl(**crawl_config)

        with open("crawl_results.json", "w", encoding="utf-8") as file:
            json.dump(
                crawl_results,
                file,
                ensure_ascii=False,
                indent=2,
            )

        logger.info("[RESULT] Saved pages: %d", len(crawl_results))
        logger.info("[RESULT] Visited URLs: %d", len(crawler.visited_urls))
        logger.info("[RESULT] Processed URLs: %d", len(crawler.processed_urls))
        logger.info("[RESULT] Failed URLs: %d", len(crawler.failed_urls))
        logger.info("[RESULT] File: crawl_results.json")
    finally:
        await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
