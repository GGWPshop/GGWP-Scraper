import asyncio
import json
import logging
import random
import re
import time
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from ..config import Settings
from .parsers import parse_gift_page, parse_search_html, _TRAILING_NUM_RE
from ..schemas import ParsedGift

logger = logging.getLogger('scraper.fragment')

# Реалистичные User-Agent для ротации (Chrome/Firefox, разные ОС)
_USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0',
]


class FragmentTransientError(RuntimeError):
    pass


class FragmentClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.base_url = settings.fragment_base_url.rstrip('/')
        self.timeout = httpx.Timeout(settings.scraper_http_timeout)
        self._client = httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=True,
            headers={
                'User-Agent': random.choice(_USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'DNT': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
            },
        )
        self._request_count = 0

    async def _request_with_retry(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        last_exc: Exception | None = None
        transient_failure = False
        max_attempts = self.settings.scraper_http_retries
        for attempt in range(1, max_attempts + 1):
            try:
                # Ротируем User-Agent каждые 50 запросов или при повторной попытке
                self._request_count += 1
                if attempt > 1 or self._request_count % 50 == 0:
                    headers = kwargs.get('headers', {})
                    headers['User-Agent'] = random.choice(_USER_AGENTS)
                    kwargs['headers'] = headers
                response = await self._client.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except Exception as exc:
                last_exc = exc
                http_status: int | None = None
                is_transient = isinstance(exc, httpx.RequestError)
                if isinstance(exc, httpx.HTTPStatusError):
                    http_status = exc.response.status_code
                    is_transient = http_status >= 500 or http_status == 429
                transient_failure = transient_failure or is_transient
                backoff = min(
                    self.settings.scraper_http_backoff_base_seconds * (2 ** (attempt - 1)),
                    self.settings.scraper_http_backoff_max_seconds,
                )
                jitter = random.uniform(0, 0.3)
                extra_wait = 1.5 if http_status == 429 else 0.0
                logger.warning(
                    'event=http_attempt_failed attempt=%s/%s url=%s status=%s exc=%s sleep=%.1fs',
                    attempt, max_attempts, url, http_status or '-', type(exc).__name__, backoff + extra_wait,
                )
                await asyncio.sleep(backoff + extra_wait + jitter)
        if last_exc is not None:
            logger.error(
                'event=http_all_attempts_failed url=%s attempts=%s transient=%s exc=%s',
                url, max_attempts, transient_failure, type(last_exc).__name__,
            )
            if transient_failure:
                raise FragmentTransientError(f'Transient request failure for {method} {url}') from last_exc
            raise last_exc
        raise RuntimeError('Unexpected retry flow')

    async def _reset_client(self) -> None:
        """Recreate HTTP client to clear accumulated session state and cookies."""
        try:
            await self._client.aclose()
        except Exception:
            pass
        self._client = httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=True,
            headers={
                'User-Agent': random.choice(_USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'DNT': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
            },
        )
        self._request_count = 0

    async def _init_session(self, page_path: str = '/gifts?sort=listed&filter=sale') -> tuple[str, dict[str, str], str]:
        # Fresh client clears cookies/session from previous runs to avoid Fragment IP blocking
        await self._reset_client()
        gifts_url = f'{self.base_url}{page_path}'
        response = await self._request_with_retry('GET', gifts_url)
        html = response.text
        cookies: dict[str, str] = dict(response.cookies)

        # If override is configured, skip hash extraction entirely
        if self.settings.fragment_api_url_override:
            api_url = self.settings.fragment_api_url_override
            if api_url.startswith('/'):
                api_url = f'{self.base_url}{api_url}'
            logger.info('event=api_url_from_override api_url=%s', api_url)
            return api_url, cookies, html

        api_url = None
        for pattern in (
            r'apiUrl":"(?P<url>[^\"]+)"',
            r'apiUrl:\s*"(?P<url>[^\"]+)"',
            r'api_url:\s*"(?P<url>[^\"]+)"',
            r'"api"\s*:\s*"(?P<url>[^\"]+)"',
        ):
            api_match = re.search(pattern, html)
            if api_match:
                api_url = api_match.group('url').replace('\\/', '/')
                break
        if not api_url:
            snippet = html[:600].replace('\n', ' ')
            logger.error(
                'event=api_url_not_found url=%s status=%s html_len=%s snippet="%s"',
                gifts_url, response.status_code, len(html), snippet,
            )
            raise FragmentTransientError('Fragment apiUrl hash was not found')
        if api_url.startswith('/'):
            api_url = f'{self.base_url}{api_url}'

        return api_url, cookies, html

    def _extract_collections_from_html(self, html: str) -> list[str]:
        soup = BeautifulSoup(html, 'html.parser')
        result: list[str] = []
        seen: set[str] = set()

        for link in soup.select('a[href^="/gifts/"]'):
            href = (link.get('href') or '').strip()
            parsed = urlparse(href)
            path = parsed.path.strip('/')
            parts = path.split('/')
            if len(parts) != 2 or parts[0] != 'gifts':
                continue
            slug = parts[1].strip().lower()
            if not slug:
                continue
            if slug not in seen:
                seen.add(slug)
                result.append(slug)

        regex_matches = re.findall(r'href="\\/gifts\\/([a-zA-Z0-9_-]+)"', html)
        for raw in regex_matches:
            slug = raw.strip().lower()
            if slug and slug not in seen:
                seen.add(slug)
                result.append(slug)

        return result

    async def _search_page(
        self,
        api_url: str,
        cookies: dict[str, str],
        offset_id: str | None,
        collection: str | None,
    ) -> tuple[list[str], str | None]:
        payload = {
            'method': 'searchAuctions',
            'type': 'gifts',
            'sort': 'listed',
            'filter': 'sale',
            'query': '',
            'collection': collection or '',
        }
        if offset_id:
            payload['offset_id'] = offset_id

        referer = f'{self.base_url}/gifts?sort=listed&filter=sale'
        if collection:
            referer = f'{self.base_url}/gifts/{collection}?sort=listed&filter=sale'

        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': referer,
            'Origin': self.base_url,
        }

        response = await self._request_with_retry(
            'POST',
            api_url,
            data=payload,
            headers=headers,
            cookies=cookies,
        )
        data = response.json()
        if isinstance(data, list):
            if len(data) == 0:
                return [], None
            first_dict = next((item for item in data if isinstance(item, dict)), None)
            if not first_dict:
                raise RuntimeError(f'searchAuctions returned non-dict list payload: {str(data)[:400]}')
            data = first_dict
        if not data.get('ok'):
            raise RuntimeError(f'searchAuctions bad response: {json.dumps(data)[:400]}')

        next_offset = data.get('next_offset') or data.get('nextOffset') or data.get('next')
        html_chunk = data.get('html')
        if not html_chunk:
            # Fragment returns partial payloads for load-more: body + foot.
            body = data.get('body', '')
            foot = data.get('foot', '')
            html_chunk = f'{body}{foot}'
        if not html_chunk:
            raise RuntimeError(f'searchAuctions missing html chunk: {json.dumps(data)[:400]}')

        slugs, html_offset = parse_search_html(html_chunk)
        if not next_offset:
            next_offset = html_offset
        return slugs, next_offset

    async def fetch_for_sale_slugs(
        self,
        collection: str | None = None,
        *,
        api_url: str | None = None,
        cookies: dict[str, str] | None = None,
    ) -> list[str]:
        if not api_url or not cookies:
            api_url, cookies, _ = await self._init_session()
        all_slugs: list[str] = []
        seen: set[str] = set()
        seen_offsets: set[str] = set()

        next_offset: str | None = None
        page_idx = 0
        stale_pages = 0
        stale_limit = max(3, int(self.settings.scraper_stale_pages_limit))
        log_every = max(1, int(self.settings.scraper_log_every_n_pages))
        logger.info('event=pagination_start collection=%s', collection or 'all')
        while True:
            if self.settings.scraper_max_pages > 0 and page_idx >= self.settings.scraper_max_pages:
                logger.info('event=pagination_stop collection=%s reason=max_pages page=%s', collection or 'all', page_idx)
                break
            if next_offset and next_offset in seen_offsets:
                logger.warning('event=pagination_stop collection=%s reason=repeating_offset', collection or 'all')
                break
            if next_offset:
                seen_offsets.add(next_offset)
            page_idx += 1
            try:
                slugs, next_offset = await self._search_page(api_url, cookies, next_offset, collection)
            except Exception:
                logger.exception('event=search_api_failed collection=%s fallback=html', collection or 'all')
                page_path = '/gifts?sort=listed&filter=sale'
                if collection:
                    page_path = f'/gifts/{collection}?sort=listed&filter=sale'
                response = await self._request_with_retry('GET', f'{self.base_url}{page_path}')
                slugs, next_offset = parse_search_html(response.text)
            if not slugs:
                logger.info('event=pagination_stop collection=%s reason=no_slugs page=%s', collection or 'all', page_idx)
                break
            before_count = len(all_slugs)
            for slug in slugs:
                if slug not in seen:
                    seen.add(slug)
                    all_slugs.append(slug)
            if len(all_slugs) == before_count:
                stale_pages += 1
            else:
                stale_pages = 0
            if page_idx == 1 or page_idx % log_every == 0 or not next_offset:
                logger.info(
                    'event=pagination_page collection=%s page=%s slugs=%s total=%s next_offset=%s',
                    collection or 'all',
                    page_idx,
                    len(slugs),
                    len(all_slugs),
                    next_offset or '-',
                )
            if stale_pages >= stale_limit:
                logger.warning(
                    'event=pagination_stop collection=%s reason=stale_pages_%s',
                    collection or 'all',
                    stale_limit,
                )
                break
            if not next_offset:
                logger.info('event=pagination_stop collection=%s reason=no_next_offset page=%s', collection or 'all', page_idx)
                break
            if self.settings.scraper_page_delay_seconds > 0:
                await asyncio.sleep(self.settings.scraper_page_delay_seconds)

        logger.info('event=pagination_finished collection=%s pages=%s total=%s', collection or 'all', page_idx, len(all_slugs))
        return all_slugs

    async def fetch_all_for_sale_slugs(self) -> tuple[list[str], list[str]]:
        api_url, cookies, html = await self._init_session()
        collections = self._extract_collections_from_html(html)
        logger.info('event=collections_discovered count=%s', len(collections))

        all_slugs: list[str] = []
        seen: set[str] = set()

        strategy = self.settings.scraper_collection_strategy.lower().strip()
        global_collections: set[str] = set()

        if strategy in {'all', 'global_only'}:
            global_slugs = await self.fetch_for_sale_slugs(None, api_url=api_url, cookies=cookies)
            global_collections = {_TRAILING_NUM_RE.sub('', slug) for slug in global_slugs if _TRAILING_NUM_RE.search(slug)}
            for slug in global_slugs:
                if slug not in seen:
                    seen.add(slug)
                    all_slugs.append(slug)
            if global_collections:
                merged = {slug for slug in collections}
                merged.update(global_collections)
                collections = sorted(merged)
                logger.info('event=collections_merged total=%s', len(collections))
            if strategy == 'global_only':
                logger.info('event=collection_scan_skipped strategy=global_only')
                return all_slugs, collections

        semaphore = asyncio.Semaphore(max(1, self.settings.scraper_collection_concurrency))

        async def _collection_task(collection_slug: str) -> list[str]:
            async with semaphore:
                try:
                    logger.info('event=collection_scan_start collection=%s', collection_slug)
                    result = await self.fetch_for_sale_slugs(
                        collection_slug,
                        api_url=api_url,
                        cookies=cookies,
                    )
                    logger.info('event=collection_scan_done collection=%s slugs=%s', collection_slug, len(result))
                    if result:
                        return result
                    # If collection is present in global feed but returned nothing, retry with fresh session.
                    if collection_slug in global_collections:
                        page_path = f'/gifts/{collection_slug}?sort=listed&filter=sale'
                        fresh_api, fresh_cookies, _ = await self._init_session(page_path)
                        await asyncio.sleep(0.2)
                        return await self.fetch_for_sale_slugs(collection_slug, api_url=fresh_api, cookies=fresh_cookies)
                    return result
                except Exception:
                    logger.exception('event=collection_scan_failed collection=%s retry=true', collection_slug)
                    try:
                        page_path = f'/gifts/{collection_slug}?sort=listed&filter=sale'
                        fresh_api, fresh_cookies, _ = await self._init_session(page_path)
                        await asyncio.sleep(0.3)
                        return await self.fetch_for_sale_slugs(collection_slug, api_url=fresh_api, cookies=fresh_cookies)
                    except Exception:
                        logger.exception('event=collection_scan_retry_failed collection=%s', collection_slug)
                        return []

        if strategy == 'collections_only':
            all_slugs.clear()
            seen.clear()
        collection_results = await asyncio.gather(
            *[_collection_task(collection_slug) for collection_slug in collections],
        )
        for collection_slugs in collection_results:
            for slug in collection_slugs:
                if slug not in seen:
                    seen.add(slug)
                    all_slugs.append(slug)

        logger.info(
            'event=for_sale_slug_total unique=%s collections=%s',
            len(all_slugs),
            len(collections),
        )
        return all_slugs, collections

    async def fetch_sold_slugs(self) -> list[str]:
        """Сканирует проданные подарки. Использует filter=sold."""
        api_url, cookies, _ = await self._init_session('/gifts?sort=listed&filter=sold')
        all_slugs: list[str] = []
        seen: set[str] = set()
        seen_offsets: set[str] = set()
        next_offset: str | None = None
        page_idx = 0
        stale_pages = 0
        stale_limit = 3
        limit = self.settings.scraper_sold_max_pages
        logger.info('event=sold_pagination_start')
        while True:
            if limit > 0 and page_idx >= limit:
                logger.info('event=sold_pagination_stop reason=max_pages page=%s', page_idx)
                break
            if next_offset and next_offset in seen_offsets:
                break
            if next_offset:
                seen_offsets.add(next_offset)
            page_idx += 1
            payload = {
                'method': 'searchAuctions',
                'type': 'gifts',
                'sort': 'listed',
                'filter': 'sold',
                'query': '',
                'collection': '',
            }
            if next_offset:
                payload['offset_id'] = next_offset
            try:
                headers = {
                    'X-Requested-With': 'XMLHttpRequest',
                    'Referer': f'{self.base_url}/gifts?sort=listed&filter=sold',
                    'Origin': self.base_url,
                }
                response = await self._request_with_retry('POST', api_url, data=payload, headers=headers, cookies=cookies)
                data = response.json()
                if isinstance(data, list):
                    first_dict = next((item for item in data if isinstance(item, dict)), None)
                    data = first_dict or {}
                if not data.get('ok'):
                    logger.warning('event=sold_search_bad_response page=%s', page_idx)
                    break
                next_offset = data.get('next_offset') or data.get('nextOffset') or data.get('next')
                html_chunk = data.get('html') or (data.get('body', '') + data.get('foot', ''))
                if not html_chunk:
                    break
                page_slugs, html_offset = parse_search_html(html_chunk)
                if not next_offset:
                    next_offset = html_offset
            except Exception:
                logger.exception('event=sold_search_failed page=%s', page_idx)
                break
            if not page_slugs:
                break
            before = len(all_slugs)
            for s in page_slugs:
                if s not in seen:
                    seen.add(s)
                    all_slugs.append(s)
            if len(all_slugs) == before:
                stale_pages += 1
                if stale_pages >= stale_limit:
                    break
            else:
                stale_pages = 0
            if not next_offset:
                break
            if self.settings.scraper_page_delay_seconds > 0:
                await asyncio.sleep(self.settings.scraper_page_delay_seconds)
        logger.info('event=sold_pagination_done pages=%s slugs=%s', page_idx, len(all_slugs))
        return all_slugs

    async def fetch_gift(self, slug: str) -> ParsedGift:
        t0 = time.perf_counter()
        response = await self._request_with_retry('GET', f'{self.base_url}/gift/{slug}')
        ms = int((time.perf_counter() - t0) * 1000)
        gift = parse_gift_page(slug, response.text)
        logger.debug('event=gift_fetched slug=%s ms=%s', slug, ms)
        return gift

    async def close(self) -> None:
        await self._client.aclose()
