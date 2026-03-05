import asyncio
import time
import logging
from collections.abc import Iterable

import httpx

from .config import Settings
from .fragment import FragmentClient
from .schemas import ParsedGift

logger = logging.getLogger('scraper.sync')


class ScraperSyncService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.fragment = FragmentClient(settings)
        self._client = httpx.AsyncClient(timeout=60)

    async def close(self) -> None:
        await self.fragment.close()
        await self._client.aclose()

    async def _fetch_details(self, slugs: Iterable[str]) -> tuple[list[ParsedGift], int]:
        semaphore = asyncio.Semaphore(self.settings.scraper_detail_concurrency)
        errors = [0]

        async def _task(slug: str) -> ParsedGift | None:
            async with semaphore:
                t0 = time.perf_counter()
                try:
                    gift = await self.fragment.fetch_gift(slug)
                    ms = int((time.perf_counter() - t0) * 1000)
                    if ms > 5000:
                        logger.warning('event=gift_fetch_slow slug=%s ms=%s', slug, ms)
                    if self.settings.scraper_detail_delay_seconds > 0:
                        await asyncio.sleep(self.settings.scraper_detail_delay_seconds)
                    return gift
                except Exception:
                    ms = int((time.perf_counter() - t0) * 1000)
                    logger.exception('event=gift_detail_fetch_failed slug=%s ms=%s', slug, ms)
                    errors[0] += 1
                    return None

        slugs_list = list(slugs)
        batch_size = max(1, int(self.settings.scraper_detail_batch_size))
        result: list[ParsedGift] = []
        for index in range(0, len(slugs_list), batch_size):
            batch = slugs_list[index : index + batch_size]
            logger.info('event=detail_batch_start start=%s end=%s total=%s', index, index + len(batch), len(slugs_list))
            batch_result = await asyncio.gather(*[_task(slug) for slug in batch])
            ok_items = [item for item in batch_result if item is not None]
            result.extend(ok_items)
            logger.info('event=detail_batch_done start=%s end=%s ok=%s', index, index + len(batch), len(ok_items))
        return result, errors[0]

    async def _push_batch(self, gifts: list[ParsedGift]) -> dict:
        endpoint = f"{self.settings.app_base_url.rstrip('/')}/api/v1/internal/catalog/upsert-batch"
        payload = {'gifts': [g.model_dump(mode='json') for g in gifts]}
        headers = {'X-Internal-Token': self.settings.internal_api_token}
        response = await self._client.post(endpoint, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()

    async def run_once(self) -> dict:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.settings.scraper_run_timeout_minutes * 60
        logger.info(
            'event=scraper_run_start detail_conc=%s detail_batch=%s upsert_batch=%s timeout_min=%s',
            self.settings.scraper_detail_concurrency,
            self.settings.scraper_detail_batch_size,
            self.settings.scraper_upsert_batch_size,
            self.settings.scraper_run_timeout_minutes,
        )
        start = time.perf_counter()
        slugs, collections = await self.fragment.fetch_all_for_sale_slugs()
        list_ms = int((time.perf_counter() - start) * 1000)

        sold_slugs: list[str] = []
        if self.settings.scraper_scan_sold:
            logger.info('event=sold_scan_start')
            try:
                sold_slugs = await self.fragment.fetch_sold_slugs()
                logger.info('event=sold_scan_done count=%s', len(sold_slugs))
            except Exception:
                logger.exception('event=sold_scan_failed')

        all_slugs_combined = list(dict.fromkeys(slugs + sold_slugs))

        if self.settings.scraper_max_gifts > 0 and len(all_slugs_combined) > self.settings.scraper_max_gifts:
            all_slugs_combined = all_slugs_combined[: self.settings.scraper_max_gifts]
        logger.info('event=slug_collection_finished slugs=%s collections=%s list_ms=%s', len(all_slugs_combined), len(collections), list_ms)
        if loop.time() > deadline:
            raise TimeoutError('Scraper run timeout reached before details')
        start = time.perf_counter()
        gifts, fetch_errors = await self._fetch_details(all_slugs_combined)
        detail_ms = int((time.perf_counter() - start) * 1000)

        if loop.time() > deadline:
            logger.warning('event=deadline_reached_before_upsert gifts=%s', len(gifts))
        logger.info('event=gift_detail_fetch_finished gifts=%s detail_ms=%s', len(gifts), detail_ms)

        if not gifts:
            return {'ok': True, 'received': 0, 'created': 0, 'updated': 0, 'list_ms': list_ms, 'detail_ms': detail_ms, 'fetch_errors': fetch_errors}
        aggregate = {
            'ok': True,
            'received': 0,
            'created': 0,
            'updated': 0,
            'price_changed': 0,
            'status_changed': 0,
            'attrs_changed': 0,
            'list_ms': list_ms,
            'detail_ms': detail_ms,
            'upsert_ms': 0,
            'deadline_reached': False,
            'fetch_errors': fetch_errors,
        }
        counter_keys = {'received', 'created', 'updated', 'price_changed', 'status_changed', 'attrs_changed'}
        upsert_start = time.perf_counter()
        batch_size = max(1, int(self.settings.scraper_upsert_batch_size))
        for index in range(0, len(gifts), batch_size):
            if loop.time() > deadline:
                aggregate['deadline_reached'] = True
                logger.warning('event=deadline_reached_during_upsert index=%s total=%s', index, len(gifts))
                break
            chunk = gifts[index : index + batch_size]
            result = await self._push_batch(chunk)
            for key in counter_keys:
                aggregate[key] += int(result.get(key, 0))
            logger.info('event=catalog_upsert_chunk start=%s end=%s result=%s', index, index + len(chunk), result)
        aggregate['upsert_ms'] = int((time.perf_counter() - upsert_start) * 1000)
        logger.info('event=scraper_run_finished result=%s', aggregate)
        return aggregate