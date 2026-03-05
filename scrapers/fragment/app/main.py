import asyncio
import logging
import logging.config
import traceback
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException

from .config import Settings
from .fragment.client import FragmentTransientError
from .sync_service import ScraperSyncService

settings = Settings()


class MoscowFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=ZoneInfo('Europe/Moscow'))
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime('%d.%m.%Y %H:%M:%S')

    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        message = " | ".join(part.strip() for part in str(message).splitlines() if part.strip()) or "-"
        event = getattr(record, 'event', None)
        if not event:
            if message.startswith('event='):
                first, _, rest = message.partition(' ')
                event = first.split('=', 1)[1] or 'log'
                message = rest or '-'
            else:
                event = 'log'
        record.event = event
        if record.exc_info:
            message = f'{message} exception="{self.formatException(record.exc_info)}"'
            record.exc_info = None
        if record.stack_info:
            stack = " | ".join(part.strip() for part in str(record.stack_info).splitlines() if part.strip())
            if stack:
                message = f'{message} stack="{stack}"'
            record.stack_info = None
        record.msg = message
        record.args = ()
        return super().format(record)

    def formatException(self, ei) -> str:
        lines = traceback.format_exception(*ei)
        return " | ".join(line.strip() for line in lines if line.strip())


def _configure_logging() -> None:
    level = settings.log_level.upper()
    logging.config.dictConfig(
        {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'moscow': {
                    '()': MoscowFormatter,
                    'format': '[%(asctime)s МСК] %(levelname)s %(name)s event=%(event)s %(message)s',
                }
            },
            'handlers': {
                'default': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'moscow',
                    'level': level,
                }
            },
            'root': {'handlers': ['default'], 'level': level},
            'loggers': {
                'uvicorn.error': {'handlers': ['default'], 'level': level, 'propagate': False},
                'uvicorn.access': {'handlers': ['default'], 'level': 'WARNING', 'propagate': False},
                'apscheduler': {'handlers': ['default'], 'level': 'WARNING', 'propagate': False},
                'httpx': {'handlers': ['default'], 'level': 'WARNING', 'propagate': False},
                'httpcore': {'handlers': ['default'], 'level': 'WARNING', 'propagate': False},
            },
        }
    )


_configure_logging()
logger = logging.getLogger('scraper')

@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    if settings.fragment_scraper_enabled:
        scheduler.add_job(_run_sync_job, 'cron', id='fragment-hourly', replace_existing=True, **_cron_kwargs(settings.scraper_schedule_cron))
        scheduler.start()
        logger.info('event=scraper_scheduler_started cron="%s"', settings.scraper_schedule_cron)
    yield
    with suppress(Exception):
        if scheduler.running:
            scheduler.shutdown(wait=False)
    with suppress(Exception):
        await service.close()


app = FastAPI(title='GGWP Scraper', version='0.2.0', lifespan=lifespan)
scheduler = AsyncIOScheduler(
    timezone='UTC',
    job_defaults={
        'max_instances': 8,
        'coalesce': True,
        'misfire_grace_time': 3600,
    },
)
service = ScraperSyncService(settings)
_running = False
_rerun_requested = False
_last_result: dict | None = None
_last_error: str | None = None
_last_success_at: str | None = None
_last_attempt_at: str | None = None
_degraded = False
_total_runs: int = 0
_total_errors: int = 0
_total_scraped: int = 0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _msk_str(dt: datetime) -> str:
    return dt.astimezone(ZoneInfo('Europe/Moscow')).strftime('%d.%m.%Y %H:%M:%S')


def _schedule_recovery_run() -> None:
    if not scheduler.running:
        return
    if scheduler.get_job('fragment-recovery'):
        return
    run_date = datetime.now(timezone.utc) + timedelta(minutes=settings.scraper_retry_after_failure_minutes)
    scheduler.add_job(_run_sync_job, 'date', id='fragment-recovery', replace_existing=True, run_date=run_date)
    logger.warning('event=scraper_recovery_scheduled run_at="%s МСК"', _msk_str(run_date))


def _clear_recovery_run() -> None:
    with suppress(Exception):
        if scheduler.get_job('fragment-recovery'):
            scheduler.remove_job('fragment-recovery')


async def _run_sync_job() -> None:
    global _running, _rerun_requested, _last_result, _last_error, _last_success_at, _last_attempt_at, _degraded
    global _total_runs, _total_errors, _total_scraped
    if _running:
        _rerun_requested = True
        logger.warning('event=scraper_sync_overlap queued=true')
        return
    _running = True
    try:
        while True:
            _rerun_requested = False
            _last_attempt_at = _utc_now_iso()
            try:
                _last_result = await service.run_once()
                _last_error = None
                _degraded = False
                _last_success_at = _utc_now_iso()
                _total_runs += 1
                _total_scraped += _last_result.get('received', 0)
                _clear_recovery_run()
            except Exception as exc:
                _last_error = f'{type(exc).__name__}: {exc}'
                _degraded = isinstance(exc, FragmentTransientError)
                _total_errors += 1
                if _degraded:
                    _schedule_recovery_run()
                logger.exception('event=scraper_sync_failed degraded=%s', _degraded)
            if not _rerun_requested:
                break
            logger.info('event=scraper_sync_run_queued_now')
    finally:
        _running = False


def _get_next_run_at() -> str | None:
    try:
        job = scheduler.get_job('fragment-hourly')
        if job and job.next_run_time:
            from zoneinfo import ZoneInfo
            return job.next_run_time.astimezone(ZoneInfo('Europe/Moscow')).strftime('%d.%m.%Y %H:%M:%S МСК')
    except Exception:
        pass
    return None


def _cron_kwargs(expr: str) -> dict:
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError('SCRAPER_SCHEDULE_CRON must have 5 parts')
    minute, hour, day, month, day_of_week = parts
    return {
        'minute': minute,
        'hour': hour,
        'day': day,
        'month': month,
        'day_of_week': day_of_week,
    }


@app.get('/healthz')
def healthz() -> dict[str, str | bool | int | dict | None]:
    status = 'degraded' if _degraded else 'ok'
    return {
        'status': status,
        'service': 'scraper',
        'enabled': settings.fragment_scraper_enabled,
        'running': _running,
        'last_result': _last_result,
        'last_error': _last_error,
        'last_success_at': _last_success_at,
        'last_attempt_at': _last_attempt_at,
        'total_runs': _total_runs,
        'total_errors': _total_errors,
        'total_scraped': _total_scraped,
        'next_run_at': _get_next_run_at(),
        'fragment_api_url_override_set': bool(settings.fragment_api_url_override),
    }


@app.get('/stats')
def stats() -> dict:
    return {
        'total_runs': _total_runs,
        'total_errors': _total_errors,
        'total_scraped': _total_scraped,
        'last_result': _last_result,
        'last_error': _last_error,
        'last_success_at': _last_success_at,
        'last_attempt_at': _last_attempt_at,
        'next_run_at': _get_next_run_at(),
        'running': _running,
        'degraded': _degraded,
    }


@app.post('/run-now')
async def run_now() -> dict:
    if not settings.fragment_scraper_enabled:
        raise HTTPException(status_code=409, detail='Scraper is disabled')
    if _running:
        raise HTTPException(status_code=409, detail='Sync is already running')
    await _run_sync_job()
    if _last_error:
        raise HTTPException(status_code=500, detail=f'Sync failed: {_last_error}')
    return _last_result or {'ok': True}