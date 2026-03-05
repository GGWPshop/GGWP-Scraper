from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    fragment_scraper_enabled: bool = True
    scraper_schedule_cron: str = '0 * * * *'

    fragment_base_url: str = 'https://fragment.com'
    # Override skips HTML-based apiUrl hash extraction.
    # Fragment returns different HTML to server IPs (no JS apiUrl pattern).
    # Update this hash when Fragment deploys new JS (check fragment.com/gifts page source).
    # Last verified: 2026-03-05 — hash b1a0a4e6289edaf4f4
    fragment_api_url_override: str | None = '/api?hash=b1a0a4e6289edaf4f4'

    scraper_max_pages: int = 0
    scraper_max_gifts: int = 0
    scraper_detail_concurrency: int = 12
    scraper_collection_concurrency: int = 4
    scraper_http_timeout: int = 40
    scraper_http_retries: int = 5
    scraper_http_backoff_base_seconds: float = 1.0
    scraper_http_backoff_max_seconds: float = 15.0
    scraper_retry_after_failure_minutes: int = 10
    scraper_page_delay_seconds: float = 0.15
    scraper_detail_delay_seconds: float = 0.05
    scraper_detail_batch_size: int = 250
    scraper_upsert_batch_size: int = 100
    scraper_run_timeout_minutes: int = 120
    scraper_stale_pages_limit: int = 8
    scraper_log_every_n_pages: int = 5
    scraper_collection_strategy: str = 'all'  # all | collections_only | global_only

    scraper_scan_sold: bool = False
    scraper_sold_max_pages: int = 20

    # Target API URL — points to the project that consumes scraped data
    # For GGWP Telegram Gifts: http://ggwp-gifts-api:8000
    app_base_url: str = 'http://api:8000'
    internal_api_token: str = 'dev-internal-token'

    log_level: str = 'INFO'