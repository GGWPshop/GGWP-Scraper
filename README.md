# GGWP Scraper

Standalone scraper microservice for the GGWP ecosystem.

## Scrapers

| Scraper | Source | Target | Status |
|---------|--------|--------|--------|
| Fragment | fragment.com/gifts | GGWP Telegram Gifts | ✅ Production |
| Xbox | xbox.com/store | GGWP Store | 🔧 Planned |
| Steam | store.steampowered.com | GGWP Store | 🔧 Planned |
| PlayStation | store.playstation.com | GGWP Store | 🔧 Planned |
| Nintendo | nintendo.com/store | GGWP Store | 🔧 Planned |

## Architecture

```
Scraper → Source Store → Parse → Push to Target API
```

Each scraper is a standalone FastAPI service that:
1. Scrapes catalog data from a source on a schedule (cron)
2. Pushes parsed data to a target project API via `APP_BASE_URL`
3. Exposes `/healthz`, `/stats`, `/run-now` endpoints

## Fragment Scraper

Scrapes Telegram gifts from fragment.com and pushes to GGWP Telegram Gifts API.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_BASE_URL` | `http://api:8000` | Target API URL |
| `INTERNAL_API_TOKEN` | required | Internal API auth token |
| `FRAGMENT_SCRAPER_ENABLED` | `true` | Enable/disable scraper |
| `SCRAPER_SCHEDULE_CRON` | `0 * * * *` | Cron schedule (every hour) |
| `FRAGMENT_API_URL_OVERRIDE` | see config | Fragment API hash URL |

### Fragment API Hash

Fragment.com changes its API URL hash when deploying new JS. Update `FRAGMENT_API_URL_OVERRIDE` when scraper returns `received=0`.

Check current hash: inspect `fragment.com/gifts` page source for `"apiUrl":"/api?hash=XXXXXXXXXXXXXXXX"`.

## Running

```bash
cp .env.example .env
# Edit .env with your values
docker compose -f infra/docker/docker-compose.prod.yml up -d
```