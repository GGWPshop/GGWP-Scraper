import re
from datetime import datetime
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from ..schemas import ParsedGift, ParsedGiftAttribute


_TON_RE = re.compile(r'([0-9]+(?:\.[0-9]+)?)')
_USD_RE = re.compile(r'\$\s*([0-9]+(?:\.[0-9]+)?)')
_RARITY_RE = re.compile(r'([0-9]+(?:\.[0-9]+)?)%')
_WS_RE = re.compile(r'\s+')
_TRAILING_NUM_RE = re.compile(r'-\d+$')

_ATTR_KEY_ALIASES = {
    'model': 'Model',
    'backdrop': 'Backdrop',
    'background': 'Backdrop',
    'symbol': 'Symbol',
    'pattern': 'Symbol',
    'effect': 'Effect',
    'wear': 'Wear',
    'collection': 'Collection',
}


def _parse_float(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _parse_time(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace('Z', '+00:00'))
    except ValueError:
        return None


def _normalize_status(raw: str | None) -> str:
    text = (raw or '').strip().lower()
    if text == 'for sale':
        return 'for_sale'
    if text == 'sold':
        return 'sold'
    if 'auction' in text:
        return 'auction'
    # Default to for_sale: scraper only fetches filter=sale listings,
    # so an unrecognised status means the page markup changed but item is still for sale.
    return 'for_sale'


def _normalize_attr_key(raw: str) -> str | None:
    key = _WS_RE.sub(' ', raw.strip())
    return _ATTR_KEY_ALIASES.get(key.lower())


def _is_reasonable_attr_value(value: str) -> bool:
    if not value:
        return False
    lowered = value.lower()
    if 't.me' in lowered or value.startswith('@'):
        return False
    if len(value) > 80:
        return False
    if not any(ch.isalpha() for ch in value):
        return False
    return True


def parse_search_html(html: str) -> tuple[list[str], str | None]:
    soup = BeautifulSoup(html, 'html.parser')
    slugs: list[str] = []
    for card in soup.select('a[href*="/gift/"]'):
        href = card.get('href') or ''
        parsed = urlparse(href)
        path = parsed.path.strip('/')
        if not path.startswith('gift/'):
            continue
        slug = path.split('/', 1)[1].strip()
        if slug and slug not in slugs:
            slugs.append(slug)

    # Fallback: capture gift slugs from raw HTML (covers non-anchor templates).
    regex_slugs = re.findall(r'/gift/([a-zA-Z0-9_-]+)', html)
    for slug in regex_slugs:
        if slug and slug not in slugs:
            slugs.append(slug)

    next_offset = None
    more_btn = soup.select_one('[data-next-offset]')
    if more_btn:
        next_offset = more_btn.get('data-next-offset')
    if not next_offset:
        raw_match = re.search(r'data-next-offset="([^"]+)"', html)
        if raw_match:
            next_offset = raw_match.group(1)

    return slugs, next_offset


def parse_gift_page(source_slug: str, html: str) -> ParsedGift:
    soup = BeautifulSoup(html, 'html.parser')
    title_text = ''
    title_node = soup.select_one('.tm-section-header h1')
    if title_node:
        title_text = title_node.get_text(' ', strip=True)
    if not title_text:
        og_title = soup.select_one('meta[property="og:title"]')
        title_text = (og_title.get('content') if og_title else '') or source_slug

    number_label = None
    num_match = re.search(r'#\d+', title_text)
    if num_match:
        number_label = num_match.group(0)
    if not number_label:
        # Fallback: extract number from slug tail (e.g. "golden-star-5" -> "#5")
        slug_num = _TRAILING_NUM_RE.search(source_slug)
        if slug_num:
            number_label = f'#{slug_num.group(0).lstrip("-")}'

    collection_slug = _TRAILING_NUM_RE.sub('', source_slug) if _TRAILING_NUM_RE.search(source_slug) else source_slug

    status_node = soup.select_one('.tm-section-header-status')
    status = _normalize_status(status_node.get_text(strip=True) if status_node else None)

    ton_value = None
    ton_selectors = [
        '.tm-section-bid-info .tm-value',
        '.tm-grid-item-value',
        '.tm-value',
        '[class*="price"] .tm-value',
    ]
    for sel in ton_selectors:
        ton_node = soup.select_one(sel)
        if ton_node:
            match = _TON_RE.search(ton_node.get_text(' ', strip=True))
            if match:
                ton_value = _parse_float(match.group(1))
                break

    usd_value = None
    usd_selectors = [
        '.tm-section-bid-info .table-cell-desc',
        '.table-cell-desc',
        '[class*="price"] .table-cell-desc',
    ]
    for sel in usd_selectors:
        usd_node = soup.select_one(sel)
        if usd_node:
            match = _USD_RE.search(usd_node.get_text(' ', strip=True))
            if match:
                usd_value = _parse_float(match.group(1))
                break

    owner_wallet = None
    owner_row = None
    for row in soup.select('.tm-table tr'):
        cell = row.select_one('.table-cell')
        if cell and cell.get_text(strip=True).lower() == 'owner':
            owner_row = row
            break
    if owner_row:
        wallet_node = owner_row.select_one('a.tm-wallet')
        if wallet_node:
            owner_wallet = wallet_node.get_text(' ', strip=True)

    listed_at = None
    for row in soup.select('.tm-table tr'):
        cell = row.select_one('.table-cell')
        if cell and 'listed' in cell.get_text(strip=True).lower():
            t = row.select_one('time[datetime]')
            if t:
                listed_at = _parse_time(t.get('datetime'))
            if not listed_at:
                val = row.select_one('.table-cell-value')
                if val:
                    ts_attr = val.get('data-timestamp') or val.get('data-time')
                    if ts_attr:
                        try:
                            from datetime import timezone
                            listed_at = datetime.fromtimestamp(int(ts_attr), tz=timezone.utc)
                        except (ValueError, OSError):
                            pass
            break
    expires_at = None
    timer = soup.select_one('.tm-countdown-timer')
    if timer:
        expires_at = _parse_time(timer.get('datetime'))

    thumbnail_url = None
    og_image = soup.select_one('meta[property="og:image"]')
    if og_image:
        thumbnail_url = og_image.get('content') or None

    attributes: list[ParsedGiftAttribute] = []
    for row in soup.select('.tm-table tr'):
        key_node = row.select_one('td .table-cell')
        if not key_node:
            continue
        key = _normalize_attr_key(key_node.get_text(' ', strip=True))
        if not key:
            continue

        value_node = row.select_one('.table-cell-value-link') or row.select_one('.table-cell-value')
        if not value_node:
            continue
        value = _WS_RE.sub(' ', value_node.get_text(' ', strip=True))
        if not _is_reasonable_attr_value(value):
            continue

        rarity = None
        rarity_node = row.select_one('.tm-rarity')
        if rarity_node:
            r_match = _RARITY_RE.search(rarity_node.get_text(' ', strip=True))
            if r_match:
                rarity = _parse_float(r_match.group(1))

        attributes.append(ParsedGiftAttribute(key=key, value=value, rarity_percent=rarity))

    return ParsedGift(
        source_slug=source_slug,
        collection_slug=collection_slug,
        title=title_text,
        number_label=number_label,
        status=status,
        price_ton=ton_value,
        price_usd=usd_value,
        owner_wallet=owner_wallet,
        listed_at=listed_at,
        expires_at=expires_at,
        thumbnail_url=thumbnail_url,
        raw_source={'source': 'fragment', 'url': f'/gift/{source_slug}'},
        attributes=attributes,
    )