from datetime import datetime

from pydantic import BaseModel, Field


class ParsedGiftAttribute(BaseModel):
    key: str
    value: str
    rarity_percent: float | None = None


class ParsedGift(BaseModel):
    source_slug: str
    collection_slug: str | None
    title: str
    number_label: str | None
    status: str
    price_ton: float | None = None
    price_usd: float | None = None
    owner_wallet: str | None = None
    listed_at: datetime | None = None
    expires_at: datetime | None = None
    thumbnail_url: str | None = None
    raw_source: dict | None = None
    attributes: list[ParsedGiftAttribute] = Field(default_factory=list)