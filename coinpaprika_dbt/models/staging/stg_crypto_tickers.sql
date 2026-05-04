select
    id,
    name,
    symbol,
    rank,
    price,
    market_cap,
    volume_24h,
    percent_change_24h,
    percent_change_7d,
    total_supply,
    max_supply,
    last_updated::timestamptz as last_updated
from public.crypto_tickers

