select
    rank,
    name,
    symbol,
    price,
    market_cap,
    volume_24h,
    percent_change_24h,
    percent_change_7d
from {{ ref('stg_crypto_tickers') }}
where rank <= 10
order by rank