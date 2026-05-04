select
    name,
    symbol,
    price,
    percent_change_24h,
    rank
from {{ ref('stg_crypto_tickers') }}
where rank <= 100
order by percent_change_24h desc
limit 20

