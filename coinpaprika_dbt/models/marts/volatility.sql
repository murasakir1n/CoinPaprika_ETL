select
    name,
    symbol,
    rank,
    percent_change_24h,
    percent_change_7d,
    case
        when abs(percent_change_24h) > 10 then 'высокая'
        when abs(percent_change_24h) > 5  then 'средняя'
        else 'низкая'
    end as volatility_level
from {{ ref('stg_crypto_tickers') }}
where rank <= 100
order by abs(percent_change_24h) desc

