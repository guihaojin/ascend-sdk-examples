select
  DATE_TRUNC('hour', ts) as ts_hour,
  order_id,
  order_name,
  ts
from
  {{Orders}}
group by
  ts_hour,
  ts,
  order_id,
  order_name