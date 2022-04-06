select
  DATE_TRUNC('hour', ts) as ts_hour,
  ts,
  order_id,
  ticket_id
from
  {{Tickets}} as t
group by
  ts_hour,
  ts,
  order_id,
  ticket_id