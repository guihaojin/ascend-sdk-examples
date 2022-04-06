select
  ticket_id,
  t.order_id,
  order_name,
  t.ts as ticket_ts,
  o.ts as order_ts,
  t.ts_hour as ticket_ts_hour,
  o.ts_hour as order_ts_hour
FROM
  {{Tickets_by_hour}} as t
  left join {{Orders_by_hour}} as o on t.order_id = o.order_id
where
  o.ts_hour >= t.ts_hour - INTERVAL 1 hour
  and o.ts_hour <= t.ts_hour + INTERVAL 1 hour
order by 
  ticket_id