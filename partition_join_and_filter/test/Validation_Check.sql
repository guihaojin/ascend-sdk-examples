SELECT
  j.ticket_id,
  j.order_id as test_order_id,
  vj.order_id as validation_order_id,
  CASE
    WHEN j.order_id <> vj.order_id THEN RAISE_ERROR("invalid data")
    ELSE "OK"
  END as test
FROM
  {{Join_with_3_hour_window_Spark_Sql_Partial_Reduction}} as j
  full outer join {{_Validation__Join_with_3_hour_window}} as vj on j.ticket_id = vj.ticket_id