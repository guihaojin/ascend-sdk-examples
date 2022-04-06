from pyspark.sql import DataFrame, SparkSession
from typing import List


def transform(spark_session: SparkSession, inputs: List[DataFrame], credentials=None) -> DataFrame:
  """Transforms input DataFrame(s) and returns a single DataFrame as a result

    # Arguments
    spark_session -- Entrypoint into PySpark's Dataset and DataFrame API
    inputs        -- A nonempty List of the input components for this Transform. The index of each
                     input in the list is determined by how the Transform is configured.
    credentials   -- If set (in Advanced Settings), this variable takes upon the string value of the
                     content of the 'Credentials Secret' field.

    # Returns
    Any object of type DataFrame
    """
  Tickets_by_hour = inputs[0]
  Orders_by_hour = inputs[1]

  Tickets_by_hour.createTempView("Tickets_by_hour")
  Orders_by_hour.createTempView("Orders_by_hour")

  return spark_session.sql("""select
  ticket_id,
  t.order_id,
  order_name,
  t.ts as ticket_ts,
  o.ts as order_ts,
  t.ts_hour as ticket_ts_hour,
  o.ts_hour as order_ts_hour
FROM
  Tickets_by_hour as t
  left join Orders_by_hour as o on t.order_id = o.order_id
where
  o.ts_hour >= t.ts_hour - INTERVAL 1 hour
  and o.ts_hour <= t.ts_hour + INTERVAL 1 hour""")
