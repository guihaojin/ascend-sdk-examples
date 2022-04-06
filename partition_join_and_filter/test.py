import base64
import os
import pathlib

from ascend.sdk import definitions
from ascend.sdk.applier import DataflowApplier
from ascend.sdk.client import Client

import ascend.protos.ascend.ascend_pb2 as ascend
import ascend.protos.component.component_pb2 as component
import ascend.protos.connection.connection_pb2 as connection
import ascend.protos.content_encoding.content_encoding_pb2 as content_encoding
import ascend.protos.core.core_pb2 as core
import ascend.protos.environment.environment_pb2 as environment
import ascend.protos.expression.expression_pb2 as expression
import ascend.protos.format.format_pb2 as format
import ascend.protos.function.function_pb2 as function
import ascend.protos.io.io_pb2 as io
import ascend.protos.operator.operator_pb2 as operator
import ascend.protos.pattern.pattern_pb2 as pattern
import ascend.protos.schema.schema_pb2 as schema
import ascend.protos.text.text_pb2 as text

from google.protobuf.wrappers_pb2 import DoubleValue
from google.protobuf.wrappers_pb2 import BoolValue
from google.protobuf.wrappers_pb2 import Int64Value
from google.protobuf.wrappers_pb2 import UInt64Value
from google.protobuf.wrappers_pb2 import Int32Value
from google.protobuf.wrappers_pb2 import UInt32Value
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.struct_pb2 import NullValue
from google.protobuf.empty_pb2 import Empty

# dataflow 'test'
## data feed connectors for dataflow 'test'
data_feed_connectors_test = []


## components for dataflow 'test'
components_test = []

component_Tickets = definitions.ReadConnector(
  id='Tickets',
  name='Tickets',
  description='',
  pattern=pattern.Pattern(
    glob='**',
  ),
  container=io.Container(
    record_connection=io.Connection.Asset.Record(
      connection_id=connection.Id(
        value='81b7953e-953c-4fca-8d9f-aba0c8845fd9',
      ),
      details={
        'object_pattern': ascend.Value(
          string_value='kitchen_sink/dpj-test-data/tickets/**',
        ),
        'object_pattern_type': ascend.Value(
          union_value=ascend.Union(
            tag='glob',
            value=ascend.Value(
              struct_value=ascend.Struct(
              ),
            ),
          ),
        ),
        'parser_type': ascend.Value(
          union_value=ascend.Union(
            tag='json',
            value=ascend.Value(
              struct_value=ascend.Struct(
              ),
            ),
          ),
        ),
      },
    ),
  ),
  update_periodical=core.Periodical(
    period=Duration(
      seconds=134217727,
    ),
    offset=Duration(
      seconds=37470117,
    ),
  ),
  last_manual_refresh_time=None,
  assigned_priority=component.Priority(
  ),
  aggregation_limit=None,
  bytes=None,
  records=component.Source.FromRecords(
    schema=schema.Map(
      field=[
        schema.Field(
          name='order_id',
          schema=schema.Schema(
            long=schema.Long(
            ),
          ),
        ),
        schema.Field(
          name='ticket_id',
          schema=schema.Schema(
            long=schema.Long(
            ),
          ),
        ),
        schema.Field(
          name='ts',
          schema=schema.Schema(
            timestamp=schema.Timestamp(
            ),
          ),
        ),
      ],
    ),
  ),
  compute_configurations=None,
)
components_test.append(component_Tickets)

component_Tickets_by_hour = definitions.Transform(
  id='Tickets_by_hour',
  name='Tickets_by_hour',
  description='',
  input_ids=[
    'Tickets',
  ],
  operator=operator.Operator(
    spark_function=operator.Spark.Function(
      executable=io.Executable(
        code=io.Code(
          language=function.Code.Language(
            sql=function.Code.Language.Sql(
            ),
          ),
          source=io.Code.Source(
            inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "test", "Tickets_by_hour.sql")).read_text(encoding="utf-8"),
          ),
        ),
      ),
      reduction=operator.Reduction(
        partial=operator.Reduction.Partial(
          partition_by={
            0: operator.Reduction.Partial.PartitionSpecification(
              column='ts',
              granularity=operator.Granularity(
                time=operator.Granularity.Time(
                  hour=operator.Granularity.Time.Hour(
                  ),
                ),
              ),
            ),
          },
        ),
      ),
    ),
  ),
  assigned_priority=component.Priority(
  ),
)
components_test.append(component_Tickets_by_hour)

component_Orders = definitions.ReadConnector(
  id='Orders',
  name='Orders',
  description='',
  pattern=pattern.Pattern(
    glob='**',
  ),
  container=io.Container(
    record_connection=io.Connection.Asset.Record(
      connection_id=connection.Id(
        value='81b7953e-953c-4fca-8d9f-aba0c8845fd9',
      ),
      details={
        'object_pattern': ascend.Value(
          string_value='kitchen_sink/dpj-test-data/orders/**',
        ),
        'object_pattern_type': ascend.Value(
          union_value=ascend.Union(
            tag='glob',
            value=ascend.Value(
              struct_value=ascend.Struct(
              ),
            ),
          ),
        ),
        'parser_type': ascend.Value(
          union_value=ascend.Union(
            tag='json',
            value=ascend.Value(
              struct_value=ascend.Struct(
              ),
            ),
          ),
        ),
      },
    ),
  ),
  update_periodical=core.Periodical(
    period=Duration(
      seconds=134217727,
    ),
    offset=Duration(
      seconds=37528540,
    ),
  ),
  last_manual_refresh_time=None,
  assigned_priority=component.Priority(
  ),
  aggregation_limit=None,
  bytes=None,
  records=component.Source.FromRecords(
    schema=schema.Map(
      field=[
        schema.Field(
          name='order_id',
          schema=schema.Schema(
            long=schema.Long(
            ),
          ),
        ),
        schema.Field(
          name='order_name',
          schema=schema.Schema(
            string=schema.String(
            ),
          ),
        ),
        schema.Field(
          name='ts',
          schema=schema.Schema(
            timestamp=schema.Timestamp(
            ),
          ),
        ),
      ],
    ),
  ),
  compute_configurations=None,
)
components_test.append(component_Orders)

component_Orders_by_hour = definitions.Transform(
  id='Orders_by_hour',
  name='Orders_by_hour',
  description='',
  input_ids=[
    'Orders',
  ],
  operator=operator.Operator(
    spark_function=operator.Spark.Function(
      executable=io.Executable(
        code=io.Code(
          language=function.Code.Language(
            sql=function.Code.Language.Sql(
            ),
          ),
          source=io.Code.Source(
            inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "test", "Orders_by_hour.sql")).read_text(encoding="utf-8"),
          ),
        ),
      ),
      reduction=operator.Reduction(
        partial=operator.Reduction.Partial(
          partition_by={
            0: operator.Reduction.Partial.PartitionSpecification(
              column='ts',
              granularity=operator.Granularity(
                time=operator.Granularity.Time(
                  hour=operator.Granularity.Time.Hour(
                  ),
                ),
              ),
            ),
          },
        ),
      ),
    ),
  ),
  assigned_priority=component.Priority(
  ),
)
components_test.append(component_Orders_by_hour)

component_Join_with_3_hour_window_PySpark = definitions.Transform(
  id='Join_with_3_hour_window_PySpark',
  name='Join_with_3_hour_window_PySpark',
  description='',
  input_ids=[
    'Tickets_by_hour',
    'Orders_by_hour',
  ],
  operator=operator.Operator(
    spark_function=operator.Spark.Function(
      executable=io.Executable(
        code=io.Code(
          language=function.Code.Language(
            python=function.Code.Language.Python(
              v3=function.Code.Language.Python.Version.V3(
              ),
            ),
          ),
          source=io.Code.Source(
            inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "test", "Join_with_3_hour_window_PySpark.py")).read_text(encoding="utf-8"),
          ),
        ),
      ),
      reduction=operator.Reduction(
        no_reduction=operator.Reduction.NoReduction(
        ),
      ),
      additional_input_configs={
        0: operator.Spark.Function.AdditionalInputConfig(
        ),
        1: operator.Spark.Function.AdditionalInputConfig(
          filter=operator.Spark.Filter(
            composite=operator.Spark.Filter.Composite(
              operator=1,
              filters=[
                operator.Spark.Filter(
                  function=operator.Spark.Filter.Function(
                    name='partition_join',
                    args=[
                      'ts_hour',
                      '>=',
                      'ts_hour',
                      "'-1'",
                      "'hour'",
                    ],
                  ),
                ),
                operator.Spark.Filter(
                  function=operator.Spark.Filter.Function(
                    name='partition_join',
                    args=[
                      'ts_hour',
                      '<=',
                      'ts_hour',
                      "'+1'",
                      "'hour'",
                    ],
                  ),
                ),
              ],
            ),
          ),
        ),
      },
    ),
  ),
  assigned_priority=component.Priority(
  ),
)
components_test.append(component_Join_with_3_hour_window_PySpark)

component_Join_with_SparkSql_and_partition_filter = definitions.Transform(
  id='Join_with_SparkSql_and_partition_filter',
  name='Join with SparkSql and partition filter',
  description='',
  input_ids=[
    'Tickets_by_hour',
    'Orders_by_hour',
  ],
  operator=operator.Operator(
    spark_function=operator.Spark.Function(
      executable=io.Executable(
        code=io.Code(
          language=function.Code.Language(
            sql=function.Code.Language.Sql(
            ),
          ),
          source=io.Code.Source(
            inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "test", "Join_with_SparkSql_and_partition_filter.sql")).read_text(encoding="utf-8"),
          ),
        ),
      ),
      reduction=operator.Reduction(
        no_reduction=operator.Reduction.NoReduction(
        ),
      ),
      additional_input_configs={
        0: operator.Spark.Function.AdditionalInputConfig(
          filter=operator.Spark.Filter(
            composite=operator.Spark.Filter.Composite(
              operator=1,
              filters=[
                operator.Spark.Filter(
                  function=operator.Spark.Filter.Function(
                    name='partition_filter',
                    args=[
                      'order_id',
                      '>',
                      "'200'",
                    ],
                  ),
                ),
              ],
            ),
          ),
        ),
        1: operator.Spark.Function.AdditionalInputConfig(
          filter=operator.Spark.Filter(
            composite=operator.Spark.Filter.Composite(
              operator=1,
              filters=[
                operator.Spark.Filter(
                  function=operator.Spark.Filter.Function(
                    name='partition_join',
                    args=[
                      'order_id',
                      '=',
                      'order_id',
                    ],
                  ),
                ),
              ],
            ),
          ),
        ),
      },
    ),
  ),
  assigned_priority=component.Priority(
  ),
)
components_test.append(component_Join_with_SparkSql_and_partition_filter)

component_Join_with_3_hour_window_Spark_Sql_Partial_Reduction = definitions.Transform(
  id='Join_with_3_hour_window_Spark_Sql_Partial_Reduction',
  name='Join with 3 hour window Spark Sql Partial Reduction',
  description='',
  input_ids=[
    'Tickets_by_hour',
    'Orders_by_hour',
  ],
  operator=operator.Operator(
    spark_function=operator.Spark.Function(
      executable=io.Executable(
        code=io.Code(
          language=function.Code.Language(
            sql=function.Code.Language.Sql(
            ),
          ),
          source=io.Code.Source(
            inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "test", "Join_with_3_hour_window_Spark_Sql_Partial_Reduction.sql")).read_text(encoding="utf-8"),
          ),
        ),
      ),
      reduction=operator.Reduction(
        partial=operator.Reduction.Partial(
          partition_by={
            0: operator.Reduction.Partial.PartitionSpecification(
              column='ts_hour',
              granularity=operator.Granularity(
                time=operator.Granularity.Time(
                  hour=operator.Granularity.Time.Hour(
                  ),
                ),
              ),
            ),
          },
        ),
      ),
      additional_input_configs={
        0: operator.Spark.Function.AdditionalInputConfig(
        ),
        1: operator.Spark.Function.AdditionalInputConfig(
          filter=operator.Spark.Filter(
            composite=operator.Spark.Filter.Composite(
              operator=1,
              filters=[
                operator.Spark.Filter(
                  function=operator.Spark.Filter.Function(
                    name='partition_join',
                    args=[
                      'ts_hour',
                      '>=',
                      'ts_hour',
                      "'-1'",
                      "'hour'",
                    ],
                  ),
                ),
                operator.Spark.Filter(
                  function=operator.Spark.Filter.Function(
                    name='partition_join',
                    args=[
                      'ts_hour',
                      '<=',
                      'ts_hour',
                      "'+1'",
                      "'hour'",
                    ],
                  ),
                ),
              ],
            ),
          ),
        ),
      },
    ),
  ),
  assigned_priority=component.Priority(
  ),
)
components_test.append(component_Join_with_3_hour_window_Spark_Sql_Partial_Reduction)

component__Validation__Join_with_3_hour_window = definitions.Transform(
  id='_Validation__Join_with_3_hour_window',
  name='_Validation__Join_with_3_hour_window',
  description='',
  input_ids=[
    'Tickets_by_hour',
    'Orders_by_hour',
  ],
  operator=operator.Operator(
    spark_function=operator.Spark.Function(
      executable=io.Executable(
        code=io.Code(
          language=function.Code.Language(
            sql=function.Code.Language.Sql(
            ),
          ),
          source=io.Code.Source(
            inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "test", "_Validation__Join_with_3_hour_window.sql")).read_text(encoding="utf-8"),
          ),
        ),
      ),
      reduction=operator.Reduction(
        full=operator.Reduction.Full(
        ),
      ),
    ),
  ),
  assigned_priority=component.Priority(
  ),
)
components_test.append(component__Validation__Join_with_3_hour_window)

component_Validation_Check = definitions.Transform(
  id='Validation_Check',
  name='Validation_Check',
  description='',
  input_ids=[
    'Join_with_3_hour_window_Spark_Sql_Partial_Reduction',
    '_Validation__Join_with_3_hour_window',
  ],
  operator=operator.Operator(
    spark_function=operator.Spark.Function(
      executable=io.Executable(
        code=io.Code(
          language=function.Code.Language(
            sql=function.Code.Language.Sql(
            ),
          ),
          source=io.Code.Source(
            inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "test", "Validation_Check.sql")).read_text(encoding="utf-8"),
          ),
        ),
      ),
      reduction=operator.Reduction(
        full=operator.Reduction.Full(
        ),
      ),
    ),
  ),
  assigned_priority=component.Priority(
  ),
)
components_test.append(component_Validation_Check)


## data feeds for dataflow 'test'
data_feeds_test = []


## component groups for dataflow 'test'
groups_test = []


dataflow_test = definitions.Dataflow(
  id='test',
  name='test',
  description='DF for legacy migration.',
  components=components_test,
  data_feeds=data_feeds_test,
  data_feed_connectors=data_feed_connectors_test,
  groups=groups_test,
)

def apply_dataflow(client: Client, data_service_id: str, dataflow: definitions.Dataflow):
  DataflowApplier(client).apply(data_service_id, dataflow)

if __name__ == "__main__":
  client = Client("dev-haojin.ascend.io")
  apply_dataflow(client, "_ascend", dataflow_test)