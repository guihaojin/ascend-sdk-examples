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

# dataflow 'weather'
## data feed connectors for dataflow 'weather'
data_feed_connectors_weather = []


## components for dataflow 'weather'
components_weather = []

component_NYC_Weather = definitions.ReadConnector(
  id='NYC_Weather',
  name='NYC Weather',
  description='',
  pattern=pattern.Pattern(
    glob='**',
  ),
  container=io.Container(
    record_connection=io.Connection.Asset.Record(
      connection_id=connection.Id(
        value='haojin_dev_test_S3',
      ),
      details={
        'bucket': ascend.Value(
          string_value='ascend-io-sample-data-read',
        ),
        'object_pattern': ascend.Value(
          string_value='a-guided-tour/NYC-TAXI/weather/weather.csv',
        ),
        'object_pattern_type': ascend.Value(
          union_value=ascend.Union(
            tag='match',
            value=ascend.Value(
              struct_value=ascend.Struct(
              ),
            ),
          ),
        ),
        'parser_type': ascend.Value(
          union_value=ascend.Union(
            tag='csv',
            value=ascend.Value(
              struct_value=ascend.Struct(
                fields={
                  'header': ascend.Value(
                    bool_value=True,
                  ),
                },
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
      seconds=35735712,
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
          name='Date',
          schema=schema.Schema(
            string=schema.String(
            ),
          ),
        ),
        schema.Field(
          name='Weather',
          schema=schema.Schema(
            string=schema.String(
            ),
          ),
        ),
        schema.Field(
          name='Precipitation',
          schema=schema.Schema(
            double=schema.Double(
            ),
          ),
        ),
      ],
    ),
  ),
  compute_configurations=None,
)
components_weather.append(component_NYC_Weather)

component_NYC_Weather_transform = definitions.Transform(
  id='NYC_Weather_transform',
  name='NYC Weather Transform',
  description='Transform from NYC Weather',
  input_ids=[
    'NYC_Weather',
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
            inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "weather", "NYC_Weather_transform.sql")).read_text(encoding="utf-8"),
          ),
          environment=function.Environment(
            container_image=environment.Spark.ContainerImage(
              name='quay.io/ascendio/pyspark:production-io.ascend-spark_2.12-3.1.0-SNAPSHOT',
              runtime='scala:2.12;spark:3.1.0',
            ),
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
components_weather.append(component_NYC_Weather_transform)

component_NYC_Weather_Partition_Filter = definitions.Transform(
  id='NYC_Weather_Partition_Filter',
  name='NYC Weather Partition Filter',
  description=' from NYC Weather Partition Filter Transform',
  input_ids=[
    'NYC_Weather_transform',
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
            inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "weather", "NYC_Weather_Partition_Filter.sql")).read_text(encoding="utf-8"),
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
                      'weather_date_ts',
                      '>',
                      "'2016-12-20T23:00:00Z'",
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
components_weather.append(component_NYC_Weather_Partition_Filter)

component_Weather_PySpark_ = definitions.Transform(
  id='Weather_PySpark_',
  name='Weather PySpark ',
  description='PySpark Transform from NYC Weather.',
  input_ids=[
    'NYC_Weather',
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
            inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "weather", "Weather_PySpark_.py")).read_text(encoding="utf-8"),
          ),
          environment=function.Environment(
            container_image=environment.Spark.ContainerImage(
              name='quay.io/ascendio/pyspark:production-io.ascend-spark_2.12-3.2.0-SNAPSHOT',
              runtime='scala:2.12;spark:3.2.0',
            ),
          ),
        ),
      ),
      reduction=operator.Reduction(
        no_reduction=operator.Reduction.NoReduction(
        ),
      ),
    ),
  ),
  assigned_priority=component.Priority(
  ),
)
components_weather.append(component_Weather_PySpark_)

component__S3_Sink_ = definitions.WriteConnector(
  id='_S3_Sink_',
  name='[S3 Sink]',
  description='',
  input_id='NYC_Weather_transform',
  container=io.Container(
    record_connection=io.Connection.Asset.Record(
      connection_id=connection.Id(
        value='S3_Sink',
      ),
      details={
        'formatter_type': ascend.Value(
          union_value=ascend.Union(
            tag='parquet',
            value=ascend.Value(
              struct_value=ascend.Struct(
              ),
            ),
          ),
        ),
        'object_name_format_prefix': ascend.Value(
          string_value='dev-haojin',
        ),
      },
    ),
  ),
  assigned_priority=component.Priority(
  ),
  bytes=None,
  records=component.Sink.ToRecords(
  ),
  compute_configurations=[
    expression.StageComputeConfiguration(
      stage=expression.Stage(
        write=expression.Stage.Write(
        ),
      ),
      configuration=operator.ComputeConfiguration(
      ),
    ),
  ],
)
components_weather.append(component__S3_Sink_)


## data feeds for dataflow 'weather'
data_feeds_weather = []


## component groups for dataflow 'weather'
groups_weather = []


dataflow_weather = definitions.Dataflow(
  id='weather',
  name='weather',
  description='',
  components=components_weather,
  data_feeds=data_feeds_weather,
  data_feed_connectors=data_feed_connectors_weather,
  groups=groups_weather,
)

def apply_dataflow(client: Client, data_service_id: str, dataflow: definitions.Dataflow):
  DataflowApplier(client).apply(data_service_id, dataflow)

if __name__ == "__main__":
  client = Client("dev-haojin.ascend.io")
  apply_dataflow(client, "Haojin_dev_test", dataflow_weather)