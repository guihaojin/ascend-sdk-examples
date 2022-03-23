import os
import pathlib

from ascend.sdk.client import Client
from ascend.protos.api.api_pb2 import DataService, Dataflow, ReadConnector, Transform, WriteConnector

from ascend.protos.component.component_pb2 import Source, View, Sink, Id
from ascend.protos.operator.operator_pb2 import Operator, Spark, Reduction
from ascend.protos.pattern.pattern_pb2 import Pattern
from ascend.protos.format.format_pb2 import Xsv
from ascend.protos.core.core_pb2 import Periodical
from google.protobuf.duration_pb2 import Duration
import ascend.protos.function.function_pb2 as function
import ascend.protos.ascend.ascend_pb2 as ascend
import ascend.protos.connection.connection_pb2 as connection

import ascend.protos.io.io_pb2 as io
import ascend.protos.schema.schema_pb2 as schema

from google.protobuf.json_format import MessageToJson

# credential configured in ~/.ascend/credentials
client = Client('dev-haojin.ascend.io')

data_service_id = 'Haojin_dev_test'
dataflow_id = 'sdk_workflow'

# ds = client.get_data_service(data_service_id)
# print(ds)
data_service = DataService(id=data_service_id)

# dataflow = client.create_dataflow('Haojin_dev_test', Dataflow(id='sdk_workflow', name='sdk dataflow'))
# print(dataflow)

# dataflow = client.get_dataflow(data_service_id, dataflow_id)
# print(dataflow)
dataflow = Dataflow(id=dataflow_id)

# pattern = Pattern(exact_match="a-guided-tour/NYC-TAXI/weather/weather.csv")
# s3_container = io_pb2.Aws.S3.Container(region='us-east-1', bucket='ascend-io-sample-data-read')
# source = Source(
#     pattern=Pattern(exact_match="a-guided-tour/NYC-TAXI/weather/weather.csv"), 
#     container=io_pb2.Container(s3=io_pb2.Aws.S3.Container(region='us-east-1', bucket='ascend-io-sample-data-read')),
#     records=Source.FromRecords(
#         schema=schema.Map(
#             field=[
#                 schema.Field(name='Date', schema=schema.Schema(string=schema.String())),
#                 schema.Field(name='Weather', schema=schema.Schema(string=schema.String())),
#                 schema.Field(name='Precipitation', schema=schema.Schema(double=schema.Double())),
#             ]
#         )
#     ))

def bytes_read_connector():
    return ReadConnector(
        id='sdk_rc_x',
        name='SDK Read Connector',
        source=Source(
            pattern=Pattern(exact_match="a-guided-tour/NYC-TAXI/weather/weather.csv"),
            container=io.Container(s3=io.Aws.S3.Container(region='us-east-1', bucket='ascend-io-sample-data-read')),
            bytes=Source.FromBytes(parser=Operator(xsv_parser=Xsv.Parser(
                delimiter=",",
                header_line_count=1,
                schema=schema.Map(field=[
                    schema.Field(
                        name="Date",
                        schema=schema.Schema(string=schema.String()),
                    ),
                    schema.Field(
                        name="Weather",
                        schema=schema.Schema(string=schema.String()),
                    ),
                    schema.Field(
                        name="Precipitation",
                        schema=schema.Schema(float=schema.Float()),
                    ),
                ], ),
            )
            ),
            ),
            updatePeriodical=Periodical(
                period=Duration(seconds=134217727),
                offset=Duration(seconds=101025131),
          ),
        )
    )

transform = Transform(
    id='sdk_transform',
    name='SDK transform',
    inputs=[Transform.Input(type="Source", uuid='edcdcd3f-3a96-4e2c-a269-03feef58f544')], # RC uuid
    view=View(
        input=[
            Id(value='sdk_rc_x'),
        ],
        operator=Operator(
            spark_function=Spark.Function(
                executable=io.Executable(
                    code=io.Code(
                        language=function.Code.Language(sql=function.Code.Language.Sql()),
                        source=io.Code.Source(
                            inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "weather", "NYC_Weather_transform.sql")).read_text(encoding="utf-8"),
                        ),
                    ),
                ),
                reduction=Reduction(full=Reduction.Full()),
            ),
        ),
    )
)

write_connector = WriteConnector(
    id='sdk_wc',
    name="SDK Write Connector",
    inputType="View",
    inputUUID='5ef19d49-ea4b-425a-99fc-c3964369f426', # transform uuid
    sink=Sink(
        input=Id(value='sdk_transform'),
        container=io.Container(
            record_connection=io.Connection.Asset.Record(
                connection_id=connection.Id(value='S3_Sink'),
                details={
                    'formatter_type': ascend.Value(
                        union_value=ascend.Union(
                            tag='parquet',
                            value=ascend.Value(
                                struct_value=ascend.Struct(),
                            ),
                        ),
                    ),
                    'object_name_format_prefix': ascend.Value(
                        string_value='dev-haojin-sdk',
                    ),
                },
            ),
        ),
        records=Sink.ToRecords(),
    ),
)

# client.create_read_connector(data_service_id=data_service_id, dataflow_id=dataflow_id, body=bytes_read_connector())

# client.create_transform(data_service_id, dataflow_id, transform)

# client.create_write_connector(data_service_id, dataflow_id, write_connector)
