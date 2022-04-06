from ascend.sdk.client import Client
from ascend.sdk.render import download_dataflow
from ascend.protos.api.api_pb2 import DataService

from google.protobuf.json_format import MessageToJson

# credential configured in ~/.ascend/credentials
client = Client('dev-haojin.ascend.io')
data_services = client.list_data_services()
print(data_services)

# convert to JSON format
data_services_json = MessageToJson(data_services)
print(data_services_json)

# download_dataflow(client, 'Haojin_dev_test', 'weather', resource_base_path='./partition_filter')
download_dataflow(client, '_ascend', 'test', resource_base_path='./partition_join_and_filter')
