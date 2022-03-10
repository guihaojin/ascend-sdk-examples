from ascend.sdk.client import Client
from ascend.protos.api.api_pb2 import DataService

from google.protobuf.json_format import MessageToJson

# credential configured in ~/.ascend/credentials
client = Client('dev-haojin.ascend.io')
data_services = client.list_data_services()
print(data_services)

# convert to JSON format
data_services_json = MessageToJson(data_services)
print(data_services_json)
