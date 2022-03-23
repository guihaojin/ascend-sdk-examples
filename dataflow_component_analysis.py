from ascendtools.test.data._ascend._ascend import components_test_graph_deploy, groups_test_graph_deploy, definitions

# Utils to check whether an Ascend dataflow component is legacy or not.

rc_components = []
wc_components = []
transform_components = []

legacy_components = []

for component in components_test_graph_deploy:
  if type(component) == definitions.ReadConnector:
    rc_components.append(component)
  elif type(component) == definitions.WriteConnector:
    wc_components.append(component)
  elif type(component) == definitions.Transform:
    transform_components.append(component)

def is_legacy_rw(component):
  return component.container.record_connection.ByteSize() == 0

def is_legacy_transform(component):
  return component.operator.spark_function.ByteSize() == 0

for component in rc_components + wc_components:
  if is_legacy_rw(component):
    print(component.id)
    legacy_components.append(component.id)
    print('yes')
  # else:
  #   print('no')

for component in transform_components:
  if is_legacy_transform(component):
    print(component.id)
    legacy_components.append(component.id)
    print('yes')
  # else:
  #   print('no')

print(len(legacy_components))
