from tfx.orchestration import metadata
from ml_metadata.proto import metadata_store_pb2
from tfx.types import artifact_utils

def get_component_output_map(metadata_connection_config, pipeline_info):
  """returns a dictionary of component_id: output"""
  output_map = {}
  with metadata.Metadata(metadata_connection_config) as m:
    context = m.get_pipeline_run_context(pipeline_info)
    for execution in m.store.get_executions_by_context(context.id):
      component_id = execution.properties['component_id'].string_value
      output_config = execution.properties['output_config'].string_value
      output_dict = {}
      for event in m.store.get_events_by_execution_ids([execution.id]):
        if event.type ==  metadata_store_pb2.Event.OUTPUT:
          artifacts = m.store.get_artifacts_by_id([event.artifact_id])
          for step in event.path.steps:
            if step.HasField("key"):
              output_dict[step.key] = artifact_utils.get_single_instance(artifacts)
      output_map[component_id] = output_dict
  return output_map

def _group_metric_by_slice(eval_result_metric):
  slice_map = {}
  for metric in eval_result_metric:
    slice_map[metric[0]] = {k: v['doubleValue'] for k, v in metric[1][''][''].items()}
  return slice_map


def compare_eval_results(eval_result, expected_eval_result, threshold):
  eval_slicing_metrics = eval_result.slicing_metrics
  expected_slicing_metrics = expected_eval_result.slicing_metrics
  # print(eval_slicing_metrics)
  print(expected_slicing_metrics)
  slice_map = _group_metric_by_slice(eval_slicing_metrics)
  expected_slice_map = _group_metric_by_slice(expected_slicing_metrics)
  # print("slice_map", slice_map)
  # print('expected_slice_map', expected_slice_map)
  for slice_item, metric_dict in slice_map.items():
    for metric_name, value in metric_dict.items():
      if slice_item not in expected_slice_map or metric_name not in expected_slice_map[slice_item]:
        # print("slice_item", slice_item)
        # print("metric_name", metric_name)
        continue
      expected_value = expected_slice_map[slice_item][metric_name]
      if value != expected_value:
        if expected_value:
          relative_diff = abs(value - expected_value)/abs(expected_value)
          if not (expected_value and relative_diff <= threshold):
            absl.logging.warning("Relative difference {} exceeded threshold {}".format(relative_diff, threshold))
        else:
          absl.logging.warning("metric_name {} value {} expected_value {}".format(metric_name, value, expected_value))

  # for eval_slice_metric, expected_eval_slice_metric in zip(eval_slicing_metrics, expected_slicing_metrics):
  #       assert eval_slice_metric[0] == expected_eval_slice_metric[0]
  #       for output_name, output_dict in eval_slice_metric[1].items():
  #         for sub_key, sub_dict in output_dict.items():
  #           for metric_name, value_dict in sub_dict.items():
  #             value = value_dict['doubleValue']
  #             expected_value = expected_eval_slice_metric[1][output_name][sub_key][metric_name]['doubleValue']
  #             if value != expected_value:
  #               if expected_value:
  #                 relative_diff = abs(value - expected_value)/abs(expected_value)
  #                 if not (expected_value and relative_diff <= threshold):
  #                   absl.logging.warning("Relative difference {} exceeded threshold {}".format(relative_diff, threshold))
  #               else:
  #                 absl.logging.warning("metric_name {} value {} expected_value {}".format(metric_name, value, expected_value))
