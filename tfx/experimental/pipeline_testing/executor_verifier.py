# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Recording pipeline from MLMD metadata."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import types
from distutils.dir_util import copy_tree

import tensorflow as tf
import tensorflow_data_validation as tfdv
from typing import Text

import absl
import tensorflow_transform as tft
import tensorflow_model_analysis as tfma
from tfx.components.evaluator import constants

from tfx.orchestration import data_types
from tfx.orchestration import metadata
from tfx.orchestration.metadata import _EXECUTION_TYPE_KEY_PIPELINE_NAME, _EXECUTION_TYPE_KEY_PIPELINE_ROOT
from ml_metadata.proto import metadata_store_pb2
from collections import defaultdict
from tfx.components.trainer import fn_args_utils
from tfx.types import artifact_utils
from tfx.proto import evaluator_pb2
from tfx.types import artifact_utils
from tfx.utils import io_utils
from tfx.utils import path_utils
from google.protobuf import json_format
from tensorflow_metadata.proto.v0 import anomalies_pb2

class ExecutorVerifier(object):
  def __init__(self, record_dir, pipeline_info, metadata_connection_config, threshold=0.5):
    """
    threshold: between 0 and 1
    components_id: components to verify
    pipeline_info: for current pipeline 
    metadata_connection_config: for current pipeline
    """
    self._record_dir = record_dir
    self._metadata_connection_config = metadata_connection_config
    self._pipeline_info = pipeline_info
    self._threshold = threshold

    # default verifier
    self._verifier_map = {'ExampleValidator': self.validator_verifier,
                          'Trainer': self.trainer_verifier,
                          'Evaluator': self.evaluator_verifier}

  def trainer_verifier(self, component_id, output_dict):
    # compares two model files
    model_uri = output_dict['model'][0].uri

    component_id = output_dict['model'][0].custom_properties['producer_component'].string_value
    path = os.path.join(self._record_dir, component_id, 'model')
    print(path)
    print(model_uri, component_id)

  def evaluator_verifier(self, component_id, output_dict):
    # compares two evaluation proto files
    eval_result = tfma.load_eval_result(output_dict['evaluation'].uri),slicing_metrics
    expected_eval_result = tfma.load_eval_result(os.path.join(record_dir, component_id, 'evaluation')).slicing_metrics
    # tfma.load_validation_result(output_dict['blessing'].uri, "BLESSED")
    # tfma.load_validation_result(os.path.join(record_dir, component_id, 'blessing'))
    for eval_slice_metric, expected_eval_slice_metric in zip(eval_result.slicing_metrics, expected_eval_result.slicing_metrics):
        assert eval_slice_metric[0] == expected_eval_slice_metric[0]
        for output_name, output_dict in eval_slice_metric[1].items():
          for sub_key, sub_dict in output_dict.items():
            for metric_name, value_dict in sub_dict.items():
              value = value_dict['doubleValue']
              expected_value = expected_eval_slice_metric[1][output_name][sub_key][metric_name]['doubleValue']
              if value != expected_value:
                if expected_value:
                  relative_diff = abs(value - expected_value)/abs(expected_value)
                  if not (expected_value and relative_diff <= self.threshold):
                    absl.logging.warning("Relative difference {} exceeded threshold {}".format(relative_diff, self.threshold))
                else:
                  absl.logging.warning("metric_name {} value {} expected_value {}".format(metric_name, value, expected_value))

  def validator_verifier(self, component_id, output_dict):
    # compares two validation proto files
    anomalies = io_utils.parse_pbtxt_file(
        os.path.join(output_dict['anomalies'].uri, 'anomalies.pbtxt'),
        anomalies_pb2.Anomalies())
    expected_anomalies = io_utils.parse_pbtxt_file(
      os.path.join(output_dict['anomalies'].uri, 'anomalies.pbtxt'),
      anomalies_pb2.Anomalies())
    if (expected_anomalies.anomaly_info != anomalies.anomaly_info):
      absl.logging.warning("anomaly info different")

  def set_verifier(self, component_id: Text, verifier_fn:types.FunctionType):
    # compares user verifier
    self._verifier_map[component_id] = verifier_fn

  def verify(self, component_ids = []):
    with metadata.Metadata(self._metadata_connection_config) as m:
      context = m.get_pipeline_run_context(self._pipeline_info)
      for execution in m.store.get_executions_by_context(context.id):
        component_id = execution.properties['component_id'].string_value
        if component_id not in component_ids:
          continue
        output_config = execution.properties['output_config'].string_value
        output_dict = defaultdict(list)
        for event in m.store.get_events_by_execution_ids([execution.id]):
          if event.type ==  metadata_store_pb2.Event.OUTPUT:
            artifacts = m.store.get_artifacts_by_id([event.artifact_id])
            for step in event.path.steps:
              if step.HasField("key"):
                output_dict[step.key] = artifacts
        verifier_fn = self._verifier_map.get(component_id, None)
        if verifier_fn:
          verifier_fn(component_id, output_dict)
