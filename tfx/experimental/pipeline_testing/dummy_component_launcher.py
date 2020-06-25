# Lint as: python2, python3
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
"""dummy component launcher which launches python executors and dummy executors in process."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import abc
from typing import Any, Dict, List, Text, Type
import absl

from tfx import types
from tfx.components.base import base_executor
from tfx.orchestration.launcher.in_process_component_launcher import InProcessComponentLauncher
from tfx.experimental.pipeline_testing.dummy_executor import BaseDummyExecutor

def create_dummy_launcher_class(record_dir, component_ids, custom_component_map):

  class DummyComponentLauncher(InProcessComponentLauncher):
    """Responsible for launching a dummy executor.

    The executor will be launched in the same process of the rest of the
    component, i.e. its driver and publisher.
    """
    def __init__(self, **kwargs):
      super(DummyComponentLauncher, self).__init__(**kwargs)
      absl.logging.info("DummyComponentLauncher")
      self._executors_dict = {}
      self._dummy_component_id = []
      self._record_dir = record_dir
      self._component_ids = component_ids
      self._custom_component_map = custom_component_map

    def _run_executor(self, execution_id: int,
                      input_dict: Dict[Text, List[types.Artifact]],
                      output_dict: Dict[Text, List[types.Artifact]],
                      exec_properties: Dict[Text, Any]) -> None:
      """Execute underlying component implementation."""
      component_id = self._component_info.component_id

      executor_context = base_executor.BaseExecutor.Context(
          beam_pipeline_args=self._beam_pipeline_args,
          tmp_dir=os.path.join(self._pipeline_info.pipeline_root, '.temp', ''),
          unique_id=str(execution_id))
      if component_id in self._component_ids:
        executor = BaseDummyExecutor(component_id,
                                     self._record_dir,
                                     executor_context)
        executor.Do(input_dict, output_dict, exec_properties)
      elif component_id in self._custom_component_map.keys():
        executor = self._custom_component_map[component_id](executor_context)
        executor.Do(input_dict, output_dict, exec_properties)
      else:
        super(BaseDummyComponentLauncher, self)._run_executor(execution_id,
                                                              input_dict,
                                                              output_dict,
                                                              exec_properties)
  return DummyComponentLauncher
