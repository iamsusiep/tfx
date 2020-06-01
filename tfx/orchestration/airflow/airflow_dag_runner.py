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
"""Definition of Airflow TFX runner."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
from typing import Any, Dict, Optional, Text, Tuple, Type, Union

import absl
from airflow import models

from tfx.components.base import base_component
from tfx.orchestration import pipeline
from tfx.orchestration import tfx_runner
from tfx.orchestration.airflow import airflow_component
from tfx.orchestration.config import base_component_config
from tfx.orchestration.config import pipeline_config
from tfx.orchestration.launcher import base_component_launcher


def find_component_launch_info(
    p_config: pipeline_config.PipelineConfig,
    component: base_component.BaseComponent,
) -> Tuple[Type[base_component_launcher.BaseComponentLauncher],
           Optional[base_component_config.BaseComponentConfig]]:
  """Find a launcher and component config to launch the component.

  The default lookup logic goes through the `supported_launcher_classes`
  in sequence for each config from the `default_component_configs`. User can
  override a single component setting by `component_config_overrides`. The
  method returns the first component config and launcher which together can
  launch the executor_spec of the component.
  Subclass may customize the logic by overriding the method.

  Args:
    p_config: the pipeline config.
    component: the component to launch.

  Returns:
    The found tuple of component launcher class and the compatible component
    config.

  Raises:
    RuntimeError: if no supported launcher is found.
  """
  if component.id in p_config.component_config_overrides:
    component_configs = [p_config.component_config_overrides[component.id]]
  else:
    # Add None to the end of the list to find launcher with no component
    # config
    component_configs = p_config.default_component_configs + [None]

  for component_config in component_configs:
    for component_launcher_class in p_config.supported_launcher_classes:
      if component_launcher_class.can_launch(component.executor_spec,
                                             component_config):
        return (component_launcher_class, component_config)
  raise RuntimeError('No launcher info can be found for component "%s".' %
                     component.component_id)


class AirflowPipelineConfig(pipeline_config.PipelineConfig):
  """Pipeline config for AirflowDagRunner."""

  def __init__(self, airflow_dag_config: Dict[Text, Any] = None, **kwargs):
    """Creates an instance of AirflowPipelineConfig.

    Args:
      airflow_dag_config: Configs of Airflow DAG model. See
        https://airflow.apache.org/_api/airflow/models/dag/index.html#airflow.models.dag.DAG
          for the full spec.
      **kwargs: keyword args for PipelineConfig.
    """

    super(AirflowPipelineConfig, self).__init__(**kwargs)
    self.airflow_dag_config = airflow_dag_config or {}


class AirflowDagRunner(tfx_runner.TfxRunner):
  """Tfx runner on Airflow."""

  def __init__(self,
               config: Optional[Union[Dict[Text, Any],
                                      AirflowPipelineConfig]] = None):
    """Creates an instance of AirflowDagRunner.

    Args:
      config: Optional Airflow pipeline config for customizing the launching of
        each component.
    """
    if config and not isinstance(config, AirflowPipelineConfig):
      absl.logging.warning(
          'Pass config as a dict type is going to deprecated in 0.1.16. Use AirflowPipelineConfig type instead.',
          PendingDeprecationWarning)
      config = AirflowPipelineConfig(airflow_dag_config=config)
    super(AirflowDagRunner, self).__init__(config)

  def run(self, tfx_pipeline: pipeline.Pipeline):
    """Deploys given logical pipeline on Airflow.

    Args:
      tfx_pipeline: Logical pipeline containing pipeline args and components.

    Returns:
      An Airflow DAG.
    """

    # Merge airflow-specific configs with pipeline args
    airflow_dag = models.DAG(
        dag_id=tfx_pipeline.pipeline_info.pipeline_name,
        **self._config.airflow_dag_config)
    if 'tmp_dir' not in tfx_pipeline.additional_pipeline_args:
      tmp_dir = os.path.join(tfx_pipeline.pipeline_info.pipeline_root, '.temp',
                             '')
      tfx_pipeline.additional_pipeline_args['tmp_dir'] = tmp_dir

    component_impl_map = {}
    for tfx_component in tfx_pipeline.components:

      (component_launcher_class,
       component_config) = find_component_launch_info(self._config,
                                                      tfx_component)
      current_airflow_component = airflow_component.AirflowComponent(
          airflow_dag,
          component=tfx_component,
          component_launcher_class=component_launcher_class,
          pipeline_info=tfx_pipeline.pipeline_info,
          enable_cache=tfx_pipeline.enable_cache,
          metadata_connection_config=tfx_pipeline.metadata_connection_config,
          beam_pipeline_args=tfx_pipeline.beam_pipeline_args,
          additional_pipeline_args=tfx_pipeline.additional_pipeline_args,
          component_config=component_config)
      component_impl_map[tfx_component] = current_airflow_component
      for upstream_node in tfx_component.upstream_nodes:
        assert upstream_node in component_impl_map, ('Components is not in '
                                                     'topological order')
        current_airflow_component.set_upstream(
            component_impl_map[upstream_node])

    return airflow_dag
