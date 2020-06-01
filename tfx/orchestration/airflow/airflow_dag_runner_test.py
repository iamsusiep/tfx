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
"""Tests for tfx.orchestration.airflow.airflow_dag_runner."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
import tensorflow as tf

from tfx import types
from tfx.components.base import base_component
from tfx.components.base import base_executor
from tfx.components.base import executor_spec
# if I remove this import, test passed.
# but this dag runner is used in airflow e2e example, and that test passed.
from tfx.orchestration.airflow import airflow_dag_runner  # pylint: disable=unused-import
from tfx.types import component_spec


class _ArtifactTypeA(types.Artifact):
  TYPE_NAME = 'ArtifactTypeA'


class _ArtifactTypeB(types.Artifact):
  TYPE_NAME = 'ArtifactTypeB'


class _ArtifactTypeC(types.Artifact):
  TYPE_NAME = 'ArtifactTypeC'


class _ArtifactTypeD(types.Artifact):
  TYPE_NAME = 'ArtifactTypeD'


class _ArtifactTypeE(types.Artifact):
  TYPE_NAME = 'ArtifactTypeE'


class _FakeComponentSpecA(types.ComponentSpec):
  PARAMETERS = {}
  INPUTS = {}
  OUTPUTS = {'output': component_spec.ChannelParameter(type=_ArtifactTypeA)}


class _FakeComponentSpecB(types.ComponentSpec):
  PARAMETERS = {}
  INPUTS = {'a': component_spec.ChannelParameter(type=_ArtifactTypeA)}
  OUTPUTS = {'output': component_spec.ChannelParameter(type=_ArtifactTypeB)}


class _FakeComponentSpecC(types.ComponentSpec):
  PARAMETERS = {}
  INPUTS = {
      'a': component_spec.ChannelParameter(type=_ArtifactTypeA),
      'b': component_spec.ChannelParameter(type=_ArtifactTypeB)
  }
  OUTPUTS = {'output': component_spec.ChannelParameter(type=_ArtifactTypeC)}


class _FakeComponentSpecD(types.ComponentSpec):
  PARAMETERS = {}
  INPUTS = {
      'b': component_spec.ChannelParameter(type=_ArtifactTypeB),
      'c': component_spec.ChannelParameter(type=_ArtifactTypeC),
  }
  OUTPUTS = {'output': component_spec.ChannelParameter(type=_ArtifactTypeD)}


class _FakeComponentSpecE(types.ComponentSpec):
  PARAMETERS = {}
  INPUTS = {
      'a': component_spec.ChannelParameter(type=_ArtifactTypeA),
      'b': component_spec.ChannelParameter(type=_ArtifactTypeB),
      'd': component_spec.ChannelParameter(type=_ArtifactTypeD),
  }
  OUTPUTS = {'output': component_spec.ChannelParameter(type=_ArtifactTypeE)}


class _FakeComponent(base_component.BaseComponent):

  SPEC_CLASS = types.ComponentSpec
  EXECUTOR_SPEC = executor_spec.ExecutorClassSpec(base_executor.BaseExecutor)

  def __init__(self, spec: types.ComponentSpec):
    instance_name = spec.__class__.__name__.replace(
        '_FakeComponentSpec', '')
    super(_FakeComponent, self).__init__(
        spec=spec, instance_name=instance_name)


class AirflowDagRunnerTest(tf.test.TestCase):

  def testAirflowDagRunnerInitBackwardCompatible(self):
    airflow_config = {
        'schedule_interval': '* * * * *',
        'start_date': datetime.datetime(2019, 1, 1)
    }
    print(airflow_config)


if __name__ == '__main__':
  tf.test.main()
