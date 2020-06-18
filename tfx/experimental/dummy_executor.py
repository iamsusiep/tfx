from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from typing import Any, Dict, List, Text
from tfx import types
from tfx.components.base import base_executor
from tfx.types.artifact import Artifact

import json
import absl
import os
import filecmp
from distutils.dir_util import copy_tree

class DummyExecutorFactory(object):
  def __init__(self, component_id, record_dir):
    self.component_id = component_id
    self.record_dir = record_dir

  def __call__(self, executor_context):
    obj = DummyExecutorFactory(self._component_id, self._record_dir)
    obj.__class__ = DummyExecutor
    return obj
# class _DummyGetter(object):
#   def __call__(self, component_id, record_dir):
#     obj = _DummyGetter()
#     obj.__class__ = make_dummy_executor(component_id, record_dir)
#     return obj
  # def __call__(self, containing_class, class_name):
  #   nested_class = getattr(containing_class, class_name)
  #   # make an instance of a simple object (this one will do), for which we can change the
  #   # __class__ later on.
  #   nested_instance = _DummyGetter()

  #   # set the class of the instance, the __init__ will never be called on the class
  #   # but the original state will be set later on by pickle.
  #   nested_instance.__class__ = nested_class
#   return nested_instance
class DummyExecutor(base_executor.BaseExecutor):
  def __init__(self, component_id, record_dir, executor_context):
    super(DummyExecutor, self).__init__(executor_context)
    self._component_id = component_id
    self._record_dir = record_dir

  def __reduce__(self):
    return (DummyExecutorFactory(), 
            (component_id, record_dir, ), 
            self.__dict__)


  def Do(self, input_dict: Dict[Text, List[types.Artifact]],
         output_dict: Dict[Text, List[types.Artifact]],
         exec_properties: Dict[Text, Any]) -> None:
    print("record_dir", record_dir)
    print("component_id", component_id)
    pass
