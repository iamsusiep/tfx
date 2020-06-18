from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from typing import Any, Dict, List, Text, Optional
from tfx import types
from tfx.components.base import base_executor
from tfx.types.artifact import Artifact
from tfx.orchestration import metadata
from tfx.types import standard_artifacts

import json
import absl
import os
import filecmp
from distutils.dir_util import copy_tree
from tfx.orchestration.metadata import Metadata


class DummyExecutor(base_executor.BaseExecutor):
  def __init__(self, component_id, record_dir, context):
    super(DummyExecutor, self).__init__(context)
    absl.logging.info("Running DummyExecutor, component_id %s", component_id)
    self._component_id = component_id
    self._record_dir = record_dir

  def _compare_contents(self, uri: Text, expected_uri: Text):
    """
    TODO: contents change every run
    recursively compare two directories,
    Files are equal if names and contents are equal.
    """
    dirs_cmp = filecmp.dircmp(uri, expected_uri)
    absl.logging.info("uri: %s", uri)
    absl.logging.info("expected_uri: %s", expected_uri)
    if len(dirs_cmp.left_only) > 0 or len(dirs_cmp.right_only) > 0 or \
      len(dirs_cmp.funny_files) > 0:
      return False

    (_, mismatch, errors) = filecmp.cmpfiles(
        uri, expected_uri, dirs_cmp.common_files, shallow=False)

    if len(mismatch) > 0 or len(errors) > 0:
      return False

    for common_dir in dirs_cmp.common_dirs:
      new_dir1 = os.path.join(uri, common_dir)
      new_dir2 = os.path.join(expected_uri, common_dir)
      if not self._compare_contents(new_dir1, new_dir2):
        return False
    return True
 def Do(self, input_dict: Dict[Text, List[types.Artifact]],
       output_dict: Dict[Text, List[types.Artifact]],
       exec_properties: Dict[Text, Any]) -> None:
    print("input_dict", input_dict)
    print("output_dict", output_dict)
    for _, artifact_list in input_dict.items():
      for artifact in artifact_list:
        if artifact.type_name == 'ExternalArtifact':
            continue
        dest = artifact.uri
        src = dest.replace(os.environ['HOME'], "").replace("/tfx/pipelines/", "")
        src = src[:src.rfind('/')] # remove trailing number
        src = os.path.join(self._record_dir, src)
        if not self._compare_contents(dest, src):
          absl.logging.info("WARNING: input checker failed")

    for _, artifact_list in output_dict.items():
      for artifact in artifact_list:
        dest = artifact.uri
        src = dest.replace(os.environ['HOME'], "").replace("/tfx/pipelines/", "")
        src = src[:src.rfind('/')] # remove trailing number
        src = os.path.join(self._record_dir, src)
        copy_tree(src, dest)
        absl.logging.info('from %s, copied to %s', src, dest)



# class DummyExecutor(base_executor.BaseExecutor):
#   def __init__(self, component_id, metadata_dir, context):
#     super(DummyExecutor, self).__init__(context)
#     absl.logging.info("Running DummyExecutor, component_id %s", component_id)
#     self._component_id = component_id
#     self._metadata_connection_config = metadata.sqlite_metadata_connection_config(metadata_dir)
#     # self._metadata = Metadata(metadata.sqlite_metadata_connection_config(metadata_dir))
    
#     # self._record_dir = record_dir

#   def _compare_contents(self, uri: Text, expected_uri: Text):
#     """
#     TODO: contents change every run
#     recursively compare two directories,
#     Files are equal if names and contents are equal.
#     """
#     dirs_cmp = filecmp.dircmp(uri, expected_uri)
#     absl.logging.info("uri: %s", uri)
#     absl.logging.info("expected_uri: %s", expected_uri)
#     if len(dirs_cmp.left_only) > 0 or len(dirs_cmp.right_only) > 0 or \
#       len(dirs_cmp.funny_files) > 0:
#       return False

#     (_, mismatch, errors) = filecmp.cmpfiles(
#         uri, expected_uri, dirs_cmp.common_files, shallow=False)

#     if len(mismatch) > 0 or len(errors) > 0:
#       return False

#     for common_dir in dirs_cmp.common_dirs:
#       new_dir1 = os.path.join(uri, common_dir)
#       new_dir2 = os.path.join(expected_uri, common_dir)
#       if not self._compare_contents(new_dir1, new_dir2):
#         return False
#     return True

#   def Do(self, input_dict: Dict[Text, List[types.Artifact]],
#        output_dict: Dict[Text, List[types.Artifact]],
#        exec_properties: Dict[Text, Any]) -> None:
#     print("input_dict", input_dict)
#     print("output_dict", output_dict)
#     with metadata.Metadata(self._metadata_connection_config) as store:
#       for _, artifact_list in input_dict.items():
#         for artifact in artifact_list:
#           print("artifact.type_name", artifact.type_name)
#           stored_artifact_list = store.get_artifacts_by_type(artifact.type_name) # self._metadata.get_artifacts_by_type(artifact.type_name)
#           print("stored_artifact_list", stored_artifact_list)

#           expected_artifact = None
#           if artifact.type_name == 'ExternalArtifact' and len(stored_artifact_list) == 1:
#             expected_artifact = stored_artifact_list[0]
#           else:
#             for stored_artifact in stored_artifact_list:
#               if stored_artifact.custom_properties['name'].string_value == artifact.name:
#                 expected_artifact = stored_artifact
#                 break
#           assert expected_artifact is not None
#           src = expected_artifact.uri
#           dest = artifact.uri
#           if not self._compare_contents(dest, src):
#             absl.logging.info("WARNING: input checker failed")

#       for _, artifact_list in output_dict.items():
#         for artifact in artifact_list:
#           print("artifact.type_name", artifact.type_name)
#           print("artifact.name", artifact.name)
#           stored_artifact_list = store.get_artifacts_by_type(artifact.type_name) #self._metadata.get_artifacts_by_type(artifact.type_name)
#           expected_artifact = None
#           print("stored_artifact_list", stored_artifact_list)
#           if artifact.type_name == 'ExternalArtifact' and len(stored_artifact_list) == 1:
#             expected_artifact = stored_artifact_list[0]
#           else:
#             for stored_artifact in stored_artifact_list:
#               if stored_artifact.custom_properties['name'].string_value == artifact.name:
#                 expected_artifact = stored_artifact
#                 break
#           assert expected_artifact is not None
#           src = expected_artifact.uri
#           dest = artifact.uri
#           copy_tree(src, dest)
#           absl.logging.info('from %s, copied to %s', src, dest)
#                              executor_context)
