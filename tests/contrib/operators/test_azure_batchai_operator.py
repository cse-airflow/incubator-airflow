# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys
import unittest

from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.contrib.operators.azure_batchai_operator import AzureBatchAIOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

# TEST SCRIPT BELOW

# def main():
#     test_operator = AzureBatchAIOperator(
#         'azure_batchai_default',
#         'batch-ai-test-rg',
#         'batch-ai-workspace',
#         'batch-ai-cluster',
#         'eastus',
#         environment_variables={},
#         volumes=[],
#         memory_in_gb=2.0,
#         cpu=1.0,
#         task_id='test_operator'
#         )
#     print "testing execute of batch ai operator....."
#     test_operator.execute()


class TestAzurBatchAIOperator(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.azure_batchai_operator.AzureBatchAIHook')
    def test_execute(self, abai_mock):
        abai_mock.return_value.get_state_exitcode.return_value = "Terminated", 0
        self.batch = AzureBatchAIOperator('azure_batchai_default',
                                    'batch-ai-test-rg',
                                    'batch-ai-workspace',
                                    'batch-ai-cluster',
                                    'eastus',
                                    environment_variables={},
                                    volumes=[],
                                    memory_in_gb=2.0,
                                    cpu=1.0,
                                    task_id='test_operator')
        self.batch.execute()
         
        # self.assertEqual(aci_mock.return_value.create_or_update.call_count, 1)
        # (called_rg, called_cn, called_cg), _ = aci_mock.return_value.create_or_update.call_args
        self.assertEqual(self.batch.resource_group, 'batch-ai-test-rg')
        self.assertEqual(self.batch.workspace_name, 'batch-ai-workspace')
        self.assertEqual(self.batch.cluster_name, 'batch-ai-cluster')
        self.assertEqual(self.batch.location, 'eastus')
    
    @mock.patch('airflow.contrib.operators.azure_batchai_operator.AzureBatchAIHook')
    
    def test_execute_with_failures(self, abai_mock):
        abai_mock.return_value.get_state_exitcode.return_value = "Terminated", 1
        print abai_mock.return_value.get_state_exitcode.return_value
        self.batch = AzureBatchAIOperator('azure_default',
                                    'batch-ai-test-rg',
                                    'batch-ai-workspace',
                                    'batch-ai-cluster',
                                    'eastus',
                                    environment_variables={},
                                    volumes=[],
                                    memory_in_gb=2.0,
                                    cpu=1.0,
                                    task_id='test_operator')
        # self.batch.return_value.get_state_exitcode.return_value = "Terminated", 1
        with self.assertRaises(AirflowException):
            self.batch.execute()
        # self.assertEqual(aci_mock.return_value.delete.call_count, 1)


    #  @mock.patch("airflow.contrib.operators."
    #             "azure_container_instances_operator.AzureContainerInstanceHook")
    
    # def test_execute_with_messages_logs(self, aci_mock):
    #     aci_mock.return_value.get_state_exitcode.side_effect = [("Running", 0),
    #                                                             ("Terminated", 0)]
    #     aci_mock.return_value.get_messages.return_value = ["test", "messages"]
    #     aci_mock.return_value.get_logs.return_value = ["test", "logs"]
    #     aci = AzureContainerInstancesOperator(None, None,
    #                                           'resource-group', 'container-name',
    #                                           'container-image', 'region',
    #                                           task_id='task')
    #     aci.execute(None)
    #      self.assertEqual(aci_mock.return_value.create_or_update.call_count, 1)
    #     self.assertEqual(aci_mock.return_value.get_state_exitcode.call_count, 2)
    #     self.assertEqual(aci_mock.return_value.get_messages.call_count, 1)
    #     self.assertEqual(aci_mock.return_value.get_logs.call_count, 1)
    #     self.assertEqual(aci_mock.return_value.delete.call_count, 1)

if __name__ == '__main__':
    unittest.main()