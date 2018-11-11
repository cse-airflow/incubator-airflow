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


import json
import unittest

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.azure_cosmos_hook import AzureCosmosDBHook

from airflow import configuration
from airflow import models
from airflow.utils import db

try:
    from unittest import mock

except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class TestAzureCosmosDbHook(unittest.TestCase):

    # Set up an environment to test with
    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            models.Connection(
                conn_id='azure_cosmos_test_key_id',
                conn_type='azure_cosmos',
                login='https://test_endpoint:443',
                password='magic_test_key',
                extra=json.dumps({"database_name": "test_database_default",
                                  "collection_name": "test_collection_default"})
            )
        )

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_create_database(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.cosmos.create_database('test_database_name')
        expected_calls = [mock.call().CreateDatabase({'id': 'test_database_name'})]
        cosmos_mock.assert_any_call('https://test_endpoint:443', {'masterKey': 'magic_test_key'})
        cosmos_mock.assert_has_calls(expected_calls)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_create_database_exception(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.assertRaises(AirflowException, self.cosmos.create_database, None)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_create_container_exception(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.assertRaises(AirflowException, self.cosmos.create_collection, None)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_create_container(self, cosmos_mock):
        # cosmos_mock.return_value.get_state_exitcode.return_value = "Terminated", 0
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.cosmos.create_collection('test_collection_name', 'test_database_name')
        expected_calls = [mock.call().CreateContainer(
            'dbs/test_database_name',
            {'id': 'test_collection_name'})]
        cosmos_mock.assert_any_call('https://test_endpoint:443', {'masterKey': 'magic_test_key'})
        cosmos_mock.assert_has_calls(expected_calls)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_create_container_default(self, cosmos_mock):
        # cosmos_mock.return_value.get_state_exitcode.return_value = "Terminated", 0
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.cosmos.create_collection('test_collection_name')
        expected_calls = [mock.call().CreateContainer(
            'dbs/test_database_default',
            {'id': 'test_collection_name'})]
        cosmos_mock.assert_any_call('https://test_endpoint:443', {'masterKey': 'magic_test_key'})
        cosmos_mock.assert_has_calls(expected_calls)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_delete_database(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.cosmos.delete_database('test_database_name')
        expected_calls = [mock.call().DeleteDatabase('dbs/test_database_name')]
        cosmos_mock.assert_any_call('https://test_endpoint:443', {'masterKey': 'magic_test_key'})
        cosmos_mock.assert_has_calls(expected_calls)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_delete_database_exception(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.assertRaises(AirflowException, self.cosmos.delete_database, None)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_delete_container_exception(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.assertRaises(AirflowException, self.cosmos.delete_collection, None)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_delete_container(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.cosmos.delete_collection('test_collection_name', 'test_database_name')
        expected_calls = [mock.call().DeleteContainer('dbs/test_database_name/colls/test_collection_name')]
        cosmos_mock.assert_any_call('https://test_endpoint:443', {'masterKey': 'magic_test_key'})
        cosmos_mock.assert_has_calls(expected_calls)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_delete_container_default(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.cosmos.delete_collection('test_collection_name')
        expected_calls = [mock.call().DeleteContainer('dbs/test_database_default/colls/test_collection_name')]
        cosmos_mock.assert_any_call('https://test_endpoint:443', {'masterKey': 'magic_test_key'})
        cosmos_mock.assert_has_calls(expected_calls)


if __name__ == '__main__':
    unittest.main()
