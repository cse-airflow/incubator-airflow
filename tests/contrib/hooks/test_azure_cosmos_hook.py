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

import unittest
import json

from airflow.contrib.hooks.azure_cosmos_hook import AzureCosmosDBHook

from airflow import configuration
from airflow import models
from airflow.utils import db


class TestAzureDocsmosDbHook(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            models.Connection(                
                conn_id='azcosmos_test_key_id',
                conn_type='azure_cosmos',
                login='ENDPOINT_URI',
                password='MASTER_KEY',
                extra=json.dumps({"database_name": "DATABASE_NAME",
                                  "collection_name": "COLLECTION_NAME"})
            )
        )

    def simple_roundtrip_test(self):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azcosmos_test_key_id')
        self.assertEqual(hook.conn_id, 'azcosmos_test_key_id')
        hook.get_conn()
        total_documents = hook.get_documents("SELECT * FROM c")
        before_count = len(total_documents)
        hook.insert_document({
            "id": "airflow_test_item",
            "temp": "stuff"})
        total_documents_after = hook.get_documents("SELECT * FROM c")
        after_count = len(total_documents_after)
        self.assertEqual(before_count + 1, after_count)
        created_document = hook.get_document('airflow_test_item')
        self.assertEqual(created_document['temp'], "stuff")
        hook.delete_document("airflow_test_item")
        total_documents_after_delete = hook.get_documents("SELECT * FROM c")
        after_delete_count = len(total_documents_after_delete)
        insert_new_documents = [{
            "id": "airflow_test_item",
            "temp": "stuff"},
            {"id": "airflow_test_item1",
            "temp": "stuff2"},
            {"id": "airflow_test_item2",
            "temp": "stuff3"}]

        hook.insert_documents(insert_new_documents)
        self.assertEqual(before_count, after_delete_count)


if __name__ == '__main__':
    unittest.main()
