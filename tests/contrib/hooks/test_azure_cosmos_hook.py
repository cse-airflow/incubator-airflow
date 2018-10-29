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
                conn_id='azcosmos_test_key',
                conn_type='azure_cosmos',
                login='client_id',
                password='client secret',
                extra=json.dumps({"endpoint_uri": "INSERT_ENDPOINT",
                                  "master_key": "INSERT_MASTERKEY",
                                  "database_name": "INSERT_DB_NAME",
                                  "collection_name": "INSERT_COLLECTIONNAME"})
            )
        )

    def simple_roundtrip_test(self):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azcosmos_test_key')
        self.assertEqual(hook.conn_id, 'azcosmos_test_key')
        hook.get_conn()
        total_documents = hook.get_documents("SELECT * FROM c")
        before_count = len(total_documents)
        hook.insert_document({
            "id": "airflow_test_item",
            "temp": "stuff"})
        total_documents_after = hook.get_documents("SELECT * FROM c")
        after_count = len(total_documents_after)
        self.assertEqual(before_count + 1, after_count)
        hook.delete_document("airflow_test_item")
        total_documents_after_delete = hook.get_documents("SELECT * FROM c")
        after_delete_count = len(total_documents_after_delete)
        self.assertEqual(before_count, after_delete_count)


if __name__ == '__main__':
    unittest.main()
