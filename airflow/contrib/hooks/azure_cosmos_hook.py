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
# import pydocumentdb
import pydocumentdb.document_client as document_client
import uuid
from airflow.exceptions import AirflowBadRequest
from airflow.hooks.base_hook import BaseHook


class AzureCosmosDBHook(BaseHook):
    """
    Interacts with Azure CosmosDB.

    Endpoint, Master Key, Database and collection name should be extra field as
    {"endpoint_uri": "<ENDPOINT_URI>", "master_key":"<MASTER_KEY>",
    "database_name": "<DATABASE_NAME>", "collection_name": "COLLECTION_NAME"}.

    :param azure_cosmos_conn_id: Reference to the Azure CosmosDB connection.
    :type azure_cosmos_conn_id: str
    """

    def __init__(self, azure_cosmos_conn_id='azure_cosmos_default'):
        self.conn_id = azure_cosmos_conn_id
        self.connection = self.get_connection(self.conn_id)
        self.extras = self.connection.extra_dejson
        self.azure_database = None
        self.database_collection = None

    def get_conn(self):
        """Return a cosmos db collection."""
        if self.database_collection is not None:
            return self.database_collection

        conn = self.get_connection(self.conn_id)
        service_options = conn.extra_dejson
        self.endpoint_uri = service_options.get('endpoint_uri')
        self.master_key = service_options.get('master_key')
        self.database_name = service_options.get('database_name')
        self.collection_name = service_options.get('collection_name')

        if self.endpoint_uri is None:
            raise AirflowBadRequest("Endpoint URI cannot be None")

        if self.master_key is None:
            raise AirflowBadRequest("Master Key cannot be None")

        if self.database_name is None:
            raise AirflowBadRequest("Database cannot be None")

        if self.collection_name is None:
            raise AirflowBadRequest("Collection name cannot be None")

        # Initialize the Python Azure Cosmos DB client
        self.cosmos_client = document_client.DocumentClient(self.endpoint_uri, {'masterKey': self.master_key})
        # Create the database
        self.azure_database = self.cosmos_client.CreateDatabase({'id': self.database_name})

        # Create collection options
        # todo: Allow these to be configurable?
        options = {
            'offerEnableRUPerMinuteThroughput': True,
            'offerVersion': "V2",
            'offerThroughput': 400
        }

        # Create a collection
        self.database_collection = self.cosmos_client.CreateCollection(
            self.azure_database['_self'],
            {'id': self.collection_name},
            options)

        return self.database_collection

    def insert_document(self, document, document_id=None):
        # Assign unique ID if one isn't provided
        if document_id is None:
            document_id = uuid.uuid4()

        if self.database_collection is None:
            raise AirflowBadRequest("No connection to insert document into.")

        if document is None:
            raise AirflowBadRequest("You cannot insert an None document")

        if document['id'] is None:
            document['id'] = document_id

        created_document = self.cosmos_client.CreateDocument(
            self.database_collection['_self'],
            document)

        return created_document

    def get_documents(self, sql_string):
        if self.database_collection is None:
            raise AirflowBadRequest("No connection to query.")

        if sql_string is None:
            raise AirflowBadRequest("Document ID cannot be None")

        # Query them in SQL
        query = {'query': sql_string}

        # todo: what would be the correct options
        options = {}
        options['enableCrossPartitionQuery'] = True

        result_iterable = self.cosmos_client.QueryDocuments(self.database_collection['_self'], query, options)
        return list(result_iterable)
