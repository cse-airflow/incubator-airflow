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
import azure.cosmos.cosmos_client as cosmos_client
import uuid

from airflow.exceptions import AirflowBadRequest
from airflow.hooks.base_hook import BaseHook


class AzureCosmosDBHook(BaseHook):
    """
    Interacts with Azure CosmosDB.

    login should be the endpoint uri, password should be the master key
    {"database_name": "<DATABASE_NAME>", "collection_name": "COLLECTION_NAME"}.

    :param azure_cosmos_conn_id: Reference to the Azure CosmosDB connection.
    :type azure_cosmos_conn_id: str
    """

    def __init__(self, azure_cosmos_conn_id='azure_cosmos_default'):
        self.conn_id = azure_cosmos_conn_id
        self.connection = self.get_connection(self.conn_id)
        self.extras = self.connection.extra_dejson

        self.endpoint_uri = self.connection.login
        self.master_key = self.connection.password
        self.database_name = self.extras.get('database_name')
        self.collection_name = self.extras.get('collection_name')

        if self.database_name is None:
            raise AirflowBadRequest("Database cannot be None")

        if self.collection_name is None:
            raise AirflowBadRequest("Collection name cannot be None")

        self.cosmos_client = None

    def get_conn(self):
        """Return a cosmos db client."""
        if self.cosmos_client is not None:
            return self.cosmos_client

        # Initialize the Python Azure Cosmos DB client
        self.cosmos_client = cosmos_client.CosmosClient(self.endpoint_uri, {'masterKey': self.master_key})

        return self.cosmos_client

    def insert_document(self, document, document_id=None):
        # Assign unique ID if one isn't provided
        if document_id is None:
            document_id = uuid.uuid4()

        if self.collection_name is None:
            raise AirflowBadRequest("No connection to insert document into.")

        if document is None:
            raise AirflowBadRequest("You cannot insert a None document")

        if document['id'] is None:
            document['id'] = document_id

        created_document = self.cosmos_client.CreateItem(
            GetCollectionLink(self.database_name, self.collection_name),
            document)

        return created_document

    def insert_documents(self, documents, document_id=None):
        if self.collection_name is None:
            raise AirflowBadRequest("No connection to insert document into.")

        if documents is None:
            raise AirflowBadRequest("You cannot insert empty documents")

        created_documents = []
        for single_document in documents:
            created_documents.append(
                self.cosmos_client.CreateItem(
                    GetCollectionLink(self.database_name, self.collection_name),
                    single_document))

        return created_documents

    def delete_document(self, document_id):
        if document_id is None:
            raise AirflowBadRequest("Cannot delete a document without an id")

        self.cosmos_client.DeleteItem(
            GetDocumentLink(self.database_name, self.collection_name, document_id))

    def get_document(self, document_id):
        if document_id is None:
            raise AirflowBadRequest("Cannot get a document without an id")

        returned_document = self.cosmos_client.ReadItem(
            GetDocumentLink(self.database_name, self.collection_name, document_id))

        return returned_document

    def get_documents(self, sql_string, partition_key=None):
        if self.collection_name is None:
            raise AirflowBadRequest("No connection to query.")

        if sql_string is None:
            raise AirflowBadRequest("SQL query string cannot be None")

        # Query them in SQL
        query = {'query': sql_string}

        result_iterable = self.cosmos_client.QueryItems(
            GetCollectionLink(self.database_name, self.collection_name),
            query,
            partition_key)

        return list(result_iterable)


def GetDatabaseLink(database_id):
    return "dbs" + "/" + database_id


def GetCollectionLink(database_id, collection_id):
    return GetDatabaseLink(database_id) + "/" + "colls" + "/" + collection_id


def GetDocumentLink(database_id, collection_id, document_id):
    return GetCollectionLink(database_id, collection_id) + "/" + "docs" + "/" + document_id
