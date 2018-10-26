# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

from azure.common.client_factory import get_client_from_auth_file
from azure.common.credentials import ServicePrincipalCredentials

from azure.batch import BatchServiceClient

class AzureBatchAIHook(BaseHook):

    def __init__(self, azure_batchai_conn_id='azure_batchai_default'):
        self.conn_id = azure_batchai_conn_id
        self.connection = self.get_conn()

    def get_conn(self):
        conn = self.get_connection(self.conn_id)
        key_path = conn.extra_dejson.get('key_path', False)
        if key_path:
            if key_path.endswith('.json'):
                self.log.info('Getting connection using a JSON key file.')
                return get_client_from_auth_file(BatchServiceClient,
                                                 key_path)
            else:
                raise AirflowException('Unrecognised extension for key file.')

        if os.environ.get('AZURE_AUTH_LOCATION'):
            key_path = os.environ.get('AZURE_AUTH_LOCATION')
            if key_path.endswith('.json'):
                self.log.info('Getting connection using a JSON key file.')
                return get_client_from_auth_file(BatchServiceClient,
                                                 key_path)
            else:
                raise AirflowException('Unrecognised extension for key file.')
        
        credentials = ServicePrincipalCredentials(
            client_id=conn.login,
            secret=conn.password,
            tenant=conn.extra_dejson['tenantId']
        )
        subscription_id = conn.extra_dejson['subscriptionId']
        return BatchServiceClient(credentials, str(subscription_id))

    def create(self, resource_group, workspace_name, cluster_name, parameters):
        self.connection.clusters.create(resource_group,
                                          workspace_name,
                                          cluster_name,
                                          parameters)
                                                        
    def update(self, resource_group, workspace_name, cluster_name):
        self.connection.clusters.update(resource_group,
                                          workspace_name,
                                          cluster_name)

    def get_state_exitcode(self, resource_group, workspace_name, cluster_name):
        response = self.connection.clusters.get(resource_group,
                                                        workspace_name,
                                                        cluster_name,
                                                        raw=True).response.json()
        cluster = response['properties']['cluster']      # TODO: check to see if 'cluster' is correct
        instance_view = cluster[0]['properties'].get('instanceView', {})
        current_state = instance_view.get('currentState', {})
        return current_state.get('state'), current_state.get('exitCode', 0)

    def get_messages(self, resource_group, workspace_name, cluster_name):
        response = self.connection.clusters.get(resource_group,
                                                    workspace_name,
                                                    cluster_name,
                                                    raw=True).response.json()
        cluster = response['properties']['cluster']      # TODO: check to see if 'cluster' is correct
        instance_view = cluster[0]['properties'].get('instanceView', {})
        return [event['message'] for event in instance_view.get('events', [])]

    # TODO: figure out how to get logs
    # def get_logs(self, resource_group, name, tail=1000):
    #     logs = self.connection.container_logs.list(resource_group, name, name, tail=tail)
    #     return logs.content.splitlines(True)

    def delete(self, resource_group, workspace_name, cluster_name):
        self.connection.clusters.delete(resource_group, workspace_name, cluster_name)