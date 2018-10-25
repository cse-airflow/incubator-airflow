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

from time import sleep

from airflow.contrib.hooks.azure_batchai_hook import (AzureBatchAIHook)
                                                        # AzureContainerRegistryHook,
                                                        # AzureContainerVolumeHook)
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.models import BaseOperator

from azure.mgmt.containerinstance.models import (EnvironmentVariable,
                                                 VolumeMount,
                                                 ResourceRequests,
                                                 ResourceRequirements,
                                                 Container,
                                                 ContainerGroup)
from msrestazure.azure_exceptions import CloudError

class AzureBatchAIOperator(BaseOperator):
    """
    Start a cluster on Azure Batch AI
    :param bai_conn_id: connection id of a service principal which will be used
        to start the batch ai cluster
    :type bai_conn_id: str
    :param registry_conn_id: connection id of a user which can login to a
        private docker registry. If None, we assume a public registry
    :type registry_conn_id: str
    :param resource_group: name of the resource group wherein this container
        instance should be started
    :type resource_group: str
    :param name: name of the batch ai cluster
    :type name: str
    :param image: the docker image to be used
    :type image: str
    :param region: the region wherein this container instance should be started
    :type region: str
    :param: environment_variables: key,value pairs containing environment variables
        which will be passed to the running container
    :type: environment_variables: dict
    :param: volumes: list of volumes to be mounted to the container.
        Currently only Azure Fileshares are supported.
    :type: volumes: list[<conn_id, account_name, share_name, mount_path, read_only>]
    :param: memory_in_gb: the amount of memory to allocate to this container
    :type: memory_in_gb: double
    :param: cpu: the number of cpus to allocate to this container
    :type: cpu: double
     :Example:
     >>>  a = AzureContainerInstancesOperator(
                'azure_service_principal',
                'azure_registry_user',
                'my-resource-group',
                'my-container-name-{{ ds }}',
                'myprivateregistry.azurecr.io/my_container:latest',
                'westeurope',
                {'EXECUTION_DATE': '{{ ds }}'},
                [('azure_wasb_conn_id',
                  'my_storage_container',
                  'my_fileshare',
                  '/input-data',
                  True),],
                memory_in_gb=14.0,
                cpu=4.0,
                task_id='start_container'
            )
    """

    template_fields = ('name', 'environment_variables')
    template_ext = tuple()
    def __init__(self, bai_conn_id, registry_conn_id, resource_group, name, image, region,
                environment_variables={}, volumes=[], memory_in_gb=2.0, cpu=1.0,
                *args, **kwargs):
        self.bai_conn_id = bai_conn_id
        self.registry_conn_id = registry_conn_id
        self.resource_group = resource_group
        self.name = name
        self.image = image
        self.region = region
        self.environment_variables = environment_variables
        self.volumes = volumes
        self.memory_in_gb = memory_in_gb
        self.cpu = cpu
        super(AzureBatchAIOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        batch_ai_hook = AzureBatchAIHook(self.ci_conn_id)