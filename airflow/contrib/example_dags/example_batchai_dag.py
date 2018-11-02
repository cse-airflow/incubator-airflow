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

import airflow
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.azure_batchai_operator import AzureBatchAIOperator
from airflow.models import DAG
import random


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='example_batchai_dag',
    default_args=args,
    schedule_interval="@daily")

batch_ai_node = AzureBatchAIOperator(
    'azure_batchai_default',
    'batch-ai-test-rg',
    'batch-ai-workspace-name',
    'batch-ai-cluster-name',
    'WestUS2',
    environment_variables={},
    volumes=[],
    task_id='run_this_first',
    dag=dag)

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=lambda: random.choice(options),
    dag=dag)
branching.set_upstream(batch_ai_node)

join = DummyOperator(
    task_id='join',
    trigger_rule='one_success',
    dag=dag
)
