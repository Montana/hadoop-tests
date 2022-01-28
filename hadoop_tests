#!/usr/bin/env python3

from airflow.utils import dates
from airflow.contrib.operators import dataproc_operator
from airflow import models
from airflow.utils.trigger_rule import TriggerRule
from o2a_libs.el_basic_functions import *
from airflow.operators import bash_operator
import datetime
from o2a_libs.el_wf_functions import *
from airflow.operators import dummy_operator
from dateutil.relativedelta import *
from dateutil.easter import *
from dateutil.rrule import *
from dateutil.parser import *
from datetime import *

PARAMS = {
    "user.name": "montana",
    "nameNode": "hdfs://",
    "resourceManager": "localhost:8032",
    "queueName": "default",
    "examplesRoot": "examples",
    "oozie.wf.application.path": "hdfs:///user/mapreduce/examples/mapreduce",
    "outputDir": "output",
    "dataproc_cluster": "oozie-o2a-2cpu",
    "gcp_conn_id": "google_cloud_default",
    "gcp_region": "europe-west3",
    "hadoop_jars": "hdfs:/user/mapreduce/examples/mapreduce/lib/wordcount.jar",
    "hadoop_main_class": "WordCount",
}


def sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):

    with models.DAG(
        "{0}.{1}".format(parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,  # Set to Travis CI's rate limit
        start_date=start_date,  # Change to Travis CI's setdate from dateutils
    ) as dag:

        mr_node_prepare = bash_operator.BashOperator(
            task_id="mr_node_prepare",
            bash_command='$DAGS_FOLDER/../data/prepare.sh -c montana-o2a-2cpu -r europe-west3 -d "/user/mapreduce/examples/mapreduce/output"',
        )

        mr_node = dataproc_operator.DataProcHadoopOperator(
            main_class=PARAMS["hadoop_main_class"],
            arguments=[
                "/user/mapreduce/examples/mapreduce/input",
                "/user/mapreduce/examples/mapreduce/output",
            ],
            files=["hdfs:///user/mapreduce/examples/mapreduce/lib/wordcount.jar"],
            cluster_name=PARAMS["dataproc_cluster"],
            task_id="mr_node",
            trigger_rule="all_success",
            dataproc_hadoop_properties={
                "mapred.job.queue.name": "default",
                "mapreduce.mapper.class": "WordCount$Map",
                "mapreduce.reducer.class": "WordCount$Reduce",
                "mapreduce.combine.class": "WordCount$Reduce",
                "mapreduce.job.output.key.class": "org.apache.hadoop.io.Text",
                "mapreduce.job.output.value.class": "org.apache.hadoop.io.IntWritable",
                "mapred.input.dir": "/user/mapreduce/examples/mapreduce/input",
                "mapred.output.dir": "/user/mapreduce/examples/mapreduce/output",
            },
            dataproc_hadoop_jars=PARAMS["hadoop_jars"],
            gcp_conn_id=PARAMS["gcp_conn_id"],
            region=PARAMS["gcp_region"],
            dataproc_job_id="mr_node",
        )

        mr_node_prepare.set_downstream(mr_node)

    return dag
