"""CareFusion360 orchestration with Amazon MWAA, EMR, and Spark."""

from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor


AWS_CONN_ID = "aws_default"
AWS_REGION = "ap-south-1"
DATA_BUCKET = "carefusion360-data-lake"
CODE_BUCKET = "carefusion360-pipeline-code"
DEPENDENCIES_URI = f"s3://{CODE_BUCKET}/dependencies/dependencies.zip"

JOB_URIS = {
    "job1": f"s3://{CODE_BUCKET}/jobs/job1_member_ingestion.py",
    "job2": f"s3://{CODE_BUCKET}/jobs/job2_claims_policies.py",
    "job3": f"s3://{CODE_BUCKET}/jobs/job3_api_events.py",
    "job4": f"s3://{CODE_BUCKET}/jobs/job4_member360.py",
}

JOB_FLOW_OVERRIDES = {
    "Name": "CareFusion360-{{ ds_nodash }}",
    "ReleaseLabel": "{{ var.value.carefusion360_emr_release_label }}",
    "Applications": [{"Name": "Spark"}],
    "LogUri": f"s3://{DATA_BUCKET}/logs/emr/",
    "StepConcurrencyLevel": 3,
    "Instances": {
        "Ec2SubnetId": "{{ var.value.carefusion360_emr_subnet_id }}",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        "InstanceGroups": [
            {
                "Name": "Primary",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
    },
    "JobFlowRole": "{{ var.value.carefusion360_emr_job_flow_role }}",
    "ServiceRole": "{{ var.value.carefusion360_emr_service_role }}",
}


def spark_step(name: str, script_uri: str) -> dict:
    """Build an EMR Spark step."""
    return {
        "Name": name,
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "--py-files",
                DEPENDENCIES_URI,
                script_uri,
            ],
        },
    }


def step_id_xcom(task_id: str) -> str:
    """Return a Jinja expression for the first step ID returned by an EMR step submission."""
    return (
        "{{ ti.xcom_pull(task_ids='"
        + task_id
        + "', key='return_value')[0] }}"
    )


def add_step_task(task_id: str, step_name: str, job_key: str) -> EmrAddStepsOperator:
    """Create one asynchronous EMR step submission task."""
    return EmrAddStepsOperator(
        task_id=task_id,
        job_flow_id=create_emr_cluster.output,
        steps=[spark_step(step_name, JOB_URIS[job_key])],
        wait_for_completion=False,
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        do_xcom_push=True,
    )


def wait_step_task(task_id: str, submit_task_id: str) -> EmrStepSensor:
    """Create a sensor for a previously submitted EMR step."""
    return EmrStepSensor(
        task_id=task_id,
        job_flow_id=create_emr_cluster.output,
        step_id=step_id_xcom(submit_task_id),
        target_states=["COMPLETED"],
        failed_states=["CANCELLED", "FAILED", "INTERRUPTED"],
        max_attempts=300,
        poke_interval=60,
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
    )


default_args = {
    "owner": "carefusion360",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="carefusion360_pipeline_cloud",
    description="CareFusion360 healthcare pipeline using MWAA, EMR, and PySpark",
    schedule="20 20 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    default_args=default_args,
    tags=["carefusion360", "healthcare", "pyspark", "emr", "aws"],
) as dag:

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )

    wait_for_emr_cluster = EmrJobFlowSensor(
        task_id="wait_for_emr_cluster",
        job_flow_id=create_emr_cluster.output,
        target_states=["RUNNING", "WAITING"],
        failed_states=["TERMINATED", "TERMINATED_WITH_ERRORS"],
        max_attempts=60,
        poke_interval=60,
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
    )

    submit_job1 = add_step_task("submit_job1", "Job1-Member-Ingestion", "job1")
    submit_job2 = add_step_task("submit_job2", "Job2-Claims-Policies", "job2")
    submit_job3 = add_step_task("submit_job3", "Job3-WebAPI-Events", "job3")

    wait_job1 = wait_step_task("wait_job1", "submit_job1")
    wait_job2 = wait_step_task("wait_job2", "submit_job2")
    wait_job3 = wait_step_task("wait_job3", "submit_job3")

    submit_job4 = EmrAddStepsOperator(
        task_id="submit_job4",
        job_flow_id=create_emr_cluster.output,
        steps=[spark_step("Job4-Member360-Gold", JOB_URIS["job4"])],
        wait_for_completion=False,
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        do_xcom_push=True,
    )

    wait_job4 = EmrStepSensor(
        task_id="wait_job4",
        job_flow_id=create_emr_cluster.output,
        step_id=step_id_xcom("submit_job4"),
        target_states=["COMPLETED"],
        failed_states=["CANCELLED", "FAILED", "INTERRUPTED"],
        max_attempts=300,
        poke_interval=60,
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id=create_emr_cluster.output,
        trigger_rule=TriggerRule.ALL_DONE,
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
    )

    create_emr_cluster >> wait_for_emr_cluster
    wait_for_emr_cluster >> [submit_job1, submit_job2, submit_job3]
    submit_job1 >> wait_job1
    submit_job2 >> wait_job2
    submit_job3 >> wait_job3
    [wait_job1, wait_job2, wait_job3] >> submit_job4 >> wait_job4
    [wait_job1, wait_job2, wait_job3, wait_job4] >> terminate_emr_cluster
