from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode
from cosmos.profiles import ClickhouseUserPasswordProfileMapping

project_config = ProjectConfig(
    dbt_project_path="/opt/dbt/dbt_projects/create_candles_1m",
)

profile_config = ProfileConfig(
    profile_name="create_candles_clickhouse",
    target_name="dev",
    profile_mapping=ClickhouseUserPasswordProfileMapping(
        conn_id="clickhouse_create_candles",
        profile_args={"secure": False},
    ),
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path="/home/airflow/dbt_venv/bin/dbt",
)

cosmos_dag = DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    operator_args={
        "install_deps": True,
    },
    dag_id="dbt_clickhouse_candles",
    # schedule=None,
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
