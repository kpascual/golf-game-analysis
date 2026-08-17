import os
from pathlib import Path
import yaml

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode, InvocationMode

# airflow - <- this marker is used for airflow to identify the DAG

# Configuration variables

DAG_ROOT = Path(__file__).parent
CONFIG_PATH = DAG_ROOT / "config.yml"
DBT_PROJECT_PATH = DAG_ROOT / "dbt_project"


with open(CONFIG_PATH) as f:
    cfg = yaml.load(f, yaml.FullLoader)
    DBT_EXECUTABLE_PATH = cfg['dbt_executable_path']



# DAG definition using dbt project
replicated_dag = DbtDag(
    dag_id='golf_shot_replicated',
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH
    ),
    profile_config=ProfileConfig(
        profile_name='golf',
        target_name='dev',
        profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml"
    ),
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,
        invocation_mode=InvocationMode.SUBPROCESS,
        dbt_executable_path=DBT_EXECUTABLE_PATH
    )
)
