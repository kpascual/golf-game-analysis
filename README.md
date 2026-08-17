# Notes

## Why Airflow DAG containing dbt project requires an executable path

dbt and Cosmos do not work well with Python 3.14, so we create a separate Python 3.13 virtual environment that can parse the dbt project
