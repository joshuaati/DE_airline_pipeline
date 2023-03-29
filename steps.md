## Creating a docker environment

create the dockerfile
create a vscode dev container with the dockerfile
Install extensions in the dev container

- python
- jupyter
- jupyter notebook renderer
- jinja
- prettier

## Lunch Prefect

prefect orion start
prefect deployment build main/etl_web_gcp.py:main_etl -n "web_to_bucket"
add the parameters in the `main_etl-deployment.yaml` file created
prefect deployment apply main_etl-deployment.yaml
prefect agent start -q 'default'

prefect deployment build main/gcp_bq.py:create_table -n "big_query_table_create"

jupyter notebook --allow-root

SELECT column_name, data_type
FROM `airlinepipeline.airline_on_time`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
WHERE table_name = '2008'

## Running DBT

dbt init
dbt debug
Create sql staging and core models
if you want to create packages, create the packages.yml file and define the package and run - dbt deps
