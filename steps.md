## Creating a docker environment

## Lunch Prefect

prefect orion start
prefect deployment build main/etl_web_gcp.py:main_etl -n "web_to_bucket"
