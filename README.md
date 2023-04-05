`This project is a pipeline created as the final project for Data Engineering Zoomcamp 2023`

# Airline on-time performance

Have you ever been stuck in an airport because your flight was delayed or cancelled and wondered if you could have predicted it if you'd had more data?

**The data**: The data consists of flight arrival and departure details for all commercial flights within the USA, from October 1987 to April 2008. This is a large dataset: there are nearly 120 million records in total, and takes up 1.6 gigabytes of space compressed and 12 gigabytes when uncompressed.

The data can be accessed on the [harvard dataverse website](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/HG7NV7)

---

## Tutorials

The tutorials on how to setup and run this project can be found [here](tutorials.md)

## Workflow

This project uses Prefect for workflow orchestration, Docker for containerization, BigQuery as a data warehouse, dbt for data transformation, Looker for data visualization and business intelligence, and Google Cloud Bucket for cloud storage.

- **Prefect**: This is a workflow orchestration tool that is used to manage and automate the execution of tasks. In this project, Prefect is used to manage scripts for downloading and uploading data, as well as creating tables in BigQuery. These scripts can be found in the `main/` folder.

- **Docker**: This is a platform for developing, shipping, and running applications in containers. Containers provide a lightweight and portable way to package and run applications.

- **BigQuery**: This is a cloud-based data warehouse provided by Google Cloud Platform. It is used to store and analyze large amounts of data.

- **dbt**: This stands for “data build tool” and is used for transforming data in BigQuery. It allows users to define data transformations using SQL and manage dependencies between different transformations.

- **Looker**: This is a data visualization and business intelligence tool that allows users to explore and analyze data.

- **Google Cloud Bucket**: This is a cloud storage service provided by Google Cloud Platform. It is used to store and retrieve data.

![Pipeline](/.images/Workflow.png)

---

## Dashboard

For a clearer version of the dashboard, you can check out google looker studio [here](https://lookerstudio.google.com/reporting/5370a0a2-b22f-41cf-84ed-2a52da71581a)

![dashboard](/.images/Data_Expo_2009__Airline_on_time_Data_Visualization.jpg)
