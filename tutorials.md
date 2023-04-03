# Tutorials on how to setup and run this project

## Table of Content

=================

- ### [Creating a Dev Container](#creating-a-dev-container)

For this project, to ensure minimal compactibility issues, I used a Dev Container in VSCode to create a developement environment with the help of Docker.

### Creating a Dev Container

To create a dev container,

1. Install Visual Studio Code and Docker on your machine, if you haven't already
2. Create a new folder for your project and open it in Visual Studio Code.
3. Create a new file named "Dockerfile" in the root directory of your project.
4. Open the Dockerfile in Visual Studio Code and add the following lines:

```bash
FROM python:3.10-slim-buster

# Install Jupyter
RUN pip install jupyter

# Install Prefect
RUN pip install prefect

# Install Git
RUN apt-get update && \
    apt-get install -y git

# Install PySpark
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless && \
    pip install pyspark

# Install dbt-bigquery
RUN pip install dbt-bigquery

# Install Piperider
RUN pip install piperider

# Install Google Cloud
RUN pip install google-cloud-storage
RUN pip install google-cloud-bigquery

# Install Pandas
RUN pip install pandas

# Set environment variables for PySpark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS='lab --ip=0.0.0.0 --port=8888 --no-browser'

# Set working directory
WORKDIR /app

# Create output directory
RUN mkdir /app/output

# Set volume for output directory
VOLUME /app/output

# Expose port for Jupyter
EXPOSE 8888

# Start PySpark
CMD ["pyspark"]

```

5. Install the Dev Containers extension by Microsoft on VS Code. A green `><` symbol should appear at the bottom left side of VSCode.
6. Click the green icon and select `Remote-Containers: Open Folder in Container`
7. In the .devcontainer created, there's a `devcontainer.json` file. Adjust the `dockerfile` key to the value of "Dockerfile".
8. Add any extensions needed under extensions
