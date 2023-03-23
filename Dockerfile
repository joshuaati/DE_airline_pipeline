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

# Expose port for Jupyter
EXPOSE 8888

# Set working directory
WORKDIR /app

# Start PySpark
CMD ["pyspark"]
