# Start from a core stack version
FROM jupyter/pyspark-notebook
RUN pip install boto3
