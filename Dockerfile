# PySpark + JupyterLab, everything inside the container.
# The base image bundles a matched Spark + Java (JDK) + Python + Jupyter, so we
# don't manage Java/Spark versions by hand.
FROM quay.io/jupyter/pyspark-notebook:latest

# Extra Python libs for local analysis + plotting.
# (pyspark, pandas, matplotlib and pyarrow already ship in the base image.)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
