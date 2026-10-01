# PySpark + JupyterLab, everything inside the container.
# The base image bundles a matched Spark + Java (JDK) + Python + Jupyter, so we
# don't manage Java/Spark versions by hand.
# Pinned by digest (not :latest) so builds don't chase the moving `latest` tag and
# re-download ~7 GB whenever Jupyter publishes a new image. Bump this digest
# deliberately when you want a newer Spark/Jupyter.
FROM quay.io/jupyter/pyspark-notebook@sha256:ec67d7df5aefbdf7e3b7f7999949c072e0274980c2c41bc08a17edf7a10c0758

# Extra Python libs for local analysis + plotting.
# (pyspark, pandas, matplotlib and pyarrow already ship in the base image.)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
