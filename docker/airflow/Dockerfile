FROM spark_notebook

USER root

RUN /usr/bin/python3 -m venv /opt/airflow-venv \
    && /opt/airflow-venv/bin/pip install --no-cache-dir "apache-airflow==3.3.2" \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.10.txt \
    && /opt/notebook-venv/bin/pip install --no-cache-dir "papermill==2.6.0" "pymongo==4.15.5" \
    && /opt/notebook-venv/bin/python -m ipykernel install \
    --prefix /opt/notebook-venv --name spark-python3 --display-name "Spark Python 3" \
    && install -d -m 0755 -o 185 /opt/airflow-state

ENV AIRFLOW_HOME=/opt/airflow-state
ENV PATH="/opt/airflow-venv/bin:${PATH}"

USER 185