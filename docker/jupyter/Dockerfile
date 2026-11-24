FROM apache/spark:4.0.1-python3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/notebook-venv \
    && /opt/notebook-venv/bin/pip install --no-cache-dir jupyterlab==4.5.0 \
    && set -- /opt/spark/python/lib/py4j-*-src.zip \
    && test -f "$1" \
    && ln -s "$1" /opt/spark-py4j.zip

ENV PATH="/opt/notebook-venv/bin:${PATH}"
ENV PYTHONPATH="/opt/spark/python:/opt/spark-py4j.zip"

USER 185