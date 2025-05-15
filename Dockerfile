FROM python:3.8-slim-buster

RUN adduser --disabled-login appuser

WORKDIR /home/appuser

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY --chown=appuser:appuser scripts/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY --chown=appuser:appuser config/*.json ./containers/datorama_report_automation/config/
COPY --chown=appuser:appuser scripts/*.py ./containers/datorama_report_automation/scripts/

ENV PYTHONPATH "${PYTHONPATH}:${WORKDIR}"
ARG GITHUB_SHA
ENV GITHUB_SHA=$GITHUB_SHA

USER appuser

ENTRYPOINT [ "python", "./containers/datorama_report_automation/scripts/entrypoint.py" ]