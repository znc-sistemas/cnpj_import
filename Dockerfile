FROM python:3.13-slim AS base

ARG APP_NAME
ARG ENVIRONMENT
ARG USER_ID

ENV APP_NAME=${APP_NAME}
ENV ENVIRONMENT=${ENVIRONMENT}
ENV USER_ID=${USER_ID}

ENV WORK_DIR=/usr/src/app
ENV REQUIREMENTS_DIR=/usr/src/requirements
ENV USERNAME=nectouser

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1


RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql-common \
    libpq-dev \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

COPY ./app /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

WORKDIR /usr/src/app


CMD ["bash"]
