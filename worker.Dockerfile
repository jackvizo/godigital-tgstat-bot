# Use an official Python runtime as a parent image
FROM python:3.10-slim-buster AS base

FROM base AS deps

# Set the working directory in the container to /app
WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/* 

FROM deps AS builder
WORKDIR /app
# Install any needed packages specified in requirements.txt
COPY requirements.txt .
COPY id_rsa_immerse .
COPY clouds.yaml .
RUN pip install --no-cache-dir -r requirements.txt

FROM builder AS runner
WORKDIR /app
COPY . .

ARG TMP_FOLDER

RUN addgroup --system --gid 1001 godigital
RUN adduser --system --uid 1001 godigital

USER root
RUN mkdir -p /prefect-tmp
RUN chown -R godigital:godigital /prefect-tmp

USER godigital