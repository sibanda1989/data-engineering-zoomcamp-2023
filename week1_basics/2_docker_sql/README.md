# Docker

Docker is a containerization technique to isolate and ensure consistency in development environments.

## Dockerfile

A Dockerfile is used to build a Docker image, which is then used to run a container. A Docker image is the base from which a container runs.

## Docker Compose
a docker-compose.yaml file is especially useful when running multiple containers that are working together. These will be working together in the same docker docker network. The network is created automatically.
Docker compose is useful in eliminating the long configs that are involved when doing a `docker-run` command. These config are listed in the *docker-compose* file and executed with a simple `docker-compose up` command.

## Data Ingestion
the ingest_data.py script is uploading data in csv files into the Postgresql database.