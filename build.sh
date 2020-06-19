#!/usr/bin/env bash

set -e

cd "$(dirname "${0}")"

mkdir -p maven-repo

docker run -it --rm --name kafka-connect-rolling-file-build -v "$(pwd):/project" -v "$(pwd)/maven-repo:/root/.m2/repository" -w "/project" maven:3.6-amazoncorretto-8 mvn clean package

BASE_IMAGE_VERSION="5.5.0"
JAR_NAME=$(basename "$(ls target/rolling-file-kafka-connect-*.jar)")

JAR_VERSION=$(echo -n "${JAR_NAME}" | sed -E 's/^rolling-file-kafka-connect-//; s/\.jar$//')

docker build --tag "kafka/connect/rolling-file:${BASE_IMAGE_VERSION}-${JAR_VERSION}" --build-arg BASE_IMAGE_VERSION="${BASE_IMAGE_VERSION}" --build-arg JAR_NAME="${JAR_NAME}" .
