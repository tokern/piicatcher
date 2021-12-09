#! /usr/bin/env bash

set -e

PROJECT_ROOT=$(dirname $(dirname $0))

echo "$PROJECT_ROOT"

DOCKERHUB_NAMESPACE=tokern


TAG=$1
if [ -z $TAG ]; then
    echo "usage: $0 <release-name> [--publish] [--latest]"
    exit 1
fi

if [ "$2" == "--publish" ]; then
    PUBLISH="YES"
fi

if [ "$3" == "--latest" ]; then
    LATEST="YES"
fi

if [ "$PUBLISH" == "YES" ] && [ -z "$DOCKERHUB_USERNAME" -o -z "$DOCKERHUB_PASSWORD" ]; then
    echo "In order to publish an image to Dockerhub you must set \$DOCKERHUB_USERNAME and \$DOCKERHUB_PASSWORD before running."
    exit 1
fi

DOCKERHUB_REPOSITORY=piicatcher
DOCKER_IMAGE="${DOCKERHUB_NAMESPACE}/${DOCKERHUB_REPOSITORY}:${TAG}"

echo "Building Docker image ${DOCKER_IMAGE} from official Tokern release ${TAG}"

# now tell docker to build our image

docker build -t "${DOCKER_IMAGE}" -f "$PROJECT_ROOT"/docker/Dockerfile "${PROJECT_ROOT}"

if [ "$PUBLISH" == "YES" ]; then
    echo "Publishing image ${DOCKER_IMAGE} to Dockerhub"

    # make sure that we are logged into dockerhub
    docker login --username="${DOCKERHUB_USERNAME}" --password="${DOCKERHUB_PASSWORD}"

    # push the built image to dockerhub
    docker push "${DOCKER_IMAGE}"

    # TODO: quick check against dockerhub to see that our new image made it

    if [ "$LATEST" == "YES" ]; then
        # tag our recent versioned image as "latest"
        docker tag "${DOCKER_IMAGE}" ${DOCKERHUB_NAMESPACE}/${DOCKERHUB_REPOSITORY}:latest

        # then push it as well
        docker push ${DOCKERHUB_NAMESPACE}/${DOCKERHUB_REPOSITORY}:latest

        # TODO: validate push succeeded
    fi
fi

echo "Done"