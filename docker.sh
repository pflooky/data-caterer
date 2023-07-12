#!/usr/bin/env bash
version=$(grep version gradle.properties | cut -d= -f2)
platform=linux/arm64  #linux/amd64,linux/arm64

echo "Creating data caterer basic and advanced jar, version=$version"
gradle clean build basicJar advancedJar -x shadowJar

echo "Creating basic docker image"
docker build --platform "$platform" \
  --build-arg "APP_VERSION=$version" \
  -t pflookyy/data-caterer-basic:$version .
  #--no-cache
#docker push pflookyy/data-caterer-basic:"$version"

echo "Creating advanced docker image"
docker build --platform "$platform" \
  --build-arg "APP_VERSION=$version" \
  -t pflookyy/data-caterer:$version -f Dockerfile_advanced .
  #--no-cache
#docker push pflookyy/data-caterer:"$version"
