#!/usr/bin/env bash
version=$(grep version gradle.properties | cut -d= -f2)
sparkVersion=$(grep spark gradle.properties | cut -d= -f2)
platform=linux/arm64  #linux/amd64,linux/arm64

echo "Creating data caterer basic jar, version=$version"
gradle clean -PapplicationType=basic build basicJar -x shadowJar

echo "Creating basic docker image"
docker build --platform "$platform" \
  --build-arg "APP_VERSION=$version" \
  --build-arg "SPARK_VERSION=$sparkVersion" \
  -t datacatering/data-caterer-basic:$version .
  #--no-cache
docker --config ~/.data-caterer push datacatering/data-caterer-basic:"$version"

echo "Creating advanced jar"
gradle -PapplicationType=advanced advancedJar -x shadowJar

echo "Creating advanced docker image"
docker build --platform "$platform" \
  --build-arg "APP_VERSION=$version" \
  --build-arg "SPARK_VERSION=$sparkVersion" \
  -t datacatering/data-caterer:$version -f Dockerfile_advanced .
  #--no-cache
docker --config ~/.data-caterer push datacatering/data-caterer:"$version"
