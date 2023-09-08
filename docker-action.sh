#!/usr/bin/env bash

version=$(grep version gradle.properties | cut -d= -f2)
sparkVersion=$(grep spark gradle.properties | cut -d= -f2)
platforms="linux/amd64,linux/arm64"

echo "Creating API jars and uploading to Github packages"
gradle clean :api:shadowJar :api:publish :api-java:shadowJar :api-java:publish
publish_res=$?
if [[ "$publish_res" -ne 0 ]] ; then
  echo "Publish Scala or Java API jar failed, exiting"
  exit 1
fi

echo "Creating data caterer basic jar, version=$version"
gradle -PapplicationType=basic build basicJar -x shadowJar
build_app=$?
if [[ "$build_app" -ne 0 ]] ; then
  echo "Failed to build app, exiting"
  exit 1
fi

echo "Creating advanced jar"
gradle -PapplicationType=advanced advancedJar -x shadowJar

docker run --privileged --rm tonistiigi/binfmt --install all
docker buildx create --use --name builder
docker buildx inspect --bootstrap builder

docker buildx build --platform $platforms \
  --build-arg "APP_TYPE=basic" \
  --build-arg "APP_VERSION=$version" \
  --build-arg "SPARK_VERSION=$sparkVersion" \
  -t datacatering/data-caterer-basic:$version --push .

docker buildx build --platform $platforms \
  --build-arg "APP_TYPE=advanced" \
  --build-arg "APP_VERSION=$version" \
  --build-arg "SPARK_VERSION=$sparkVersion" \
  -t datacatering/data-caterer:$version --push .
