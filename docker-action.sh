#!/usr/bin/env bash

version=$(grep version gradle.properties | cut -d= -f2)
sparkVersion=$(grep spark gradle.properties | cut -d= -f2)
platforms="linux/amd64,linux/arm64"

echo "Creating API jar and uploading to Github packages"
gradle clean :api:build :api:publish

echo "Creating data caterer basic jar, version=$version"
gradle clean -PapplicationType=basic build basicJar -x shadowJar
echo "Creating advanced jar"
gradle -PapplicationType=advanced advancedJar -x shadowJar

docker run --privileged --rm tonistiigi/binfmt --install all
docker buildx create --use --name builder
docker buildx inspect --bootstrap builder

docker buildx build --platform $platforms \
  --build-arg "APP_VERSION=$version" \
  --build-arg "SPARK_VERSION=$sparkVersion" \
  -t datacatering/data-caterer-basic:$version --push .

docker buildx build --platform $platforms \
  --build-arg "APP_VERSION=$version" \
  --build-arg "SPARK_VERSION=$sparkVersion" \
  -t datacatering/data-caterer:$version --push -f Dockerfile_advanced .
