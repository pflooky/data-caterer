#!/usr/bin/env bash
version=0.1
platform=linux/amd64  #linux/amd64,linux/arm64

#gradle clean shadowJar
docker build --platform $platform --no-cache -t pflookyy/spartagen:$version .
docker push pflookyy/spartagen:$version
