#!/usr/bin/env bash

gradle clean shadowJar -Dorg.gradle.java.home=/Users/peter/Library/Java/JavaVirtualMachines/openjdk-17.0.2/Contents/Home

#run
#/Users/peter/Library/Java/JavaVirtualMachines/openjdk-17.0.2/Contents/Home/bin/java -jar build/libs/trial-0.0.1-SNAPSHOT-all.jar
