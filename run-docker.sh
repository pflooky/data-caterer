#!/bin/bash
echo "======================"
echo "Try to run basic image"
echo "======================"
docker run -e ENABLE_RECORD_TRACKING=true -v /tmp/datagen:/opt/app/data-caterer datacatering/data-caterer-basic:0.1
