# Docker

## Quick start

1. `mkdir /tmp/datagen`
2. `docker run -v /tmp/datagen:/opt/app/spartagen pflookyy/spartagen:0.1`
3. `head /tmp/datagen/sample/json/account-gen/part-0000*`

## Run with custom data connections

1. Use sample `application.conf` from [here](../../app/src/main/resources/application.conf) and put under folder `/tmp/datagen`
   1. `cp app/src/main/resources/application.conf /tmp/datagen`
2. Fill in details of data connections as found [here](connections.md)
3. `docker run -v /tmp/datagen:/opt/app/spartagen -e APPLICATION_CONFIG_PATH=/opt/app/datagen/application.conf pflookyy/spartagen:0.1`
