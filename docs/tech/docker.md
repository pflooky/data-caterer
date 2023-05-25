# Docker
## Quick start
`docker run spartagen:latest -v /tmp/datagen:/tmp`

## Run with custom data connections
1. Use sample `application.conf` from [here](../../app/src/main/resources/application.conf)
2. Fill in details of data connections
3. `docker run spartagen:latest -v /tmp/datagen/tmp -e CONFIG_PATH=`
