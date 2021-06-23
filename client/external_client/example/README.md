# Overview

Example application around Concord Client Pool. Feel free to change and adopt
to meet your needs!

# Build

`docker build -f concord/src/external_client/example/DockerfileClientPoolExample -t client-pool-lib`

## ASAN

Check DockerfileClientPoolExample to enable/disable ASAN. For fast development,
adjust concord/Makefile, `make` and copy the binary into the container.

# Modify docker-compose-tee.yml

If you didn't use `client-pool-lib` as image tag above then you need to adapt
your docker `.env` or modify the client-pool-lib service directly. In addition,
make sure you replace the `entrypoint` with something convenient like
`tail -f /dev/null`.

# Run with TEE deployment

**Note**: We use an exisiting Client Pool configuration which is why we don't
generate one in here. Check the default in the application. Generating our own
would involve more steps/modifications - I'm sure the reader can figure this
out if they need it.

```sh
## Configure TEE environment
$ cd docker
docker $ ./gen-docker-concord-config.sh config-public/dockerConfigurationInput-tee.yaml

## Start local deployment
docker $ docker-compose -f docker-compose-tee.yml up -d

## Run example app
$ docker exec -it docker_client_pool_1 bash
container> ./client-pool-example
```
