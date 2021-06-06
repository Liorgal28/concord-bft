# Client side overview
To use the daml-ledger API, we need to build three images before we build the daml-ledger-api image.

The first image is called 'external-client-lib' and it represents the client pool which through it we send requests from the ledger API to the replicas.

The second image is called 'trc-lib' and it represents the thin-replica client.

The third image is called 'participant-lib' it takes both images and combined them into one, daml-ledger-api image is using this image.

# Build & Test

Be sure that you have built concord to use it for generating configurations and running docker-compose.
```bash
# Enter the concord root of the repo
cd ~/vmwathena_blockchain/concord

# Ensure you have the latest code built outside the concord image for fast rebuilds
make

# Build the image
cd .. # Root vmwathena_blockchain directory
docker build -f concord/Dockerfile -t concord-core:latest .

# Build client side images
cd concord/src/external_client
./build_all_client_images.sh

```

Everything below this line starts from the root docker directory. Replace with the location of your repo.

`cd ~/vmwathena_blockchain/docker`

## Setup your docker environment

Use the remote images for everything that matches your build.
`./make-prebuilt-env.sh > temp.env && mv temp.env .env`

Edit .env file:

Change concord_repo to be concord-core and tag to latest.

Change daml_ledger_api_repo to be daml-ledger-api and tag to latest.
```
concord_repo=concord-core
concord_tag=latest
daml_ledger_api_repo=daml-ledger-api
daml_ledger_api_tag=latest
```

## Create participant node configs
```
./gen-docker-client-config.sh config-public/dockerClientConfigInput.yaml
```

## Create configuration files

Generate the corresponding configuration files with
in `config-public/dockerConfigurationInput-daml-nano.yaml` comment out `key_view_path: /tmp`
Then, type the following:
```
./gen-docker-concord-config.sh config-public/dockerConfigurationInput-daml-nano.yaml
```
# Start cluster

This starts 4 concord containers and their corresponding execution engines.

`docker-compose -f docker-compose-daml-nano.yml up`