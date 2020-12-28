#!/bin/sh
set -e

docker build . -t ark-fly-notification
docker tag ark-fly-notification:latest 120400168286.dkr.ecr.us-west-2.amazonaws.com/ark-fly-notification:latest
aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 120400168286.dkr.ecr.us-west-2.amazonaws.com
docker push 120400168286.dkr.ecr.us-west-2.amazonaws.com/ark-fly-notification:latest