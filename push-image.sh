#!/bin/sh 

if [ "$#" -ne 2 ]; then
   echo "Usage: ./push_image.sh <ACCOUNT_ID> <VERSION_NUMBER>"
   exit 1
fi

ACCOUNT_ID=$1
IMG_NAME=dwh-miner
IMG_VERSION=$2
REGION=eu-central-1

echo aws ecr get-login-password ---region $REGION 
aws --profile saml ecr get-login-password --region $REGION | docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

tag=$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$IMG_NAME:$IMG_VERSION
echo "Building version: " $tag
docker build -t $tag .
docker push $tag

