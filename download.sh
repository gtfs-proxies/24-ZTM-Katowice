#!/bin/bash

# RELEASE_DATE=$(./get-release-date.sh)
RELEASE_URL=$(./get-release-url.sh)
if [ -f curl_options ]; then
    CURL_OPTIONS=$(cat curl_options)
fi
mkdir -p /tmp/$FEED_NAME/original/
wget -cN -v --ca-certificate ./home-pl.pem $CURL_OPTIONS $RELEASE_URL -P /tmp/$FEED_NAME/original/