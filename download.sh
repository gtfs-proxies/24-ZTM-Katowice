#!/bin/bash

# RELEASE_DATE=$(./get-release-date.sh)
RELEASE_URL=$(./get-release-url.sh)
if [ -f curl_options ]; then
    CURL_OPTIONS=$(cat curl_options)
fi
mkdir -p /tmp/$FEED_NAME/original/

for url in $RELEASE_URL; do
    wget -cN -v $CURL_OPTIONS $url --no-check-certificate -P /tmp/$FEED_NAME/original/
done