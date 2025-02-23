#!/bin/bash

#
# get URL to download latest GTFS feed
#

DATASET_URL="https://otwartedane.metropoliagzm.pl/dataset/317435cc-0075-4d10-b8ef-6e9b0010e90a.jsonld"
RELEASE_URL=$(curl --connect-timeout 30 --cacert ./home-pl.pem -s $DATASET_URL     | \
              jq ' ."@graph"[]."dcat:accessURL"."@id"'                             | \
              grep -F 'schedule_ztm'                                               | \
              sed -e 's/^"//' -e 's/"$//' -e 's/^\(.*\)\(schedule_ztm.*\)$/\2 \1/' | \
              sort -r                                                              | \
              head -1                                                              | \
              sed -e 's/^\(.*\) \(http.*\)$/\2\1/')

if [ -n "$RELEASE_URL" ]
then
    echo $RELEASE_URL
fi
