#!/bin/bash

#
# get URL to download latest GTFS feed
#

# Extended dataset, GTFS feeds are exclusive e.g. for one day, can be user after merging.
# DATASET_URL="https://otwartedane.metropoliagzm.pl/dataset/317435cc-0075-4d10-b8ef-6e9b0010e90a.jsonld"

# Simple dataset, can be used as is
DATASET_URL="https://otwartedane.metropoliagzm.pl/dataset/5d8d7145-1be1-4ed2-9c18-5535e056a56d.jsonld"

RELEASE_URL=$(curl --connect-timeout 30 -sk $DATASET_URL                            | \
              jq ' ."@graph"[]."dcat:accessURL"."@id"'                              | \
              grep -F 'schedule_'                                                   | \
              sed -e 's/^"//' -e 's/"$//' -e 's/^\(.*\)\(schedule_.*\)$/\2 \1/'     | \
              sort -r                                                               | \
              head -1                                                               | \
              sed -e 's/^\(.*\) \(http.*\)$/\2\1/')

if [ -n "$RELEASE_URL" ]
then
    echo $RELEASE_URL
fi
