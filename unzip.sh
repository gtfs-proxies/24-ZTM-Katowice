#!/bin/bash

FILE_LOCATION=/tmp/$FEED_NAME/original
FEED_FILE=$(ls $FILE_LOCATION | sort -r | head -1)

unzip -u $FILE_LOCATION/$FEED_FILE -d feed/
