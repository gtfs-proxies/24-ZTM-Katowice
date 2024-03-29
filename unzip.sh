#!/bin/bash

FEED_FILE=$(ls /tmp/$FEED_NAME/original/)

unzip $FEED_FILE -d feed/
