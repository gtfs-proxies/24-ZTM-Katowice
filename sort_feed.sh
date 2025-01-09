#!/bin/bash

column=1
for f in stops.txt stops_ext.txt routes_ext.txt routes.txt trips_ext.txt
do
    # sort -k1 -n -t, feed/$f -o feed/$f
    header=$(head -n 1 "feed/$f")
    tail -n +2 "feed/$f" | sort -t, -k"$column","$column"n -o "feed/$f"
    echo $header | cat - "feed/$f" > /tmp/out && mv /tmp/out "feed/$f"
done

column=2
for f in trip_order_ext.txt
do
    # sort -k1 -n -t, feed/$f -o feed/$f
    header=$(head -n 1 "feed/$f")
    tail -n +2 "feed/$f" | sort -t, -k"$column","$column"n -o "feed/$f"
    echo $header | cat - "feed/$f" > /tmp/out && mv /tmp/out "feed/$f"
done

column=3
for f in trips.txt
do
    # sort -k1 -n -t, feed/$f -o feed/$f
    header=$(head -n 1 "feed/$f")
    tail -n +2 "feed/$f" | sort -t, -k"$column","$column"n -o "feed/$f"
    echo $header | cat - "feed/$f" > /tmp/out && mv /tmp/out "feed/$f"
done