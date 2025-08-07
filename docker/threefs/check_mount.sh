#!/bin/bash

MountPath="$1"
count=0
while ! mount | grep $MountPath | grep "hf3fs.stage"
do
    sleep 2
    count=$((count + 1))
    if test $count -eq 15
    then
        echo "timed out!"
        exit 1
    fi
done
echo "$(date "+%Y-%m-%d %H:%M:%S")"
echo "succeed in checking mount point $MountPath"