#!/bin/bash

# 定义变量
MAX_RETRIES=12
TIMEOUT=5
MountPath="$1"
COMMAND="umount -f $MountPath"
retry_count=0

while [ $retry_count -lt $MAX_RETRIES ]; do
  echo "Attempt $(($retry_count + 1)) of $MAX_RETRIES: Running command with timeout $TIMEOUT seconds..."

  timeout $TIMEOUT bash -c "$COMMAND"
  exit_code=$?
  if [ $exit_code -eq 0 ]; then
    echo "Command succeeded!"
    break
  else
    echo "Command failed. Exist code$exit_code. Retrying..."
    retry_count=$(($retry_count + 1))
  fi
done

if [ $retry_count -eq $MAX_RETRIES ]; then
  echo "Max retries ($MAX_RETRIES) reached. Giving up."
  exit 1
fi
