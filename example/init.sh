#!/bin/bash

rm -rf /tmp/database

mkdir -p /tmp/buckets

# 4 buckets 0..3 each for 1024 * 2048 bytes
for idx in $(seq 0 7)
do
  touch /tmp/buckets/${idx}_bucket.kv
  touch /tmp/buckets/${idx}_bucket.aux
  rm /tmp/buckets/${idx}_bucket.kv
  rm /tmp/buckets/${idx}_bucket.aux
  dd if=/dev/zero of=/tmp/buckets/${idx}_bucket.kv bs=1024 count=1024
  dd if=/dev/zero of=/tmp/buckets/${idx}_bucket.aux bs=1024 count=1024
done
