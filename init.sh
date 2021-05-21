#!/bin/bash

rm -rf /tmp/database

rm /tmp/buckets/0_bucket.kv
rm /tmp/buckets/1_bucket.kv
rm /tmp/buckets/0_bucket.aux
rm /tmp/buckets/1_bucket.aux

dd if=/dev/zero of=/tmp/buckets/0_bucket.kv bs=1024 count=1
dd if=/dev/zero of=/tmp/buckets/1_bucket.kv bs=1024 count=1
dd if=/dev/zero of=/tmp/buckets/0_bucket.aux bs=1024 count=1
dd if=/dev/zero of=/tmp/buckets/1_bucket.aux bs=1024 count=1
