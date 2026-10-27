#!/bin/sh
# S3 identity config is generated from the environment -> no credentials in the repo
set -e
cat > /tmp/s3.json <<JSON
{"identities":[{"name":"lakehouse","credentials":[{"accessKey":"${S3_ACCESS_KEY}","secretKey":"${S3_SECRET_KEY}"}],
  "actions":["Admin","Read","Write","List","Tagging"]}]}
JSON
exec weed server -dir=/data -s3 -s3.port=8333 -s3.config=/tmp/s3.json \
  -master.volumeSizeLimitMB=1024 -volume.max=0
