#!/bin/bash

# Replaces environment variables in a sample harmony message with their
# values in the project's `.env` file.

if [ "$#" != 1 ]
then
  echo "Usage"
  echo "  replace.sh harmony-message.json"
  exit 1
fi

if [ ! -f .env ]
then
  echo "Error: Missing environment file (.env). See README."
fi

set -o allexport
. .env
sed -e "s/\${STAGING_BUCKET}/$STAGING_BUCKET/" $1
set +o allexport
