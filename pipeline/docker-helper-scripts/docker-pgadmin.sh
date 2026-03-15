#!/usr/bin/env bash

## bash script to start pgadmin
docker run -it --rm \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  --mount type=volume,source=pgadmin_data,target=/var/lib/pgadmin \
  -p 8085:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4