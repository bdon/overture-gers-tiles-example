#!/usr/bin/env bash

set -e
set -u
set -o pipefail

RELEASE=$1
BBOX=$2
OUTPUT_FILE=$3

# Parse bbox
IFS=',' read -r MIN_LON MIN_LAT MAX_LON MAX_LAT <<< "$BBOX"

PARQUET_PATH="s3://overturemaps-us-west-2/release/$RELEASE/theme=places/type=place/*.parquet"

# Run DuckDB query
duckdb -c "
INSTALL spatial;
LOAD spatial;

SET s3_region='us-west-2'; SET s3_url_style='path';

-- Query and filter data by bbox
COPY (
    SELECT
        *
    FROM read_parquet('$PARQUET_PATH', union_by_name=true, filename=true, hive_partitioning=false)
    WHERE bbox.xmin <= $MAX_LON
      AND bbox.xmax >= $MIN_LON
      AND bbox.ymin <= $MAX_LAT
      AND bbox.ymax >= $MIN_LAT
) TO '$OUTPUT_FILE';
"