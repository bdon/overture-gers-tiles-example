# Overture GERS Tiles Example

Generates a vector tileset of [Overture Maps](https://overturemaps.org) Places for Dallas, TX, joined to parcel zoning data from [Regrid](https://regrid.com) via the Overture [Global Entity Reference System (GERS)](https://docs.overturemaps.org/gers/).

View the tileset at [bdon.github.io/overture-gers-tiles-example/](https://bdon.github.io/overture-gers-tiles-example/).

## Data sources

- **Overture Places** (`overture_dallas.parquet`) — point-of-interest data with GERS IDs and confidence scores, queried by bounding box from the official Overture S3 bucket (`s3://overturemaps-us-west-2/release/`) using DuckDB.
- **Regrid parcels** (`tx_dallas.parquet`) — parcel records for Dallas County including zoning type/subtype, keyed by `ll_uuid`. Dallas County is available as [free sample data](https://app.regrid.com/store/samples) from the Regrid Data Store.
- **GERS bridge table** (`dallas_bridge.parquet`) — maps Overture GERS IDs to Regrid `ll_uuid`s, enabling the join between the two datasets.

## Requirements

- Java 21+
- [DuckDB](https://duckdb.org) (for downloading Overture data from S3)

## Setup

Fetch all dependencies (Planetiler JAR, Overture Places extract, and Regrid parcel data):

```sh
./get_dependencies.sh
```

## Generate tiles

```sh
java -cp planetiler.jar OvertureProfile.java
```

This produces `dallas.pmtiles`.

## View the map

Serve the project directory with any static file server and open `index.html`:

```sh
python3 -m http.server
```
