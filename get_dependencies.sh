# download planetiler jar
curl -L https://github.com/onthegomap/planetiler/releases/download/v0.10.2/planetiler.jar -o planetiler.jar

# download Regrid Data Store sample
curl -L https://app.regrid.com/store/us/tx/dallas/sample/full.parquet -o tx_dallas.parquet

# download latest release of Overture Maps, for only Dallas County
./bbox.sh 2026-04-15.0 -97.0383833,32.5453486,-96.5168819,32.9896692 overture_dallas.parquet