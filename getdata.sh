#!/usr/bin/env bash
mkdir ~/tmp/data
mkdir ~/tmp/data/pl-mlp-roads
cd ~/tmp/data
#wget https://download.geofabrik.de/europe/poland/malopolskie-latest-free.shp.zip
unzip -j malopolskie-latest-free.shp.zip gis_osm_roads_* -d pl-mlp-roads
#wget https://download.geofabrik.de/europe/poland/mazowieckie-latest-free.shp.zip
#unzip -j mazowieckie-latest-free.shp.zip gis_osm_roads_* -d pl-maz-roads
#wget https://download.geofabrik.de/europe/poland/podkarpackie-latest-free.shp.zip
#unzip -j podkarpackie-latest-free.shp.zip gis_osm_roads_* -d pl-pod-roads
#wget https://download.geofabrik.de/europe/poland/slaskie-latest-free.shp.zip
#unzip -j slaskie-latest-free.shp.zip gis_osm_roads_* -d pl-sla-roads
#wget https://download.geofabrik.de/europe/poland/dolnoslaskie-latest-free.shp.zip
#unzip -j dolnoslaskie-latest-free.shp.zip gis_osm_roads_* -d pl-dln-roads
