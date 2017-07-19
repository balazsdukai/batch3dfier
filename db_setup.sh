psql postgres

create role batch3dfier_tester with login password 'batch3d_test';
create database batch3dfier_testing owner batch3dfier_tester;
\c batch3dfier_testing
create extension postgis;
\c batch3dfier_testing batch3dfier_tester localhost
create schema ahn3;
create schema bagactueel;
\q

ogr2ogr -f PostgreSQL PG:"dbname=batch3dfier_testing active_schema=tile_index\
 host=localhost port=5432 user=batch3dfier_tester password=batch3d_test"\
 ./data/bag_index.geojson -nln bag_index\
 -skip-failure -a_srs EPSG:28992 -lco FID=gid -lco GEOMETRY_NAME=geom
 
ogr2ogr -f PostgreSQL PG:"dbname=batch3dfier_testing active_schema=tile_index\
 host=localhost port=5432 user=batch3dfier_tester password=batch3d_test"\
 ./data/ahn_index.geojson -nln ahn_index\
 -skip-failure -a_srs EPSG:28992 -lco FID=gid -lco GEOMETRY_NAME=geom
 
ogr2ogr -f PostgreSQL PG:"dbname=batch3dfier_testing active_schema=bagactueel\
 host=localhost port=5432 user=batch3dfier_tester password=batch3d_test"\
 ./data/bag_pand.geojson -nln pand\
 -skip-failure -a_srs EPSG:28992 -lco FID=gid -lco GEOMETRY_NAME=geovlak

