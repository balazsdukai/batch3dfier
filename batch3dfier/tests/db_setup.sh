
#psql -d postgres -c "create role batch3dfier with login password 'batch3d_test';"
createdb -O batch3dfier batch3dfier_test 
psql -d batch3dfier_test -c "create extension postgis;\
                             create schema tile_index authorization batch3dfier;\
                             create schema bag authorization batch3dfier;"

ogr2ogr -f PostgreSQL PG:"dbname=batch3dfier_test\
 host=localhost port=5432 user=batch3dfier password=batch3d_test"\
 ./example_data/bag_index.geojson -nln bag_index\
 -skip-failure -a_srs EPSG:28992 -lco FID=gid -lco GEOMETRY_NAME=geom\
 -lco SCHEMA=tile_index
 
ogr2ogr -f PostgreSQL PG:"dbname=batch3dfier_test\
 host=localhost port=5432 user=batch3dfier password=batch3d_test"\
 ./example_data/ahn_index.geojson -nln ahn_index\
 -skip-failure -a_srs EPSG:28992 -lco FID=gid -lco GEOMETRY_NAME=geom\
 -lco SCHEMA=tile_index
 
ogr2ogr -f PostgreSQL PG:"dbname=batch3dfier_test\
 host=localhost port=5432 user=batch3dfier password=batch3d_test"\
 ./example_data/bag_pand.geojson -nln pand\
 -skip-failure -a_srs EPSG:28992 -lco FID=gid -lco GEOMETRY_NAME=geom\
 -lco SCHEMA=bag
 
#python3 -m pytest -v ./batch3dfier/tests/test_footprints.py

dropdb batch3dfier_test
#psql -d postgres -c "drop role batch3dfier;"