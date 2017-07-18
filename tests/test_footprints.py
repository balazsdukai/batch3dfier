from batch3dfier import db
from batch3dfier import footprints

dbs = db.db(dbname='batch3dfier_testing', host='localhost', port='5432',
         user= 'batch3dfier_tester', password='batch3d_test')


dbs.sendQuery("ALTER TABLE tile_index.bag_index DROP COLUMN IF EXISTS geom_border CASCADE;")
dbs.sendQuery("DROP TABLE IF EXISTS bagactueel.pand_centroid CASCADE;")
dbs.sendQuery("DROP SCHEMA IF EXISTS bag_tiles CASCADE;")


bag_index = ['tile_index', 'bag_index']
ahn_index = ['tile_index', 'ahn_index']
bag_fields = ['gid', 'geom', 'unit']

def test_update_tile_index():
    assert footprints.update_tile_index(dbs, bag_index, bag_fields) == None



table_centroid = ['bagactueel', 'pand_centroid']
table_footprint = ['bagactueel', 'pand']
fields_footprint = ['gid', 'geovlak']

def test_create_centroids():
    assert footprints.create_centroids(dbs, table_centroid, table_footprint,
                                       fields_footprint
                                       ) == None



schema_tiles = 'bag_tiles'
fields_centroid = ['gid', 'geom']
fields_footprint.append('identificatie')
prefix_tiles = None

def test_create_views():
    assert footprints.create_views(dbs, schema_tiles, bag_index, bag_fields,
                           table_centroid, fields_centroid, table_footprint,
                           fields_footprint, prefix_tiles
                           ) == "4 Views created in schema 'bag_tiles'."
