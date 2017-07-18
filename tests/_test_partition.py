from batch3dfier import db
from batch3dfier import footprints

dbs = db.db(dbname='batch3dfier_testing', host='localhost', port='5432',
         user= 'batch3dfier_tester', password='batch3d_test')


dbs.sendQuery("ALTER TABLE tile_index.bag_index DROP COLUMN IF EXISTS geom_border CASCADE;")
dbs.sendQuery("DROP TABLE IF EXISTS bagactueel.pand_centroid CASCADE;")
dbs.sendQuery("DROP SCHEMA IF EXISTS bag_tiles CASCADE;")

bag_index = ['tile_index', 'bag_index']
bag_fields = ['gid', 'geom', 'unit']
table_footprint = ['bagactueel', 'pand']
fields_footprint = ['gid', 'geovlak', 'identificatie']
schema_tiles = 'bag_tiles'
prefix_tiles = 't_'

def test_partition():
    assert footprints.partition(dbs, schema_tiles=schema_tiles, table_index=bag_index,
                     fields_index=bag_fields, table_footprint=table_footprint,
                     fields_footprint=fields_footprint,
                     prefix_tiles=prefix_tiles
                     ) == None

