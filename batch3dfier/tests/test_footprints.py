from batch3dfier import db
from batch3dfier import footprints

dbs = db.db(dbname='batch3dfier_testing', host='localhost', port='5432',
         user= 'batch3dfier_tester', password='batch3d_test')


def test_update_tile_index():
    dbs.sendQuery("ALTER TABLE tile_index.bag_index DROP COLUMN IF EXISTS geom_border CASCADE;")
    dbs.sendQuery("DROP TABLE IF EXISTS bag.pand_centroid CASCADE;")
    dbs.sendQuery("DROP SCHEMA IF EXISTS bag_tiles CASCADE;")
    
    bag_index = ['tile_index', 'bag_index']
#     ahn_index = ['tile_index', 'ahn_index']
    bag_fields = ['gid', 'geom', 'unit']
    assert footprints.update_tile_index(dbs, bag_index, bag_fields) == None


def test_create_centroids():
    table_centroid = ['bag', 'pand_centroid']
    table_footprint = ['bag', 'pand']
    fields_footprint = ['gid', 'geom']
    assert footprints.create_centroids(dbs, table_centroid, table_footprint,
                                       fields_footprint
                                       ) == None


def test_create_views():
    table_centroid = ['bag', 'pand_centroid']
    table_footprint = ['bag', 'pand']
    bag_index = ['tile_index', 'bag_index']
    bag_fields = ['gid', 'geom', 'unit']
    schema_tiles = 'bag_tiles'
    fields_centroid = ['gid', 'geom']
    fields_footprint = ['gid', 'geom']
    fields_footprint.append('identification')
    prefix_tiles = None
    assert footprints.create_views(dbs, schema_tiles, bag_index, bag_fields,
                           table_centroid, fields_centroid, table_footprint,
                           fields_footprint, prefix_tiles
                           ) == "4 Views created in schema 'bag_tiles'."
                           


def test_partition():
    bag_index = ['tile_index', 'bag_index']
    bag_fields = ['gid', 'geom', 'unit']
    table_footprint = ['bag', 'pand']
    fields_footprint = ['gid', 'geom', 'identification']
    schema_tiles = 'bag_tiles'
    prefix_tiles = 't_'
    
    dbs.sendQuery("ALTER TABLE tile_index.bag_index DROP COLUMN IF EXISTS geom_border CASCADE;")
    dbs.sendQuery("DROP TABLE IF EXISTS bag.pand_centroid CASCADE;")
    dbs.sendQuery("DROP SCHEMA IF EXISTS bag_tiles CASCADE;")
    
    assert footprints.partition(dbs, schema_tiles=schema_tiles, table_index=bag_index,
                     fields_index=bag_fields, table_footprint=table_footprint,
                     fields_footprint=fields_footprint,
                     prefix_tiles=prefix_tiles
                     ) == None


