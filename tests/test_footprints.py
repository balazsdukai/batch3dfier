import pytest

from .context import db
from .context import footprints



def test_update_tile_index(batch3dfier_db):
    batch3dfier_db.sendQuery(
        "ALTER TABLE tile_index.bag_index DROP COLUMN IF EXISTS geom_border CASCADE;")
    batch3dfier_db.sendQuery("DROP TABLE IF EXISTS bag.pand_centroid CASCADE;")
    batch3dfier_db.sendQuery("DROP SCHEMA IF EXISTS bag_tiles CASCADE;")

    bag_index = ['tile_index', 'bag_index']
#     ahn_index = ['tile_index', 'ahn_index']
    bag_fields = ['gid', 'geom', 'unit']
    assert footprints.update_tile_index(
        batch3dfier_db, bag_index, bag_fields) is None


def test_create_centroids(batch3dfier_db):
    table_centroid = ['bag', 'pand_centroid']
    table_footprint = ['bag', 'pand']
    fields_footprint = ['gid', 'geom']
    assert footprints.create_centroids(
        batch3dfier_db,
        table_centroid,
        table_footprint,
        fields_footprint) is None


def test_create_views(batch3dfier_db):
    table_centroid = ['bag', 'pand_centroid']
    table_footprint = ['bag', 'pand']
    bag_index = ['tile_index', 'bag_index']
    bag_fields = ['gid', 'geom', 'unit']
    schema_tiles = 'bag_tiles'
    fields_centroid = ['gid', 'geom']
    fields_footprint = ['gid', 'geom']
    fields_footprint.append('identification')
    prefix_tiles = None
    assert footprints.create_views(
        batch3dfier_db,
        schema_tiles,
        bag_index,
        bag_fields,
        table_centroid,
        fields_centroid,
        table_footprint,
        fields_footprint,
        prefix_tiles) == "4 Views created in schema 'bag_tiles'."


def test_partition(batch3dfier_db):
    bag_index = ['tile_index', 'bag_index']
    bag_fields = ['gid', 'geom', 'unit']
    table_footprint = ['bag', 'pand']
    fields_footprint = ['gid', 'geom', 'identification']
    schema_tiles = 'bag_tiles'
    prefix_tiles = 't_'

    batch3dfier_db.sendQuery(
        "ALTER TABLE tile_index.bag_index DROP COLUMN IF EXISTS geom_border CASCADE;")
    batch3dfier_db.sendQuery("DROP TABLE IF EXISTS bag.pand_centroid CASCADE;")
    batch3dfier_db.sendQuery("DROP SCHEMA IF EXISTS bag_tiles CASCADE;")

    assert footprints.partition(
        batch3dfier_db,
        schema_tiles=schema_tiles,
        table_index=bag_index,
        fields_index=bag_fields,
        table_footprint=table_footprint,
        fields_footprint=fields_footprint,
        prefix_tiles=prefix_tiles) is None
