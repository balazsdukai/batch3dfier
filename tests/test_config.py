import os.path

import pytest
import yaml

from .context import db
from .context import config

from pprint import pprint



@pytest.fixture("module")
def polygons():
    p = {'file': os.path.abspath("example_data/extent_small.geojson"),
         'ewkb': '010300002040710000010000000A000000DC5806A57984FD4047175D5475B01D41FEC869BE0583FD4062E2FD2847AF1D415FAB6787D87EFD40D24517BD20AE1D418C2EBAE89980FD4025A7F9FA6AAC1D41F17EE434E48AFD40F923A7597EAC1D41B0D5B3430B8AFD405A06A562CFAD1D411526DE8F028DFD40E3FDC8893BAF1D41D47CAD9E298CFD40CCA054383BB01D414A8589F71387FD401626DE2FB7B01D41DC5806A57984FD4047175D5475B01D41',
         'wkt': 'POLYGON ((120903.6027892561978661 486429.3323863637051545, 120880.3589876033074688 486353.7900309917749837, 120813.5330578512366628 486280.1846590909408405, 120841.6193181818234734 486170.7450929752667435, 121006.2629132231377298 486175.5875516529777087, 120992.7040289256256074 486259.8463326446944848, 121040.1601239669398637 486350.8845557851600461, 121026.6012396694277413 486414.8050103306304663, 120945.2479338842967991 486445.7967458678176627, 120903.6027892561978661 486429.3323863637051545))'}
    return(p)


@pytest.fixture("module")
def cfg():
    c = {'tile_index': {'elevation': {'fields': {'geometry': 'geom',
                                                 'primary_key': 'gid',
                                                 'unit_name': 'unit'},
                                      'schema': 'tile_index',
                                      'table': 'ahn_index'},
                        'polygons': {'fields': {'geometry': 'geom',
                                                'primary_key': 'gid',
                                                'unit_name': 'unit'},
                                     'schema': 'tile_index',
                                     'table': 'bag_index'}}}
    return(c)


@pytest.fixture("module")
def pointcloud():
    p = {'pc_tiles': ['25gn1_a', '25gn1_b'],
         'dataset_dir': os.path.abspath("example_data/"),
         'dataset_name': "c_{tile}.laz",
         'tile_case': "lower",
         'pc_path': [os.path.abspath('example_data/c_25gn1_a.laz'),
                     os.path.abspath('example_data/c_25gn1_b.laz')]
         }
    return(p)


@pytest.fixture("module")
def pc_name_map():
    d = {'/home/bdukai/Development/batch3dfier/example_data': {
            'name': 'a_{tile}.laz',
            'priority': 0},
        '/home/bdukai/Development/batch3dfier/example_data/ahn1': {
            'name': 'a_{tile}.laz',
            'priority': 3},
        '/home/bdukai/Development/batch3dfier/example_data/ahn2/ground': {
            'name': 'b_{tile}.laz',
            'priority': 2},
        '/home/bdukai/Development/batch3dfier/example_data/ahn2/other': {
            'name': 'o-{tile}.laz',
            'priority': 2},
        '/home/bdukai/Development/batch3dfier/example_data/ahn2/rest': {
            'name': 'c{tile}.laz',
            'priority': 2},
        '/home/bdukai/Development/batch3dfier/example_data/ahn3': {
            'name': 'a_{tile}.laz',
            'priority': 1}}
    return d


@pytest.fixture("module")
def pc_dir():
    pc_dir = ['/home/bdukai/Development/batch3dfier/example_data',
          '/home/bdukai/Development/batch3dfier/example_data/ahn3',
          ['/home/bdukai/Development/batch3dfier/example_data/ahn2/ground',
           '/home/bdukai/Development/batch3dfier/example_data/ahn2/rest',
           '/home/bdukai/Development/batch3dfier/example_data/ahn2/other'],
          '/home/bdukai/Development/batch3dfier/example_data/ahn1']
    return pc_dir


@pytest.fixture("module")
def pc_dataset_name():
    dataset_name = ['a_{tile}.laz', 'a_{tile}.laz', 
                ['b_{tile}.laz', 'c{tile}.laz', 'o-{tile}.laz'],
                'a_{tile}.laz']
    return dataset_name


# @pytest.fixture("module")
# def pc_format():
#     p = {'pc_tiles': ['1kD', '2aba', '3AA', 'zz4'],
#          'dataset_name'}
#     return p


@pytest.fixture("module")
def tile():
    return('25gn1_c1')


def test_polygon_to_ewkb(batch3dfier_db, cfg, polygons):
    poly, ewkb = config.extent_to_ewkb(batch3dfier_db,
                                       cfg['tile_index']['polygons'],
                                       polygons['file'])
    assert ewkb == polygons['ewkb']
    assert poly.to_wkt() == polygons['wkt']


def test_get_2Dtiles(batch3dfier_db, cfg, polygons):
    tiles = config.get_2Dtiles(batch3dfier_db,
                               cfg['tile_index']['polygons'],
                               cfg['tile_index']['polygons']['fields'],
                               polygons['ewkb'])
    assert tiles == ['25gn1_c1', '25gn1_c2', '25gn1_c3', '25gn1_c4']


def test_find_pc_tiles_extent(batch3dfier_db, cfg, polygons):
    tiles = config.find_pc_tiles(
        batch3dfier_db,
        table_index_pc=cfg['tile_index']['elevation'],
        fields_index_pc=cfg['tile_index']['elevation']['fields'],
        extent_ewkb=polygons['ewkb'])
    assert tiles == ['25gn1_a', '25gn1_b']


def test_find_pc_tiles_tile(batch3dfier_db, cfg, tile):
    tiles = config.find_pc_tiles(
        batch3dfier_db,
        table_index_pc=cfg['tile_index']['elevation'],
        fields_index_pc=cfg['tile_index']['elevation']['fields'],
        table_index_footprint=cfg['tile_index']['polygons'],
        fields_index_footprint=cfg['tile_index']['polygons']['fields'],
        tile_footprint=tile)
    assert tiles == ['25gn1_a', '25gn1_b']


def test_pc_name_dict(pc_name_map, pc_dir, pc_dataset_name):
    pc_dict = config.pc_name_dict(pc_dir, pc_dataset_name)
    assert pc_dict == pc_name_map


def test_pc_name_dict_err():
    dataset_dir = ['a', ['b', ['c', 'c1']], 'd']
    dataset_name = ['a_{tile}', ['b{tile}', ['c-{tile}', 'c1-{tile}']], 'd_{tile}']
    with pytest.raises(ValueError):
        config.pc_name_dict(dataset_dir, dataset_name)


def test_pc_file_index(pc_name_map):
    d = {
        '1': ['/home/bdukai/Development/batch3dfier/example_data/a_1.laz'],
        '2': ['/home/bdukai/Development/batch3dfier/example_data/ahn3/a_2.laz'],
        '3a': ['/home/bdukai/Development/batch3dfier/example_data/ahn2/other/o-3a.laz',
               '/home/bdukai/Development/batch3dfier/example_data/ahn2/rest/c3a.laz',
               '/home/bdukai/Development/batch3dfier/example_data/ahn2/ground/B_3A.laz'],
        '4b': ['/home/bdukai/Development/batch3dfier/example_data/ahn2/ground/B_4B.laz'],
        '6': ['/home/bdukai/Development/batch3dfier/example_data/ahn2/other/o-6.laz']}
    pc_file_idx = config.pc_file_index(pc_name_map)
    pc_vals = set([f for i in pc_file_idx.values() for f in i])
    d_vals = set([f for i in d.values() for f in i])
    assert (set(pc_file_idx.keys()) == set(d.keys())) & (pc_vals == d_vals)


# def test_format_tile_names():
#     res = config.format_tile_name(pc_tiles, pc_dataset_name, pc_tile_case)

def test_find_pc_files(pointcloud):
    pc_path = config.find_pc_files(pointcloud['pc_tiles'],
                                   pointcloud['dataset_dir'],
                                   pointcloud['dataset_name'],
                                   pointcloud['tile_case'])

    assert pc_path == pointcloud['pc_path']


def test_find_pc_files_none(pointcloud):
    pc_path = config.find_pc_files(pointcloud['pc_tiles'],
                                   pointcloud['dataset_dir'],
                                   pointcloud['dataset_name'],
                                   "upper")

    assert pc_path is None


def test_yamlr(batch3dfier_db, pointcloud, tile):
    schema_tiles = 'bag_tiles'
    output_format = 'CSV-BUILDINGS-MULTIPLE'
    uniqueid = 'identificatie'

    y_test = {
        'input_elevation': [
            {
                'datasets': [
                    os.path.abspath('example_data/c_25gn1_a.laz'),
                    os.path.abspath('example_data/c_25gn1_b.laz')],
                'omit_LAS_classes': None,
                'thinning': 0}],
        'input_polygons': [
            {
                'datasets': ['PG:dbname=batch3dfier_db host=localhost user=batch3dfier password=batch3d_test schemas=bag_tiles tables=25gn1_c1'],
                'lifting': 'Building',
                'uniqueid': 'identificatie'}],
        'lifting_options': {
            'Building': {
                'height_floor': 'percentile-10',
                                'height_roof': 'percentile-90',
                                'lod': 1}},
        'options': {
            'building_radius_vertex_elevation': 2.0,
            'radius_vertex_elevation': 1.0,
            'threshold_jump_edges': 0.5},
        'output': {
            'building_floor': True,
            'format': 'CSV-BUILDINGS-MULTIPLE',
            'vertical_exaggeration': 0}}

    y = config.yamlr(dbname=batch3dfier_db.dbname,
                     host=batch3dfier_db.host,
                     user=batch3dfier_db.user,
                     pw=batch3dfier_db.password,
                     schema_tiles=schema_tiles,
                     bag_tile=tile,
                     pc_path=pointcloud['pc_path'],
                     output_format=output_format,
                     uniqueid=uniqueid)

    y_parsed = yaml.load(y)

    assert y_parsed == y_test


def test_get_view_fields(batch3dfier_db):
    user_schema = 'bag_tiles'
    tile_views = ['t_25gn1_c1', 't_25gn1_c2', 't_25gn1_c3', 't_25gn1_c4']
    fields = config.get_view_fields(batch3dfier_db, user_schema, tile_views)
    fields_test = {'all': ['gid', 'geom', 'identification'],
                   'geometry': 'geom'}

    assert fields == fields_test


def test_parse_sql_select_fields(batch3dfier_db):
    table = 't_25gn1_c1'
    fields = ['gid', 'geom', 'identification']
    sql = config.parse_sql_select_fields(table, fields)
    sql_test = '"t_25gn1_c1"."gid", "t_25gn1_c1"."geom", "t_25gn1_c1"."identification"'
    sql_str = sql.as_string(batch3dfier_db.conn)

    assert sql_str == sql_test
