import pytest
import yaml

from batch3dfier import db
from batch3dfier import config

@pytest.fixture("module")
def batch3dfier_db(request):
    dbs = db.db(dbname='batch3dfier_db', host='localhost', port='5432',
                user= 'batch3dfier', password='batch3d_test')
    def disconnect():
        dbs.close()
    request.addfinalizer(disconnect)
    
    return(dbs)


@pytest.fixture("module")
def polygons():
    p = {'file': "/example_data/extent_small.geojson",
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
         'dataset_dir': "/example_data/",
         'dataset_name': "c_{tile}.laz",
         'tile_case': "lower",
         'pc_path': ['/example_data/c_25gn1_a.laz',
                     '/example_data/c_25gn1_b.laz']
         }
    return(p)


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
    tiles = config.find_pc_tiles(batch3dfier_db,
                                 table_index_pc=cfg['tile_index']['elevation'],
                                 fields_index_pc=cfg['tile_index']['elevation']['fields'],
                                 extent_ewkb=polygons['ewkb'])
    assert tiles == ['25gn1_a', '25gn1_b']
    


def test_find_pc_tiles_tile(batch3dfier_db, cfg, tile):
    tiles = config.find_pc_tiles(batch3dfier_db,
                                 table_index_pc=cfg['tile_index']['elevation'],
                                 fields_index_pc=cfg['tile_index']['elevation']['fields'],
                                 table_index_footprint=cfg['tile_index']['polygons'],
                                 fields_index_footprint=cfg['tile_index']['polygons']['fields'],
                                 tile_footprint=tile)
    assert tiles == ['25gn1_a', '25gn1_b']


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
    
    assert pc_path == None
    
    

def test_yamlr(batch3dfier_db, pointcloud, tile):
    schema_tiles = 'bag_tiles'
    output_format = 'CSV-BUILDINGS-MULTIPLE'
    uniqueid = 'identificatie'
    
    y_test = {'input_elevation': [{'datasets': ['/example_data/c_25gn1_a.laz',
                                                '/example_data/c_25gn1_b.laz'],
                                   'omit_LAS_classes': [1],
                                   'thinning': 0}],
              'input_polygons': [{'datasets': ['PG:dbname=batch3dfier_db host=localhost user=batch3dfier_tester password=batch3d_test schemas=bag_tiles tables=25gn1_c1'],
                                  'lifting': 'Building',
                                  'uniqueid': 'identificatie'}],
              'lifting_options': {'Building': {'height_floor': 'percentile-10',
                                               'height_roof': 'percentile-90',
                                               'lod': 1}},
              'options': {'building_radius_vertex_elevation': 2.0,
                          'radius_vertex_elevation': 1.0,
                          'threshold_jump_edges': 0.5},
              'output': {'building_floor': True,
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
    