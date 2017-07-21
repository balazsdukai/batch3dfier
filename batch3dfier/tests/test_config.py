import yaml

from batch3dfier import db
from batch3dfier import config

dbs = db.db(dbname='batch3dfier_testing', host='localhost', port='5432',
         user= 'batch3dfier_tester', password='batch3d_test')

cfg = {'tile_index': {'elevation': {'fields': {'geometry': 'geom',
    'primary_key': 'gid',
    'unit_name': 'unit'},
   'schema': 'tile_index',
   'table': 'ahn_index'},
  'polygons': {'fields': {'geometry': 'geom',
    'primary_key': 'gid',
    'unit_name': 'unit'},
   'schema': 'tile_index',
   'table': 'bag_index'}}}



ewkb_test = '010300002040710000010000000A000000DC5806A57984FD4047175D5475B01D41FEC869BE0583FD4062E2FD2847AF1D415FAB6787D87EFD40D24517BD20AE1D418C2EBAE89980FD4025A7F9FA6AAC1D41F17EE434E48AFD40F923A7597EAC1D41B0D5B3430B8AFD405A06A562CFAD1D411526DE8F028DFD40E3FDC8893BAF1D41D47CAD9E298CFD40CCA054383BB01D414A8589F71387FD401626DE2FB7B01D41DC5806A57984FD4047175D5475B01D41'
bag_index = cfg['tile_index']['polygons']
bag_fields_index = cfg['tile_index']['polygons']['fields']
ahn_index = cfg['tile_index']['elevation']
ahn_fields_index = cfg['tile_index']['elevation']['fields']

def test_polygon_to_ewkb():
    file = "/home/bdukai/Development/batch3dfier/example_data/extent_small.geojson"
#     ewkb_test = '010300002040710000010000000A000000DC5806A57984FD4047175D5475B01D41FEC869BE0583FD4062E2FD2847AF1D415FAB6787D87EFD40D24517BD20AE1D418C2EBAE89980FD4025A7F9FA6AAC1D41F17EE434E48AFD40F923A7597EAC1D41B0D5B3430B8AFD405A06A562CFAD1D411526DE8F028DFD40E3FDC8893BAF1D41D47CAD9E298CFD40CCA054383BB01D414A8589F71387FD401626DE2FB7B01D41DC5806A57984FD4047175D5475B01D41'
    poly_test = 'POLYGON ((120903.6027892561978661 486429.3323863637051545, 120880.3589876033074688 486353.7900309917749837, 120813.5330578512366628 486280.1846590909408405, 120841.6193181818234734 486170.7450929752667435, 121006.2629132231377298 486175.5875516529777087, 120992.7040289256256074 486259.8463326446944848, 121040.1601239669398637 486350.8845557851600461, 121026.6012396694277413 486414.8050103306304663, 120945.2479338842967991 486445.7967458678176627, 120903.6027892561978661 486429.3323863637051545))'
    poly, ewkb = config.extent_to_ewkb(dbs, bag_index, file)
    assert ewkb == ewkb_test
    assert poly.to_wkt() == poly_test


def test_get_2Dtiles():
#     ewkb_test = '010300002040710000010000000A000000DC5806A57984FD4047175D5475B01D41FEC869BE0583FD4062E2FD2847AF1D415FAB6787D87EFD40D24517BD20AE1D418C2EBAE89980FD4025A7F9FA6AAC1D41F17EE434E48AFD40F923A7597EAC1D41B0D5B3430B8AFD405A06A562CFAD1D411526DE8F028DFD40E3FDC8893BAF1D41D47CAD9E298CFD40CCA054383BB01D414A8589F71387FD401626DE2FB7B01D41DC5806A57984FD4047175D5475B01D41'
    tiles = config.get_2Dtiles(dbs, bag_index, bag_fields_index, ewkb_test)
    assert tiles == ['25gn1_c1', '25gn1_c2', '25gn1_c3', '25gn1_c4']


def test_find_pc_tiles_extent():
    tiles = config.find_pc_tiles(dbs, table_index_pc=ahn_index,
                                 fields_index_pc=ahn_fields_index,
                                 extent_ewkb=ewkb_test)
    assert tiles == ['25gn1_a', '25gn1_b']
    


def test_find_pc_tiles_tile():
    tile = '25gn1_c1'
    tiles = config.find_pc_tiles(dbs, table_index_pc=ahn_index,
                                 fields_index_pc=ahn_fields_index,
                                 table_index_footprint=bag_index,
                                 fields_index_footprint=bag_fields_index,
                                 tile_footprint=tile)
    assert tiles == ['25gn1_a', '25gn1_b']


def test_find_pc_files():
    pc_tiles = ['25gn1_a', '25gn1_b']
    pc_dir = "/home/bdukai/Development/batch3dfier/example_data/"
    pc_file_name = "c_{tile}.laz"
    tile_case = "lower"
    pc_path_test = ['/home/bdukai/Development/batch3dfier/example_data/c_25gn1_a.laz', '/home/bdukai/Development/batch3dfier/example_data/c_25gn1_b.laz']

    pc_path = config.find_pc_files(pc_tiles, pc_dir, pc_file_name, tile_case)
    pc_path_none = config.find_pc_files(pc_tiles, pc_dir, pc_file_name, "upper")
    
    assert pc_path == pc_path_test
    assert pc_path_none == None
    

def test_yamlr():
    schema_tiles = 'bag_tiles'
    tile = '25gn1_c1'
    pc_path_test = ['/home/bdukai/Development/batch3dfier/example_data/c_25gn1_a.laz', '/home/bdukai/Development/batch3dfier/example_data/c_25gn1_b.laz']
    output_format = 'CSV-BUILDINGS-MULTIPLE'
    
    y_test = {'input_elevation': [{'datasets': ['/home/bdukai/Development/batch3dfier/example_data/c_25gn1_a.laz',
                '/home/bdukai/Development/batch3dfier/example_data/c_25gn1_b.laz'],
               'omit_LAS_classes': [1],
               'thinning': 0}],
             'input_polygons': [{'datasets': ['PG:dbname=batch3dfier_testing host=localhost user=batch3dfier_tester password=batch3d_test schemas=bag_tiles tables=25gn1_c1'],
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
    
    y = config.yamlr(dbname=dbs.dbname, host=dbs.host, user=dbs.user,
                   pw=dbs.password, schema_tiles=schema_tiles,
                   bag_tile=tile, pc_path=pc_path_test,
                   output_format=output_format)
    
    y_parsed = yaml.load(y)
    
    assert y_parsed == y_test


def test_get_view_fields():
    user_schema = 'bag_tiles'
    tile_views = ['t_25gn1_c1', 't_25gn1_c2', 't_25gn1_c3', 't_25gn1_c4']
    fields = config.get_view_fields(dbs, user_schema, tile_views)
    fields_test = {'all': ['gid', 'geom', 'identification'], 'geometry': 'geom'}
    
    assert fields == fields_test

def test_parse_sql_select_fields():
    table = 't_25gn1_c1'
    fields = ['gid', 'geom', 'identification']
    sql = config.parse_sql_select_fields(table, fields)
    sql_test = '"t_25gn1_c1"."gid", "t_25gn1_c1"."geom", "t_25gn1_c1"."identification"'
    sql_str = sql.as_string(dbs.conn)

    assert sql_str == sql_test
    