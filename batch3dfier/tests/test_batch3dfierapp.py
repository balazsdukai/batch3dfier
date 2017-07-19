from batch3dfier.batch3dfierapp import parse_config_yaml

args_in = {}
args_in['cfg_file'] = "/home/bdukai/Development/batch3dfier/test_batch3dfier_config.yml"

def test_parse_config_yaml():
    cfg = parse_config_yaml(args_in)
    cfg.pop('dbase')
    assert cfg == {'elevation': {'fields': {'geometry': 'geom',
                   'primary_key': 'gid',
                   'unit_name': 'unit'},
                  'schema': 'tile_index',
                  'table': 'ahn_index'},
                 'extent_file': '/home/bdukai/Development/batch3dfier/example_data/extent_small.geojson',
                 'output_dir': '/home/bdukai/Data/3DBAG',
                 'output_format': 'CSV-BUILDINGS-MULTIPLE',
                 'path_3dfier': '/opt/3dfier-0.9.6/build/3dfier',
                 'pc_dir': '/home/bdukai/Development/batch3dfier/example_data',
                 'pc_file_name': 'c_{tile}.laz',
                 'pc_tile_case': 'lower',
                 'polygons': {'fields': {'geometry': 'geom',
                   'primary_key': 'gid',
                   'unit_name': 'unit'},
                  'schema': 'tile_index',
                  'table': 'bag_index'},
                 'tile_schema': 'bag_tiles',
                 'tiles': None,
                 'user_schema': 'bag_tiles'}