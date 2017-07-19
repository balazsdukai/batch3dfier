from batch3dfier import db
from batch3dfier import config

dbs = db.db(dbname='batch3dfier_testing', host='localhost', port='5432',
         user= 'batch3dfier_tester', password='batch3d_test')

bag_index = ['tile_index', 'bag_index']
file = "./batch3dfier/data/extent_small.geojson"
config.get_2Dtiles(dbs, bag_index, file)