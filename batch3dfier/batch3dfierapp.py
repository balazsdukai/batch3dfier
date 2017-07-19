#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""The batch3dfier application."""


import os.path
import queue
import threading
import time
import warnings
import argparse
from subprocess import call

import yaml
from psycopg2 import sql

from batch3dfier import config
from batch3dfier import db


def main():
    #===========================================================================
    # User input and Settings
    #===========================================================================
    # Parse command-line arguments ---------------------------------------------
    parser = argparse.ArgumentParser(description="Batch 3dfy 2D dataset(s).")
    parser.add_argument(
        "config",
        metavar="config_file",
        help="The YAML config file for batch3dfier.")
    parser.add_argument(
        "-t", "--threads",
        help="The number of threads to initiate.",
        default=3,
        type=int)
    args = parser.parse_args()
    CFG_FILE = os.path.abspath(args.config)
    CFG_DIR = os.path.dirname(CFG_FILE)
    THREADS = args.threads
    
    stream = open(CFG_FILE, "r")
    cfg = yaml.load(stream)
    
    PC_FILE_NAME = cfg["input_elevation"]["dataset_name"]
    PC_DIR = os.path.abspath(cfg["input_elevation"]["dataset_dir"])
    TILE_CASE = cfg["input_elevation"]["tile_case"]
    DBNAME = cfg["input_polygons"]["database"]["dbname"]
    HOST = cfg["input_polygons"]["database"]["host"]
    PORT = cfg["input_polygons"]["database"]["port"]
    USER = cfg["input_polygons"]["database"]["user"]
    PW = cfg["input_polygons"]["database"]["pw"]
    TILE_SCHEMA = cfg["input_polygons"]["database"]["tile_schema"]
    POLY_TILE_INDEX = cfg["tile_index"]["polygons"]
    PC_TILE_INDEX = cfg["tile_index"]["elevation"]
    USER_SCHEMA = cfg["input_polygons"]["database"]["user_schema"]
    
    OUTPUT_FORMAT = cfg["output"]["format"]
    if all(f not in OUTPUT_FORMAT.lower() for f in ["csv", "obj"]):
        warnings.warn(
            "\n No file format is appended to output. Currently only .obj or .csv is handled.\n")
    
    OUTPUT_DIR = os.path.abspath(cfg["output"]["dir"])
    PATH_3DFIER = cfg["path_3dfier"]
    
    try:
        # in case user gave " " or "" for 'extent'
        if len(cfg["input_polygons"]["extent"]) <= 1:
            EXTENT_FILE = None
        EXTENT_FILE = os.path.abspath(cfg["input_polygons"]["extent"])
    except (NameError, AttributeError, TypeError):
        tiles = cfg["input_polygons"]["tile_list"]  # a list of 2D tiles as input
        tiles = [t.strip() for t in tiles.split(sep=",")]
        EXTENT_FILE = None
        union_view = None
        tiles_clipped = None
    
    # 'user_schema' is used for the '_clip3dfy_' and '_union' views, thus
    # only use 'user_schema' if 'extent' is provided
    if (USER_SCHEMA is None) or (EXTENT_FILE is None):
        USER_SCHEMA = TILE_SCHEMA
    
    # Prefix for naming the clipped/united views. This value shouldn't be a
    # substring in the pointcloud file names.
    CLIP_PREFIX = "_clip3dfy_"
    
    # Connect to database ------------------------------------------------------
    dbase = db.db(DBNAME, HOST, PORT, USER ,PW)
    
    #===========================================================================
    # Get tile list if EXTENT_FILE provided
    #===========================================================================
    # TODO: assert that CREATE/DROP allowed on TILE_SCHEMA and/or USER_SCHEMA
    if EXTENT_FILE:
        poly, ewkb = config.extent_to_ewkb(dbase, POLY_TILE_INDEX, EXTENT_FILE)
        
        tiles = config.get_2Dtiles(dbase, POLY_TILE_INDEX, ewkb)
    
        # Get view names for tiles
        tile_views = config.get_2Dtile_views(dbase, TILE_SCHEMA, tiles)
    
        # clip 2D tiles to extent
        tiles_clipped = config.clip_2Dtiles(dbase, USER_SCHEMA, TILE_SCHEMA,
                                            tile_views, poly, CLIP_PREFIX)
    
        # if the area of the extent is less than that of a tile, union the tiles is the
        # extent spans over many
        tile_area = config.get_2Dtile_area(db=dbase, tile_index=POLY_TILE_INDEX)
        if len(tiles_clipped) > 1 and poly.area < tile_area:
            union_view = config.union_2Dtiles(
                dbase, USER_SCHEMA, tiles_clipped, CLIP_PREFIX)
        else:
            union_view = []
    else:
        tile_views = config.get_2Dtile_views(dbase, TILE_SCHEMA, tiles)
    
    #===========================================================================
    # Get tile list if TILE_LIST = 'all'
    #===========================================================================
    
    if 'all' in tiles:
        schema = sql.Identifier(POLY_TILE_INDEX[0])
        table = sql.Identifier(POLY_TILE_INDEX[1])
        query = sql.SQL("""
                    SELECT a.unit
                    FROM {schema}.{table} as a;
                    """).format(schema=schema, table=table)
        resultset = dbase.getQuery(query)
        tiles = [tile[0] for tile in resultset]
        
    #===========================================================================
    # Get pointcloud tiles
    #===========================================================================
#     if EXTENT_FILE:
#         pc_tiles = config.get_2Dtiles(ewkb=ewkb, db=dbase,
#                                       tile_index=PC_TILE_INDEX)
    #===========================================================================
    # Process multiple threads
    # reference: http://www.tutorialspoint.com/python3/python_multithreading.htm
    #===========================================================================
#     
#     exitFlag = 0
#     tiles_skipped = []
#     
#     
#     class myThread (threading.Thread):
#     
#         def __init__(self, threadID, name, q):
#             threading.Thread.__init__(self)
#             self.threadID = threadID
#             self.name = name
#             self.q = q
#     
#         def run(self):
#             print ("Starting " + self.name)
#             process_data(self.name, self.q)
#             print ("Exiting " + self.name)
#     
#     
#     def process_data(threadName, q):
#         while not exitFlag:
#             queueLock.acquire()
#             if not workQueue.empty():
#                 tile = q.get()
#                 queueLock.release()
#                 print ("%s processing %s" % (threadName, tile))
#                 t = config.call3dfier(tile=tile,
#                                       thread=threadName,
#                                       clip_prefix=CLIP_PREFIX,
#                                       union_view=union_view,
#                                       tiles=tiles,
#                                       pc_file_name=PC_FILE_NAME,
#                                       pc_dir=PC_DIR,
#                                       tile_case=TILE_CASE,
#                                       yml_dir=CFG_DIR,
#                                       dbname=DBNAME,
#                                       host=HOST,
#                                       user=USER,
#                                       pw=PW,
#                                       tile_schema=USER_SCHEMA,
#                                       output_format=OUTPUT_FORMAT,
#                                       output_dir=OUTPUT_DIR,
#                                       path_3dfier=PATH_3DFIER)
#                 if t is not None:
#                     tiles_skipped.append(t)
#             else:
#                 queueLock.release()
#                 time.sleep(1)
#     
#     # Prep
#     
#     threadList = ["Thread-" + str(t+1) for t in range(THREADS)]
#     queueLock = threading.Lock()
#     workQueue = queue.Queue(0)
#     threads = []
#     threadID = 1
#     
#     # Create new threads
#     for tName in threadList:
#         thread = myThread(threadID, tName, workQueue)
#         thread.start()
#         threads.append(thread)
#         threadID += 1
#     
#     # Fill the queue
#     queueLock.acquire()
#     if union_view:
#         workQueue.put(union_view)
#     elif tiles_clipped:
#         for tile in tiles_clipped:
#             workQueue.put(tile)
#     else:
#         for tile in tile_views:
#             workQueue.put(tile)
#     queueLock.release()
#     
#     # Wait for queue to empty
#     while not workQueue.empty():
#         pass
#     
#     # Notify threads it's time to exit
#     exitFlag = 1
#     
#     # Wait for all threads to complete
#     for t in threads:
#         t.join()
#     print ("Exiting Main Thread")
#     
    # Drop temporary views that reference the clipped extent
    if union_view:
        tiles_clipped.append(union_view)
    if tiles_clipped:
        config.drop_2Dtiles(dbase, USER_SCHEMA, views_to_drop=tiles_clipped)
         
#     # Delete temporary config files
#     yml_cfg = [os.path.join(CFG_DIR, t + "_config.yml") for t in threadList]
#     command = "rm"
#     for c in yml_cfg:
#         command = command + " " + c
#     call(command, shell=True)
#      
#      
#     #=========================================================================
#     # Reporting
#     #=========================================================================
#     tiles = set(tiles)
#     tiles_skipped = set(tiles_skipped)
#     print("\nTotal number of tiles processed: " +
#           str(len(tiles.difference(tiles_skipped))))
#     print("Total number of tiles skipped: " + str(len(tiles_skipped)))
#     print("Done.")

if __name__ == '__main__':
    main()
