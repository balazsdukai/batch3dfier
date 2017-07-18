batch3dfier
===========

|Licence| |Python 3.5|

This package is a wrapper around the
`3dfier <https://github.com/tudelft3d/3dfier>`__ software and helps you
3dfy datasets in batch. The idea for the package came from the need to
efficiently create a 3D model for the whole Netherlands, using building
footprints from the
`BAG <https://www.kadaster.nl/basisregistratie-gebouwen>`__ dataset and
the `AHN <http://www.ahn.nl/>`__ pointcloud.

Therefore currently some database related parameters (e.g. field names)
are hard-coded for a BAG database that has been created from PostgreSQL
dump found at http://data.nlextract.nl/bag/postgis/.

Setting up a BAG database
-------------------------

As noted above, I used the PostgreSQL dump provided at
http://data.nlextract.nl/bag/postgis/. The dump was restored with the
command:

``pg_restore --no-owner --no-privileges -d bag bag-laatst.backup``

After, a *tile index* of the 2D footprints needs to be loaded into the database.
For example:

   ::
   
      ogr2ogr -f PostgreSQL PG:"dbname=batch3dfier_testing active_schema=tile_index\
       host=localhost port=5432 user=batch3dfier_tester password=batch3d_test"\
       ./data/bag_index.geojson -nln bag_index\
       -skip-failure -a_srs EPSG:28992 -lco FID=gid -lco GEOMETRY_NAME=geom

A *tile index* is a dataset of rectangular polygons (or tiles) that partition the 2D footprints dataset.
The ``footprints`` module helps you to generate database *views* for each tile in *tile_index*.
These *views* then serve as an input for *3dfier*.
Usage:

   ::
   
      bag_index = ['tile_index', 'bag_index']
      bag_fields = ['gid', 'geom', 'unit']
      table_footprint = ['bagactueel', 'pand']
      fields_footprint = ['gid', 'geovlak', 'identificatie']
      schema_tiles = 'bag_tiles'
      prefix_tiles = 't_'
      
      footprints.partition(dbs, schema_tiles=schema_tiles, table_index=bag_index,
                           fields_index=bag_fields, table_footprint=table_footprint,
                           fields_footprint=fields_footprint,
                           prefix_tiles=prefix_tiles)


Install and run
---------------

Download and install the latest release:

``pip3 install git+https://github.com/balazsdukai/batch3dfier``

+ run *batch3dfier* from the command line:

``batch3dfy ./batch3dfier_config.yml``

Where ``batch3dfier_config.yml`` is the YAML configuration file that
*batch3dfier* uses (similarly to *3dfier*).

+ get help:

``batch3dfy -h``

+ set the number of concurrent threads (deafult=3):

``batch3dfy -t 4 ./batch3dfier_config.yml``

In the YAML file there are two options to tell batch3dfier what to
extrude:

1. provide a polygon for the area

   ::

       input_polygons:
           extent: path/to/polygon

2. give a list of 2D tile IDs

   ::

       input_polygons:
           tile_list: 25gn1, 25ez1

Not all of the *3dfier* configuration options are mirrored in the
*batch3dfier* configuration file. If you notice that something is
missing that you need, add it manually in the source of
``config.yamlr()``.

A few design details
--------------------

2D polygons are stored in PostgreSQL, pointcloud in files.

A spatial dataset that covers a whole country can be rather large to
3dfy in one go, the BAG is no exception. Therefore it is partitioned
into rectangular tiles, where a tile is stored as a PostgreSQL *view* of
the polygons within the partition. Physically, a partition is a polygon
and a corresponding ID, stored in second dataset which is referred to as
the *tile index*.

The *view* names composed as *t\_<tile ID>*.

Currently, *batch3dfier* matches the 2D tiles to the pointcloud tiles on
their ID. This is because the `AHN tile
index <http://www.ahn.nl/binaries/content/assets/ahn-nl/downloads/ahn_subunits.zip>`__
was used to partition the 2D dataset.

*batch3dfier* tries to find the matching poincloud file for the 2D tile
in the provided directory (``dataset_dir:``) and it skips the tile if it
cannot find the file.

The diagram below tries to illustrate how the tile, view, file and
variable names relate to each other. Variable names are always the last,
e.g. ``tile_views`` or ``pc_tile``.

::

                Organization of BAG and AHN tiles

                         +-------------+
                         | AHN tile ID |
                         |      =      |
              +----------+   tile ID   +--------+
              |          |   (25gn1)   |        |
              |          |      =      |        |
              |          |    tiles    |        |
              |          +-------------+        |
              |                                 |
    +---------v---------+               +-------v-------+
    |    2D tile name   |               | AHN file name |
    |          =        |               | (C_25GN1.LAZ) |
    | 2D tile View name |               |       =       |
    |      (t_25gn1)    |               |    pc_tile    |
    |          =        |               |               |
    |      tile_views   |               +---------------+
    |                   |
    +---------+---------+
              |
              |
      +-------v--------+
      |output file name|
      |       =        |
      |    tile_out    |
      |                |
      +----------------+

In order to process several tiles efficiently *batch3dfier* starts 3
concurrent threads, each of them processing a single tile at a time.

Requirements
------------

Python 3

The package has been tested only on Linux with the following packages:

-  PyYAML (3.11)
-  psycopg2 (2.7)
-  Fiona (1.7.1)
-  Shapely (1.5.17)

.. |Licence| image:: https://img.shields.io/badge/licence-GPL--3-blue.svg
   :target: http://www.gnu.org/licenses/gpl-3.0.html
.. |Python 3.5| image:: https://img.shields.io/badge/python-3.5-blue.svg

