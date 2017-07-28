========================================
batch3dfier: 3dfiying data sets in batch
========================================

.. Contents::

Introduction
============

A few notes on terminology...
-----------------------------

The term BAG refers to the Dutch official data set of buildings and addresses (`Basisregistraties Adressen en Gebouwen <https://www.kadaster.nl/basisregistratie-gebouwen>`__).

The term AHN refers to the Dutch, country-wide elevation dataset (pointcloud) (`Actueel Hoogtebestand Nederland <http://www.ahn.nl/>`__).

The term *footprints* refers to a 2D polygon data set that is to be 3dfied. I used the BAG.

A *tile index* is data set of polygons that tesselates the footprints or the pointcloud data. A *tile index unit* is one polygon in the *tile index* and it usually has an *ID* (e.g. 1) and a *name* (e.g. 25gn1_a) that uniquely identifies it. Yes, both *ID* and *name* do the same thing, uniquely identify a polygon thus one of them is redundant. However, *ID* can serve as a primary key while *name* as a human friendly identifier for quickly locating tiles. This concept is used by the `AHN tile index <http://www.ahn.nl/binaries/content/assets/ahn-nl/downloads/ahn_units.zip>`__, which served as an example for development.

For an example, see the image below or ``/example_data/ahn_index.geojson``, ``/example_data/bag_index.geojson``.

.. image:: https://github.com/balazsdukai/batch3dfier/blob/release/0.5.0/doc/tile_index.png
   :align: center

In *batch3dfier* the term *tile* refers to a data subset (footprints or pointcloud), that falls within the limits of a *tile index unit*. A footprint belongs to a tile, if its centroid is in the interior of a tile index unit or on the left/lower edge of a tile index unit. Therefore a footprint cannot belong to two tiles at the same time, thus there won't be overlaps between two neighbouring tiles.


Preparing the tiles
===================

In order to be able to 3dfy any size of data set, *batch3dfier* processes the input piece by piece (or tile by tile). Therefore the bottleneck becomes the size of a single footprint and pointcloud tile. Both tiles need to be *small* enough, so that *3dfier* can process them.


Setting up the footprint tiles in a database
--------------------------------------------

Let's take the example of a tiny subset of the BAG data set. In this case the two tile indexes and the footprints are stored in a separate schema. If you have a large data set (like BAG) with many tables and views, this helps to keep things organised.

.. code-block:: sh

        # $ /batch3dfier/db_setup.sh
        
        # or manually:
        
        $ psql -d postgres -c "create role batch3dfier with login password 'batch3d_test';"
        $ createdb -O batch3dfier batch3dfier_test 
        $ psql -d batch3dfier_test -c "create extension postgis;\
                                     create schema tile_index authorization batch3dfier;\
                                     create schema bag authorization batch3dfier;"
        
        $ ogr2ogr -f PostgreSQL PG:"dbname=batch3dfier_test\
         host=localhost port=5432 user=batch3dfier password=batch3d_test"\
         ./example_data/bag_index.geojson -nln bag_index\
         -skip-failure -a_srs EPSG:28992 -lco FID=gid -lco GEOMETRY_NAME=geom\
         -lco SCHEMA=tile_index
         
        $ ogr2ogr -f PostgreSQL PG:"dbname=batch3dfier_test\
         host=localhost port=5432 user=batch3dfier password=batch3d_test"\
         ./example_data/ahn_index.geojson -nln ahn_index\
         -skip-failure -a_srs EPSG:28992 -lco FID=gid -lco GEOMETRY_NAME=geom\
         -lco SCHEMA=tile_index
         
        $ ogr2ogr -f PostgreSQL PG:"dbname=batch3dfier_test\
         host=localhost port=5432 user=batch3dfier password=batch3d_test"\
         ./example_data/bag_pand.geojson -nln pand\
         -skip-failure -a_srs EPSG:28992 -lco FID=gid -lco GEOMETRY_NAME=geom\
         -lco SCHEMA=bag

Once you have a database up and running and loaded all the data, you can partition it with the `footprints` module.

.. code-block:: python

        from batch3dfier import db
        from batch3dfier import footprints
        
        dbs = db.db(dbname='batch3dfier_test', host='localhost', port='5432',
                    user= 'batch3dfier', password='batch3d_test')
        
        bag_index = ['tile_index', 'bag_index']
        bag_fields = ['gid', 'geom', 'unit']
        table_footprint = ['bag', 'pand']
        fields_footprint = ['gid', 'geom', 'identification']
        schema_tiles = 'bag_tiles'
        prefix_tiles = 't_'
          
        footprints.partition(dbs, schema_tiles=schema_tiles, table_index=bag_index,
                             fields_index=bag_fields, table_footprint=table_footprint,
                             fields_footprint=fields_footprint,
                             prefix_tiles=prefix_tiles)

This will create a database view for each footprint tile in ``bag_index``, such as:

    ::
    
                          List of relations
          Schema   |    Name    | Type |    Owner    
        -----------+------------+------+-------------
         bag_tiles | t_25gn1_c1 | view | batch3dfier
         bag_tiles | t_25gn1_c2 | view | batch3dfier
         bag_tiles | t_25gn1_c3 | view | batch3dfier
         bag_tiles | t_25gn1_c4 | view | batch3dfier
        (4 rows)

Where the name of the view is ``prefix_tiles`` + the value in field ``unit``. ``prefix_tiles`` can be ``None``.

Then *batch3dfier* will use (the content of) these views as input. 

Using batch3dfier
=================

The YAML configuration file
---------------------------

Following the convention of *3dfier*, *batch3dfier* also uses a YAML configuration file. You'll find a template at ``/batch3dfier/batch3dfier_config.yml``.

-   Database access

    ::
    
        input_polygons:
            database:
                dbname: batch3dfier_test
                host: localhost
                port: 5432
                user: batch3dfier
                pw: batch3d_test

-   Name of the schema that contains the footprint tile views. In case the user has no CREATE and DROP privilege on ``tile_schema``, in ``user_schema`` you can provide a schema where it has. Only relevant when ``extent`` is provided.

    ::
    
        tile_schema: bag_tiles 
        user_schema:
        
-   Prefix prepended to the footprint tile view names. If blank, its assumed that the views are named as the values in field referenced by ``tile_index:elevation:fields:unit_name``

    ::
    
        tile_prefix: t_
    
-   Name of the field in the views in ``tile_schema`` that uniquely identifies a footprint.

    ::
    
        uniqueid: identification
    
There are two options to tell batch3dfier what to extrude:

1. provide a polygon for the area

   ::

       input_polygons:
           extent: path/to/polygon

2. give a list of 2D tile IDs

   ::

       input_polygons:
           tile_list: [t_25gn1_c1, t_25gn1_c2]
           
3. process all tiles found in ``tile_index: polygons: fields: unit_name:``

   ::

       input_polygons:
           tile_list: [all]
           

-   *batch3dfier* searches a directory to find the pointcloud file(s) that match a given tile in the pointcloud tile index. The match between the file name and the tile index unit name is strict, the tile index unit name has to be part of the file name. This feature is handy when you have hundreds or thousands of pointcloud files (e.g. AHN).

    ::
   
        input_elevation:
            dataset_dir: /batch3dfier/example_data
   
-   Naming convention for the pointcloud files, where tile_case controls how the string matching is done for {tile} in order to find the ``input_elevation`` files in ``dataset_dir``. Allowed are options are:

    -   'upper' (e.g. C_25GN1_filtered.LAZ),
    -   'lower' (e.g. C_25gn1_filtered.LAZ),
    -   'mixed' (e.g. C_25Gn1_filtered.LAZ). In case of 'mixed', the values in ``tile_index: elevation: fields: unit_name`` should match exactly the {tile} in dataset_name.
    
    ::
   
        dataset_name: c_{tile}.laz # naming convention for the pointcloud files
        tile_case: lower
    
-   Both the footprint and pointcloud tile indexes are expected to be in the database.

    ::
    
        tile_index:
            polygons:
                # schema, table that stores the tile extent/index polygons and IDs
                schema: tile_index
                table: bag_index
                fields:
                    primary_key: gid # name of the primary key field in bag_units
                    geometry: geom # name of the geometry field in bag_units
                    unit_name: unit # name of the field of the tile index unit names
            elevation: 
                schema: tile_index
                table: ahn_index
                fields:
                    primary_key: gid
                    geometry: geom
                    unit_name: unit

-   Output format for the 3dfied tiles, and the directory where to put them.

    ::
    
        output:
            format: OBJ 
            dir: /Data/3DBAG

-   Location of the *3dfier* executable.

    ::
    
        path_3dfier: opt/3dfier/build/3dfier 


Run
---

-   Run *batch3dfier* from the command line:

    ``batch3dfy ./batch3dfier_config.yml``

    Where ``batch3dfier_config.yml`` is the YAML configuration file that *batch3dfier* uses (similarly to *3dfier*).

-   Get help:

    ``batch3dfy -h``

-   In order to process several tiles efficiently *batch3dfier* starts 3 concurrent threads by default, each of them processing a single tile at a time. Set the number of threads:

    ``batch3dfy -t 4 ./batch3dfier_config.yml``

Contact/Contributing
====================

-   Contact

You can send me an e-mail at balazs.dukai AT gmail DOT com (please head your subject with [batch3dfier]).

-   Contributing

The development is still in early stages, thus things can change drastically. Nevertheless, issues, comments, pull request are very welcome. Take a look at `the issue on v1.0.0 <https://github.com/balazsdukai/batch3dfier/issues/1>`__ if you want a hint where to start.

Testing, testing, testing. Always appreciated.





