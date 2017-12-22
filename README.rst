===========
batch3dfier
===========

|Licence| |Python 3.4+| |build| |codecov|

.. contents:: :local:

Description
===========

This Python program helps you to create 3D models of LoD1 from massive data sets. The idea for the program came from the need to efficiently create a 3D model for the whole Netherlands, using building footprints from the `BAG <https://www.kadaster.nl/basisregistratie-gebouwen>`__ dataset and the `AHN <http://www.ahn.nl/>`__ pointcloud.

The software `3dfier <https://github.com/tudelft3d/3dfier>`__ is the heart of *batch3dfier*, where *batch3dfier* adds a tiling framework, allowing to scale to an arbitrary size of input data set.

As of version 0.7.0 *batch3dfier*:

-   Can 3dfy 2D footprints (polygons) that are on a single *layer*, in this case, a single database table (e.g. building footprints).

-   Its ``footprints`` module helps in partitioning a large footprints data set into tiles. Only rectangular tiles are handled.

-   3dfies the footprints tile per tile, running concurrent threads. And as input it accepts,

    -   a polygon of the desired extent,
    
    -   a list of footprint tile IDs,
    
    -   ``all`` to process all tiles in the footprint tile index.

-   Expects that the footprints, the *tile indexes* of footprints and pointcloud are stored in a PostgreSQL database, the pointcloud stored in files.

- In case of CSV-BUILDINGS-MULTIPLE output format, it can copy the CSV contents into postgres, thus the different buildings heights can be joined on the footprints.

-   Has been only tested with *BAG* and *AHN* data sets.

-   It does not mirror the complete feature set of *3dfier*. Apart from those listed above, the most notable differences are:

    -   3dfied tiles are not stitched together to create a 'watertight' model. 
    
    -   Some of the *3dfier* configuration parameters are not included in the *batch3dfier* configuration file. Mainly because there was no need for them. If you notice that something is missing, let me know in an issue or submit a pull request.
    
To get further information please refer to the `documentation <https://github.com/balazsdukai/batch3dfier/tree/master/docs/batch3dfier.rst>`_.

Plans for version 1.0.0
-----------------------

See `the related issue <https://github.com/balazsdukai/batch3dfier/issues/1>`__.


Requirements
============

`3dfier <https://github.com/tudelft3d/3dfier>`__

Python 3.4+

The package has been tested with Python3.4-3.6 on Linux with the following packages:

-  PyYAML (3.11)
-  psycopg2 (2.7)
-  Fiona (1.7.1)
-  Shapely (1.5.17)


Install and run
===============

-   Download and install the latest release:

    ``$ pip3 install git+https://github.com/balazsdukai/batch3dfier``

-   Run *batch3dfier* from the command line:

    ``$ batch3dfy ./batch3dfier_config.yml``

    Where ``batch3dfier_config.yml`` is the YAML configuration file that *batch3dfier* uses (similarly to *3dfier*).

-   Get help:

    ``$ batch3dfy -h``

-   In order to process several tiles efficiently *batch3dfier* starts 3  concurrent threads by default, each of them processing a single tile at a time. Set the number of threads:

    ``$ batch3dfy -t 4 ./batch3dfier_config.yml``
    
-   Run the tests:

    ``$ pytest batch3dfier/tests``
    
Example data sets are in ``batch3dfier/example_data``.



.. |Licence| image:: https://img.shields.io/badge/licence-GPL--3-blue.svg
   :target: http://www.gnu.org/licenses/gpl-3.0.html
.. |Python 3.4+| image:: https://img.shields.io/badge/python-3.4+-blue.svg
.. |build| image:: https://travis-ci.org/balazsdukai/batch3dfier.svg?branch=master
   :target: https://travis-ci.org/balazsdukai/batch3dfier
.. |codecov| image:: https://codecov.io/gh/balazsdukai/batch3dfier/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/balazsdukai/batch3dfier



