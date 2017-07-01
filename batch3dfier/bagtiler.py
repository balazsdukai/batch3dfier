"""
/***************************************************************************
 batch3dfier
 
        begin                : 2017-06-20
        copyright            : (C) 2017 by Balázs Dukai, TU Delft
        email                : balazs.dukai@gmail.com
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 3 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

#===============================================================================
# Batch create views for the 2D(BAG) tiles.
# Before running this script, make sure the database is set up according to
# prep_bag_tiles.sql
#===============================================================================

from psycopg2 import sql
import db

def create_tile_edges(db):
    """Update tiles to include the lower/left boundary
    """
    db.sendQuery("""ALTER TABLE ahn3.ahn_units
             ADD COLUMN geom_border geometry;""")
    db.sendQuery("""
            UPDATE
                ahn3.ahn_units
            SET
                geom_border = b.geom::geometry(linestring,28992)
            FROM
                (
                    SELECT
                        id,
                        st_setSRID(
                            st_makeline(
                                ARRAY[st_makepoint(
                                    st_xmax(geom),
                                    st_ymin(geom)
                                ),
                                st_makepoint(
                                    st_xmin(geom),
                                    st_ymin(geom)
                                ),
                                st_makepoint(
                                    st_xmin(geom),
                                    st_ymax(geom)
                                ) ]
                            ),
                            28992
                        ) AS geom
                    FROM
                        ahn3.ahn_units
                ) b
            WHERE
                ahn3.ahn_units.id = b.id;
            """)
    
    db.sendQuery("""
            CREATE INDEX units_geom_border_idx ON ahn3.ahn_units USING gist (geom_border);
            SELECT populate_geometry_columns('ahn3.ahn_units'::regclass);
            """)
    
    db.conn.commit()
    
    db.vacuum(schema = "ahn3", table = "ahn_units")
 

def create_centroid_table(db):
    """Creates a table of building footprint centroids in a standard BAG database

    Parameters
    ----------
    conn : psycopg2 Connection object
        

    Returns
    -------
    boolean
        indicating success/failure

    """
    
    db.sendQuery("""
        CREATE TABLE bagactueel.pand_centroid AS
            SELECT gid, st_centroid(geovlak)::geometry(point, 28992) AS geom
            FROM bagactueel.pandactueelbestaand;
        
        SELECT populate_geometry_columns('bagactueel.pand_centroid'::regclass);
        
        CREATE
            INDEX pand_centroid_geom_idx ON
            bagactueel.pand_centroid
                USING gist(geom);
        """)
    
    db.conn.commit()
    
    db.vacuum("bagactueel", "pand_centroid")
    
def bagtiler(conn):
    """Creates the BAG tiles (or Views) in the bag_tiles schema.

    Parameters
    ----------
    conn : psycopg2 Connection object
        

    Returns
    -------
    boolean
        indicating success/failure

    """
    # Create schema to store the tiles
    schema = sql.Identifier("bag_tiles")
    query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(schema)
    db.sendQuery(query)
        
            # Get AHN3 tile IDs
    tiles = db.getQuery("SELECT unit FROM ahn3.ahn_units;")
    tiles = [i[0] for i in tiles]
        
        
    # Create a BAG tile with equivalent area of an AHN tile
    for tile in tiles:
        # the 't_' prefix is hard-coded in config.call3dfier()
        view = sql.Identifier('t_' + tile)
        query = sql.SQL("""CREATE OR REPLACE VIEW {schema}.{view} AS
                        SELECT
                            b.gid,
                            b.identificatie,
                            b.geovlak
                        FROM
                            bagactueel.pand b
                        INNER JOIN bagactueel.pand_centroid c ON
                            b.gid = c.gid,
                            ahn3.ahn_units a
                        WHERE
                            a.unit = {tile}
                            AND(
                                st_containsproperly(
                                    a.geom,
                                    c.geom
                                )
                                OR st_contains(
                                    a.geom_border,
                                    c.geom
                                )
                        );""").format(schema=schema, view=view, tile=tile)
        db.sendQuery(query)
    
    db.conn.commit()

    print("%s BAG tiles created in schema 'bag_tiles'." % (len(tiles)))
    print("Couldn't create BAG tiles in schema 'bag_tiles'. Rolling back transaction.")

