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


def create_centroid_table(conn):
    """Creates a table of building footprint centroids in a standard BAG database

    Parameters
    ----------
    conn : psycopg2 Connection object
        

    Returns
    -------
    nothing

    """
    
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE bagactueel.pand_centroid AS
                SELECT gid, st_centroid(geovlak)::geometry(point, 28992) AS geom
                FROM bagactueel.pandactueelbestaand;
            
            SELECT populate_geometry_columns('bagactueel.pand_centroid'::regclass);
            
            CREATE
                INDEX pand_centroid_geom_idx ON
                bagactueel.pand_centroid
                    USING gist(geom);
            """)
    conn.commit()
    
    with conn.cursor() as cur:
        cur.execute("VACUUM ANALYZE bagactueel.pand_centroid;")
    conn.commit()
    
    
    
def bagtiler(conn):
    """Creates the BAG tiles (or Views) in the bag_tiles schema.

    Parameters
    ----------
    conn : psycopg2 Connection object
        

    Returns
    -------
    nothing

    """
    with conn.cursor() as cur:
        # Create schema to store the tiles
        schema = sql.Identifier("bag_tiles")
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(schema))
        conn.commit()
        
        
        # Get AHN3 tile IDs
        cur.execute("SELECT unit FROM ahn3.ahn_units;")
        tiles = [i[0] for i in cur.fetchall()]
        
        
        # Create a BAG tile with equivalent area of an AHN tile
        for tile in tiles:
            # the 't_' prefix is hard-coded in config.call3dfier()
            view = sql.Identifier('t_' + tile)
            cur.execute(
                sql.SQL("""CREATE OR REPLACE VIEW {schema}.{view} AS
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
                                a.unit = %s
                                AND(
                                    st_containsproperly(
                                        a.geom,
                                        c.geom
                                    )
                                    OR st_contains(
                                        a.geom_border,
                                        c.geom
                                    )
                                );""").format(schema=schema, view=view), [tile])
        try:
            conn.commit()
            print("%s BAG tiles created in schema 'bag_tiles'." % (len(tiles)))
        except:
            conn.rollback()
            print("Couldn't create BAG tiles in schema 'bag_tiles'. Rolling back transaction.")
