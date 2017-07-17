# -*- coding: utf-8 -*-

"""Batch create views for the 2D(BAG) tiles.

The module helps you to create tiles in a BAG (https://www.kadaster.nl/wat-is-de-bag)
database. These tiles are then used by batch3dfier.
"""

from psycopg2 import sql


def create_tile_edges(db):
    """Update tiles to include the lower/left boundary

    Parameters
    ----------
    db : db Class instance
        

    Returns
    -------
    nothing

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
    db : db Class instance
        

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
    
def bagtiler(db):
    """Creates the BAG tiles (or Views) in the bag_tiles schema.

    Parameters
    ----------
    db : db Class instance
        

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
        tile = sql.Literal(tile)
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
    
    try:
        db.conn.commit()
        print("%s BAG tiles created in schema 'bag_tiles'." % (len(tiles)))
    except:
        db.conn.rollback()
        print("Cannot create BAG tiles in schema 'bag_tiles'")

