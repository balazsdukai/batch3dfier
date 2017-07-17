# -*- coding: utf-8 -*-

"""Batch create views for the 2D(BAG) tiles.

The module helps you to create tiles in a BAG (https://www.kadaster.nl/wat-is-de-bag)
database. These tiles are then used by batch3dfier.
"""

from psycopg2 import sql


def create_tile_edges(db, schema, table, id_col, geom_col):
    """Update tiles to include the lower/left boundary

    Parameters
    ----------
    db : db Class instance
    schema : str
        Name of the schema that contains the tile polygons.
    table : str
        Name of the table in schema that contains the tile polygons.
    id_col : str
        The ID field in table.
    geom_col : str
        The geometry field in table.

    Returns
    -------
    nothing

    """
    schema_q = sql.Identifier(schema)
    table_q = sql.Identifier(table)
    geom_col_q = sql.Identifier(geom_col)
    id_col_q = sql.Identifier(id_col)
    db.sendQuery(sql.SQL("""ALTER TABLE {}.{}
             ADD COLUMN geom_border geometry;""").format(schema_q, table_q))
    db.sendQuery(
        sql.SQL("""
            UPDATE
                {schema}.{table}
            SET
                geom_border = b.geom::geometry(linestring,28992)
            FROM
                (
                    SELECT
                        {id_col},
                        st_setSRID(
                            st_makeline(
                                ARRAY[st_makepoint(
                                    st_xmax({geom_col}),
                                    st_ymin({geom_col})
                                ),
                                st_makepoint(
                                    st_xmin({geom_col}),
                                    st_ymin({geom_col})
                                ),
                                st_makepoint(
                                    st_xmin({geom_col}),
                                    st_ymax({geom_col})
                                ) ]
                            ),
                            28992
                        ) AS geom
                    FROM
                        {schema}.{table}
                ) b
            WHERE
                {schema}.{table}.{id_col} = b.{id_col};
            """).format(schema=schema_q, table=table_q, geom_col=geom_col_q,
                        id_col=id_col_q))
    sql_query = sql.SQL("""
            CREATE INDEX units_geom_border_idx ON {schema}.{table} USING gist (geom_border);
            SELECT populate_geometry_columns({}::regclass);
            """).format(sql.Literal(schema + '.' + table),
                         schema=schema_q, table=table_q)
    db.sendQuery(sql_query)
    
    db.vacuum(schema, table)


def create_centroid_table(db, schema, table_centroid, table_poly, id_col, geom_col):
    """Creates a table of building footprint centroids in a standard BAG database

    Parameters
    ----------
    db : db Class instance
    schema : str
        Name of the schema that contains the 2D polygons.
    table_centroid : str
        Name of the new table that contains the 2D polygon centroids.
    table_poly : str
        Name of the table of the 2D polygons.
    geom_col : str
        The geometry field in table_poly.

    Returns
    -------
    boolean
        indicating success/failure

    """
    schema_q = sql.Identifier(schema)
    table_centroid_q = sql.Identifier(table_centroid)
    table_poly_q = sql.Identifier(table_poly)
    geom_col_q = sql.Identifier(geom_col)
    id_col_q = sql.Identifier(id_col)
    
    sql_query = sql.SQL("""
        CREATE TABLE {schema}.{table_centroid} AS
            SELECT {id_col}, st_centroid({geom_col})::geometry(point, 28992) AS geom
            FROM {schema}.{table_poly};
        
        SELECT populate_geometry_columns({sch_tbl}::regclass);
        
        CREATE
            INDEX {tbl_idx} ON
            {schema}.{table_centroid}
                USING gist(geom);
        """).format(schema=schema_q,
                    table_centroid=table_centroid_q,
                    geom_col=geom_col_q,
                    id_col=id_col_q,
                    table_poly=table_poly_q,
                    sch_tbl=sql.Literal(schema + '.' + table_centroid),
                    tbl_idx=sql.Identifier(table_centroid + '_geom_idx')
                    )
        
    db.sendQuery(sql_query)
    
    db.vacuum(schema, table_centroid)


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

