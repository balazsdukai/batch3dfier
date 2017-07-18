# -*- coding: utf-8 -*-

"""Batch create views for the 2D(BAG) tiles.

The module helps you to create tiles in a BAG (https://www.kadaster.nl/wat-is-de-bag)
database. These tiles are then used by batch3dfier.
"""
from psycopg2 import sql


def create_tile_edges(db, table_index, fields_index):
    """Update the tile index to include the lower/left boundary of each polygon.
    
    The function is mainly relevant for the tile index of the footprints. 
    The tile edges are then used for checking centroid containment in a tile polygon.

    Parameters
    ----------
    db : db Class instance
    table_index : tuple of str
        (schema, table) that contains the tile index polygons.
    fields_index: tuple of str
        (ID, geometry) field names of the ID and geometry fields in table_index.

    Returns
    -------
    nothing

    """
    schema = table_index[0]
    table = table_index[1]
    id_col = fields_index[0]
    geom_col = fields_index[1]
    
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
                """).format(schema=schema_q,
                            table=table_q,
                            geom_col=geom_col_q,
                            id_col=id_col_q)
                 )
    
    sql_query = sql.SQL("""
            CREATE INDEX {idx_name} ON {schema}.{table} USING gist (geom_border);
            SELECT populate_geometry_columns({name}::regclass);
            """).format(idx_name=sql.Identifier(table + "_" + geom_col + "_border_idx"),
                        schema=schema_q,
                        table=table_q,
                        name=sql.Literal(schema + '.' + table))
    db.sendQuery(sql_query)
    
    db.vacuum(schema, table)


def create_centroid_table(db, table_centroid, table_footprint, fields_footprint):
    """Creates a table of footprint centroids.
    
    The table_centroid is then used by bagtiler().

    Parameters
    ----------
    db : db Class instance
    table_centroid : tuple of str
        (schema, table) for the new relation that contains the footprint centroids.
    table_footprint : tuple of str
        (schema, table) of the footprints (e.g. building footprints) that will be extruded.
    fields_footprint : tuple of str
        (ID, geometry) field names of the ID geometry fields in table_footprint.

    Returns
    -------
    nothing

    """
    schema_ctr = table_centroid[0]
    table_ctr = table_centroid[1]
    schema_poly = table_footprint[0]
    table_poly = table_footprint[1]
    id_col = fields_footprint[0]
    geom_col = fields_footprint[1]
    
    schema_ctr_q = sql.Identifier(schema_ctr)
    table_ctr_q = sql.Identifier(table_ctr)
    schema_poly_q = sql.Identifier(schema_poly)
    table_poly_q = sql.Identifier(table_poly)
    geom_col_q = sql.Identifier(geom_col)
    id_col_q = sql.Identifier(id_col)
    
    sql_query = sql.SQL("""
        CREATE TABLE {schema_ctr}.{table_ctr} AS
            SELECT {id_col}, st_centroid({geom_col})::geometry(point, 28992) AS geom
            FROM {schema_poly}.{table_poly};
        
        SELECT populate_geometry_columns({sch_tbl}::regclass);
        
        CREATE
            INDEX {tbl_idx} ON
            {schema_ctr}.{table_ctr}
                USING gist(geom);
        """).format(schema_ctr=schema_ctr_q,
                    table_ctr=table_ctr_q,
                    id_col=id_col_q,
                    geom_col=geom_col_q,
                    schema_poly=schema_poly_q,
                    table_poly=table_poly_q,
                    sch_tbl=sql.Literal(schema_ctr + '.' + table_ctr),
                    tbl_idx=sql.Identifier(table_ctr + '_geom_idx')
                    )
        
    db.sendQuery(sql_query)
    
    db.vacuum(schema_ctr, table_ctr)


def bagtiler(db, schema_tiles, tile_index, centroid_table):
    """Creates the BAG tiles (or Views).

    Parameters
    ----------
    db : db Class instance
    schema_tiles : str 
        Name of the schema where to create the tiles.
    tile_index : tuple
        (schema, table, field) that contains the tile index.
    centroid_table : tuple
        (schema, table) that contains the footprint centroids.

    Returns
    -------
    nothing

    """
    # Create schema to store the tiles
    schema_tiles_q = sql.Identifier(schema_tiles)
    query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(schema_tiles_q)
    db.sendQuery(query)
        
    # Get AHN3 tile IDs
    schema_idx_q = sql.Identifier(tile_index[0])
    table_idx_q = sql.Identifier(tile_index[1])
    field_idx_q =sql.Identifier(tile_index[2])
    tiles = db.getQuery(
        sql.SQL("SELECT {} FROM {}.{};").format(field_idx_q, schema_idx_q,
                                                table_idx_q)
                        )
    tiles = [i[0] for i in tiles]
    
    schema_ctr_q = sql.Identifier(centroid_table[0])
    table_ctr_q = sql.Identifier(centroid_table[1])
        
    # Create a BAG tile with equivalent area of an AHN tile
    for tile in tiles:
        # the 't_' prefix is hard-coded in config.call3dfier()
        view = sql.Identifier('t_' + tile)
        tile = sql.Literal(tile)
        query = sql.SQL("""CREATE OR REPLACE VIEW {schema_tiles}.{view} AS
                        SELECT
                            b.gid,
                            b.identificatie,
                            b.geovlak
                        FROM
                            bagactueel.pand b
                        INNER JOIN {schema_ctr}.{table_ctr} c ON
                            b.gid = c.gid,
                            {schema_idx}.{table_idx} a
                        WHERE
                            a.{field_idx} = {tile}
                            AND(
                                st_containsproperly(
                                    a.geom,
                                    c.geom
                                )
                                OR st_contains(
                                    a.geom_border,
                                    c.geom
                                )
                        );""").format(schema_tiles=schema_tiles_q, view=view,
                                      tile=tile, schema_idx=schema_idx_q,
                                      table_idx=table_idx_q,
                                      field_idx=field_idx_q,
                                      schema_ctr=schema_ctr_q,
                                      table_ctr=table_ctr_q)
        db.sendQuery(query)
    
    # db.conn.commit() is totally redundant
    try:
        db.conn.commit()
        print("%s BAG tiles created in schema '%s'." % (len(tiles), schema_tiles))
    except:
        db.conn.rollback()
        print("Cannot create BAG tiles in schema '%s'" % schema_tiles)

