# -*- coding: utf-8 -*-

"""Generate a 3D BAG data set"""

import os.path
from io import StringIO
from subprocess import call

from psycopg2 import sql


def create_heights_table(db, schema, table):
    """Create a postgres table that can store the content of 3dfier CSV-BUILDINGS-MULTIPLE
    
    Note
    ----
    The 'id' field is set to numeric because of the BAG 'identificatie' field.
    """

    schema_q = sql.Identifier(schema)
    table_q = sql.Identifier(table)
    query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        id numeric,
        "ground-0.00" real,
        "ground-0.10" real,
        "ground-0.20" real,
        "ground-0.30" real,
        "ground-0.40" real,
        "ground-0.50" real,
        "roof-0.00" real,
        "roof-0.10" real,
        "roof-0.25" real,
        "roof-0.50" real,
        "roof-0.75" real,
        "roof-0.90" real,
        "roof-0.95" real,
        "roof-0.99" real,
        ahn_file_date timestamptz
        );
    """).format(schema=schema_q, table=table_q)
    try:
        db.sendQuery(query)
        return(True)
    except:
        return(False)


def csv2db(db, cfg, args_in, out_paths):
    """Create a table with multiple height info per BAG building footprint
    
    Note
    ----
    Only for 3dfier's CSV-BUILDINGS-MULTIPLE output. Only works when the AHN3
    and BAG tiles are the same (same size and identifier)
    
    Parameters
    ----------
    db : db Class instance
    cfg: dict
        batch3dfier YAML config (output by parse_config_yaml() )
    args_in: dict
        batch3dfier command line arguments
    out_paths: list of strings
        Paths of the CSV files
    """
    schema_pc_q = sql.Identifier(cfg['elevation']['schema'])
    table_pc_q = sql.Identifier(cfg['elevation']['table'])
    field_pc_unit_q = sql.Identifier(cfg['elevation']['fields']['unit_name'])
    
    schema_out_q = sql.Identifier(cfg['out_schema'])
    table_out_q = sql.Identifier(cfg['out_table'])
    
    create_heights_table(db, cfg['out_schema'],
                                cfg['out_table'])
    with db.conn:
        with db.conn.cursor() as cur:
            tbl = ".".join([cfg['out_schema'], cfg['out_table']])
            for p in out_paths:
                csv_file = os.path.split(p)[1]
                fname = os.path.splitext(csv_file)[0]
                tile = fname.replace(cfg['prefix_tile_footprint'], '', 1)
                tile_q = sql.Literal(tile)
                
                query = sql.SQL("""SELECT file_date
                                    FROM {schema}.{table}
                                    WHERE {unit_name} = {tile};
                                """).format(schema=schema_pc_q,
                                           table=table_pc_q,
                                           unit_name=field_pc_unit_q,
                                           tile=tile_q)
                cur.execute(query)
                resultset = cur.fetchall()
                ahn_file_date = resultset[0][0]
                
                with open(p, "r") as f_in:
                    next(f_in) # skip header
                    for row in f_in:
                        f = StringIO(row + ahn_file_date.isoformat()) # trailing commas from the CSV (until #58 is fixed in 3dfier)
                        cur.copy_from(f, tbl, sep=',')

                # delete the CSV
                if args_in['rm']:
                    command = "rm" + p
                    call(command, shell=True)
                else:
                    pass
    db.sendQuery(
        sql.SQL("""CREATE INDEX IF NOT EXISTS {table}_id_idx
                ON {schema}.{table} (id);
                """.format(schema=schema_out_q,
                           table=table_out_q)
                )
    )
    db.sendQuery(
        sql.SQL("""COMMENT ON TABLE {schema}.{table} IS
                'Building heights generated with 3dfier.';
                """.format(schema=schema_out_q,
                           table=table_out_q)
                )
    )