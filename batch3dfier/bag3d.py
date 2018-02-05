#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Generate a 3D BAG data set. Linux only."""

import os.path
from subprocess import run

import os.path as path
from os import walk
import argparse
from psycopg2 import sql
import datetime

from batch3dfier.batch3dfierapp import parse_config_yaml


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
        print("Created heights table")
        return(True)
    except:
        return(False)


def csv2db(db, cfg, out_paths):
    """Create a table with multiple height info per BAG building footprint
    
    Note
    ----
    Only for 3dfier's CSV-BUILDINGS-MULTIPLE output. 
    Only works when the AHN3 and BAG tiles are the same (same size and identifier). 
    Only Linux.
    Alter the CSV files by adding the ahn_file_date field and values.
    
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
    
    table_idx = sql.Identifier(cfg['out_schema'] + "_id_idx")
    
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
                # the AHN3 file creation date that is stored in the tile index
                ahn_file_date = resultset[0][0].isoformat()
                
                # Need to do some linux text-fu so that the whole csv file can
                # be imported with COPY instead of row-wise edit and import
                # in python (suuuper slow)
                # Watch out for trailing commas from the CSV (until #58 is fixed in 3dfier)
                command = "gawk -i inplace -F',' \
                'BEGIN { OFS = \",\" } {$16=\"%s\"; print}' %s" % (ahn_file_date, p)
                run(command, shell=True)
                command = "sed -i '1s/.*/id,ground-0.00,ground-0.10,ground-0.20,\
                ground-0.30,ground-0.40,ground-0.50,roof-0.00,roof-0.10,\
                roof-0.25,roof-0.50,roof-0.75,roof-0.90,roof-0.95,roof-0.99,\
                ahn_file_date/' %s" % p
                run(command, shell=True)
                
                with open(p, "r") as f_in:
                    next(f_in) # skip header
                    cur.copy_from(f_in, tbl, sep=',')
                    
    db.sendQuery(
        sql.SQL("""CREATE INDEX {table}
                ON {schema_q}.{table_q} (id);
                """).format(schema_q=schema_out_q,
                            table_q=table_out_q,
                            table=table_idx)
    )
    db.sendQuery(
        sql.SQL("""COMMENT ON TABLE {schema}.{table} IS
                'Building heights generated with 3dfier.';
                """).format(schema=schema_out_q,
                           table=table_out_q)
    )


def create_bag3d_relations(db):
    """Creates the necessary postgres tables and views for the 3D BAG"""
    
    query = sql.SQL("""
    CREATE TABLE bagactueel.bag3d AS
    SELECT
        p.gid,
        p.identificatie,
        p.aanduidingrecordinactief,
        p.aanduidingrecordcorrectie,
        p.officieel,
        p.inonderzoek,
        p.documentnummer,
        p.documentdatum,
        p.pandstatus,
        p.bouwjaar,
        p.begindatumtijdvakgeldigheid,
        p.einddatumtijdvakgeldigheid,
        p.geovlak,
        h."ground-0.00",
        h."ground-0.10",
        h."ground-0.20",
        h."ground-0.30",
        h."ground-0.40",
        h."ground-0.50",
        h."roof-0.00",
        h."roof-0.10",
        h."roof-0.25",
        h."roof-0.50",
        h."roof-0.75",
        h."roof-0.90",
        h."roof-0.95",
        h."roof-0.99",
        h.ahn_file_date
    FROM bagactueel.pandactueelbestaand p
    INNER JOIN bagactueel.heights h ON p.identificatie = h.id;
    """)
    db.sendQuery(query)
    
    db.sendQuery("CREATE INDEX bag3d_identificatie_idx ON bagactueel.bag3d (identificatie);")
    db.sendQuery("CREATE INDEX bag3d_geovlak_idx ON bagactueel.bag3d USING GIST (geovlak);")
    db.sendQuery("SELECT populate_geometry_columns('bagactueel.bag3d'::regclass);")
    db.sendQuery("COMMENT ON TABLE bagactueel.bag3d IS 'The 3D BAG';")
    db.sendQuery("DROP TABLE bagactueel.heights CASCADE;")
    
    db.sendQuery("""
    CREATE OR REPLACE VIEW bagactueel.bag3d_valid_height AS
    SELECT *
    FROM bagactueel.bag3d
    WHERE bouwjaar <= date_part('YEAR', ahn_file_date) 
    AND begindatumtijdvakgeldigheid < ahn_file_date;
    """)
    
    db.sendQuery("COMMENT ON VIEW bagactueel.bag3d_valid_height IS 'The BAG footprints where the building was built before the AHN3 was created';")


# def union_csv(csv_dir, out_file, rm=False):
#     """Merge CSV files in a directory"""
#     p = os.path.join(csv_dir, "*.csv")
#     command = "cat {p} > {o}".format(p=p,
#                                      o=out_file)
#     run(command, shell=True)
#     if rm:
#         run(["rm", "-r", csv_dir])
        


def export_csv(cur, csv_out):
    """Export the 3DBAG table into a CSV file"""

    query = sql.SQL("""COPY (
    SELECT
        gid,
        identificatie,
        aanduidingrecordinactief,
        aanduidingrecordcorrectie,
        officieel,
        inonderzoek,
        documentnummer,
        documentdatum,
        pandstatus,
        bouwjaar,
        begindatumtijdvakgeldigheid,
        einddatumtijdvakgeldigheid,
        "ground-0.00",
        "ground-0.10",
        "ground-0.20",
        "ground-0.30",
        "ground-0.40",
        "ground-0.50",
        "roof-0.00",
        "roof-0.10",
        "roof-0.25",
        "roof-0.50",
        "roof-0.75",
        "roof-0.90",
        "roof-0.95",
        "roof-0.99",
        ahn_file_date
    FROM bagactueel.bag3d)
    TO STDOUT
    WITH (FORMAT 'csv', HEADER TRUE, ENCODING 'utf-8')""")
    
    with open(csv_out, "w") as c_out:
        cur.copy_to(query, c_out)


def export_bag3d(db, out_dir):
    """Export and prepare the 3D BAG in various formats
    
    PostGIS dump is restored as:
    
    createdb <db>
    psql -d <db> -c 'create extension postgis;'
    
    pg_restore \
    --no-owner \
    --no-privileges \
    -h <host> \
    -U <user> \
    -d <db> \
    -w bagactueel_schema.backup
    
    pg_restore \
    --no-owner \
    --no-privileges \
    -j 2 \
    --clean \
    -h <host> \
    -U <user> \
    -d <db> \
    -w bag3d_30-12-2017.backup
    
    """
    date = datetime.date.today().isoformat()
    
    postgis_dir = os.path.join(out_dir, "postgis")
    # PostGIS schema (required because of the pandstatus custom data type)
    command = "pg_dump \
--host {h} \
--port {p} \
--username {u} \
--no-password \
--format custom \
--no-owner \
--compress 7 \
--encoding UTF8 \
--verbose \
--schema-only \
--schema bagactueel \
--file {f} \
bag".format(h=db.host,
            p=db.port,
            u=db.user,
            f=os.path.join(postgis_dir,"bagactueel_schema.backup"))
    run(command, shell=True)
    
    # The 3D BAG (building heights + footprint geom)
    x =  "bag3d_{d}.backup".format(d=date)
    command = "pg_dump \
--host {h} \
--port {p} \
--username {u} \
--no-password \
--format custom \
--no-owner \
--compress 7 \
--encoding UTF8 \
--verbose \
--file {f} \
--table bagactueel.bag3d \
bag".format(h=db.host,
            p=db.port,
            u=db.user,
            f=os.path.join(postgis_dir, x))
    run(command, shell=True)
    
    # Create GeoPackage
    x = "bag3d_{d}.gpkg".format(d=date)
    f = os.path.join(out_dir, "gpkg", x)
    command = "ogr2ogr -f GPKG {f} \
    PG:'dbname={db} \
    host={h} \
    user={u} \
    password={pw} \
    schemas=bagactueel tables=bag3d'".format(f=f,
                                             db=db.dbname,
                                             h=db.host,
                                             pw=db.password,
                                             u=db.user)
    run(command, shell=True)
    
    # CSV
    x = "bag3d_{d}.csv".format(d=date)
    csv_out = os.path.join(out_dir, "csv", x)
    with db.conn:
        with db.conn.cursor() as cur:
            export_csv(cur, csv_out)


def main():
    """Main function
    
    !!! The script processes ALL csv files in the given directory !!!

    Creates the table if doesn't exists.
    
    Example:
    
    $ csv2db.py -d /some/directory/with/CSVs -s bagactueel -t heights -rm 
    """
    
    parser = argparse.ArgumentParser(description="Copy CSV-BUILDINGS-MULTIPLE files to PostgreSQL table")
    parser.add_argument(
        "-d",
        help="Directory with CSV files")
    parser.add_argument(
        "-c",
        help="batch3dfier config file")
    parser.add_argument(
        "-o",
        help="Output directory")
    parser.add_argument(
        "--del-csv",
        action="store_true",
        dest="rm",
        help="Remove CSV files from disk after import")
    parser.add_argument(
        "--keep-csv",
        action="store_false",
        dest="rm",
        help="Keep CSV files from disk after import (default)")
    parser.set_defaults(rm=False)

    args = parser.parse_args()
    args_in = {}
    args_in['csv_dir'] = path.abspath(args.d)
    args_in['out_dir'] = path.abspath(args.o)
    args_in['rm'] = args.rm
    args_in["cfg_file"] = args.c
    
    cfg = parse_config_yaml(args_in)

    # Get CSV files in dir
    for root, dir, filenames in walk(args_in['csv_dir'], topdown=True):
        csv_files = [f for f in filenames if path.splitext(f)[1].lower() == ".csv"]
        out_paths = [path.join(args_in['csv_dir'], f) for f in csv_files]
    print("There are {} CSV files in the directory".format(len(csv_files)))

    csv2db(cfg['dbase'], cfg, out_paths)
    
    create_bag3d_relations(cfg['dbase'])
    
    export_bag3d(cfg['dbase'], args_in['out_dir'])
    
    if args_in['rm']:
        run(["rm", "-r", args_in['csv_dir']])
    
    # report how many files were created and how many tiles are there


if __name__ == '__main__':
    main()