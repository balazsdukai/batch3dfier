#===============================================================================
# Batch create views for the 2D(BAG) tiles.
# Before running this script, make sure the database is set up according to
# prep_bag_tiles.sql
#===============================================================================

from psycopg2 import sql
import psycopg2


# Connect to database
try:
    conn = psycopg2.connect(
        database="bag", user="bag", password="bag2017", host="localhost",
        port="5432")
    print("Opened database successfully")
except:
    print("I'm unable to connect to the database")
cur = conn.cursor()


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
conn.commit()
