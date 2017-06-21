import os.path
from subprocess import call
from shapely.geometry import shape
from shapely import geos
from psycopg2 import sql
import fiona



def yamlr(dbname, host, user, pw, tile_schema,
          bag_tile, pc_path, output_format):
    """Parse the YAML config file for 3dfier.

    Parameters
    ----------
    See batch3dfier_config.yml.
        

    Returns
    -------
    string
        the YAML config file for 3dfier

    """

    pc_dataset = ""
    if len(pc_path) > 1:
        for p in pc_path:
            pc_dataset += "- " + p + "\n" + "      "
    else:
        pc_dataset += "- " + pc_path[0]

    config = """
input_polygons:
  - datasets:
      - "PG:dbname={dbname} host={host} user={user} password={pw} schemas={tile_schema} tables={bag_tile}"
    uniqueid: identificatie
    lifting: Building

lifting_options:
  Building:
    height_roof: percentile-90
    height_floor: percentile-10
    lod: 1

input_elevation:
  - datasets:
      {pc_path}
    omit_LAS_classes:
      - 1
    thinning: 0

options:
  building_radius_vertex_elevation: 3.0
  radius_vertex_elevation: 1.0
  threshold_jump_edges: 0.5

output:
  format: {output_format}
  building_floor: true
  vertical_exaggeration: 0
        """.format(dbname=dbname, host=host, user=user, pw=pw,
                   tile_schema=tile_schema,
                   bag_tile=bag_tile, pc_path=pc_dataset,
                   output_format=output_format)
    return(config)


def call3dfier(tile, thread, clip_prefix, union_view, tiles, pc_file_name,
               pc_dir, tile_case, yml_dir, dbname, host, user, pw,
               tile_schema, output_format, output_dir, path_3dfier):
    """Call 3dfier with the YAML config created by yamlr().
    
    Note
    ----
    For the rest of the parameters see batch3dfier_config.yml.

    Parameters
    ----------
    tile : str
        Name of of the 2D tile.
    thread : str
        Name/ID of the active thread.
    clip_prefix : str 
        Prefix for naming the clipped/united views. This value shouldn't be a substring of the pointcloud file names.
    union_view : str

    Returns
    -------
    list
        The tiles that are skipped because no corresponding pointcloud file 
        was found in 'dataset_dir' (YAML)

    """

    tile_skipped = None

    # Prepare AHN file names ---------------------------------------------------
    if union_view:
        # Prepare pointcloud file names for searching them in dataset_dir
        # the output of this block is only passed to
        tile_out = "output_batch3Dfier"
        if tile_case == "upper":
            tiles = [pc_file_name.format(tile=t.upper()) for t in tiles]
        elif tile_case == "lower":
            tiles = [pc_file_name.format(tile=t.lower()) for t in tiles]
        elif tile_case == "mixed":
            tiles = [pc_file_name.format(tile=t) for t in tiles]
        else:
            raise "Please provide one of the allowed values for tile_case."
        # use the tile list in tiles to parse the pointcloud file names
        pc_path = [os.path.join(pc_dir, pc_tile) for pc_tile in tiles]
    else:
        # name of the 3D output matches the view name of the 2D tile
        tile_out = tile.replace(clip_prefix, '', 1)
        # Prepare pointcloud file names for searching them in dataset_dir
        # FIXME: do this properly without hard-coding the tile view prefix
        ptile = tile_out.replace('t_', '', 1)
        if tile_case == "upper":
            pc_tile = pc_file_name.format(tile=ptile.upper())
        elif tile_case == "lower":
            pc_tile = pc_file_name.format(tile=ptile.lower())
        elif tile_case == "mixed":
            pc_tile = pc_file_name.format(tile=ptile)
        else:
            raise "Please provide one of the allowed values for tile_case."
        pc_path = [os.path.join(pc_dir, pc_tile)]

    # Call 3dfier --------------------------------------------------------------
    # Check if the required AHN3 file exists in pc_path
    if all([os.path.isfile(p) for p in pc_path]):
        # Needs a YAML per thread so one doesn't overwrite it while the other
        # uses it
        yml_name = thread + "_config.yml"
        yml_path = os.path.join(yml_dir, yml_name)
        config = yamlr(dbname=dbname, host=host, user=user,
                       pw=pw, tile_schema=tile_schema,
                       bag_tile=tile, pc_path=pc_path,
                       output_format=output_format)
        # Write temporary config file
        try:
            with open(yml_path, "w") as text_file:
                text_file.write(config)
        except:
            print("Error: can\'t write tempconfig.yml")
        # Prep output file name
        if "obj" in output_format.lower():
            o = tile_out + ".obj"
            output_path = os.path.join(output_dir, o)
        elif "csv" in output_format.lower():
            o = tile_out + ".csv"
            output_path = os.path.join(output_dir, o)
        else:
            output_path = os.path.join(output_dir, tile_out)
        # Run 3dfier
        command = (path_3dfier + " {yml} -o {out}").format(
            yml=yml_path, out=output_path)
        try:
            call(command, shell=True)
        except:
            print("\nCannot run 3dfier on tile " + tile)
            tile_skipped = tile
    else:
        print("\nPointcloud file " + pc_tile +
              " not available. Skipping tile.\n")
        tile_skipped = tile

    return(tile_skipped)


def get_2Dtile_area(conn, tile_index):
    """Get the area of a 2D tile.
    
    Note
    ----
    Assumes that all tiles have equal area. Area is in units of the tile CRS.

    Parameters
    ----------
    conn : psycopg2 Connection object
    tile_index : tuple
        As (schema name, table name) of the table of tile index.

    Returns
    -------
    float

    """
    schema = sql.Identifier(tile_index[0])
    table = sql.Identifier(tile_index[1])

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("""
                    SELECT public.st_area(geom) AS area
                    FROM {schema}.{table}
                    LIMIT 1;
                    """).format(schema=schema, table=table))
        area = cur.fetchall()[0][0]

    return(area)


def get_2Dtiles(file, conn, tile_index):
    """Returns a list of tiles that overlap the output extent. Returns the output extent as Shapely polygon.

    Parameters
    ----------
    file : str
        Path to the polygon for clipping the input.
        Must be in the same CRS as the tile_index.
    conn : psycopg2 Connection object
    tile_index : tuple 
        As (schema name, table name) of the table of tile index.

    Returns
    -------
    [[tile IDs], Shapely polygon]
        Tiles that are intersected by the polygon that is provided in
        'extent' (YAML).

    """
    schema = sql.Identifier(tile_index[0])
    table = sql.Identifier(tile_index[1])

    with conn.cursor() as cur:
        # I didn't find a simple way to safely get SRIDs from the input geometry
        # Therefore its obtained from the database
        cur.execute(sql.SQL("""SELECT st_srid(geom) AS srid
                                FROM {schema}.{table}
                                LIMIT 1;""").format(schema=schema, table=table))
        srid = cur.fetchall()[0][0]

        # Get clip polygon and set SRID
        with fiona.open(file, 'r') as src:
            poly = shape(src[0]['geometry'])
            # Change a the default mode to add this, if SRID is set
            geos.WKBWriter.defaults['include_srid'] = True
            # set SRID for polygon
            geos.lgeos.GEOSSetSRID(poly._geom, srid)
            ewkb = poly.wkb_hex

        # TODO: user input for a.unit
        cur.execute(
            sql.SQL("""
                    SELECT a.unit
                    FROM {schema}.{table} as a
                    WHERE st_intersects(a.geom, %s::geometry);
                    """).format(schema=schema, table=table), [ewkb])
        resultset = cur.fetchall()
    tiles = [tile[0] for tile in resultset]

    print("Nr. of tiles in clip extent: " + str(len(tiles)))

    return([tiles, poly])


def get_2Dtile_views(conn, tile_schema, tiles):
    """Get View names of the 2D tiles. It tries to find views in tile_schema
    that contain the respective tile ID in their name.

    Parameters
    ----------
    conn : psycopg2 Connection object
    tile_schema: str
        Name of the schema where the 2D tile views are stored.
    tiles : list
        Tile IDs

    Returns
    -------
    list
        Name of the view that contain the tile ID as substring.

    """
    # Get View names for the tiles
    t = ["%" + tile + "%" for tile in tiles]
    with conn.cursor() as cur:
        cur.execute("""
                    SELECT table_name
                    FROM information_schema.views
                    WHERE table_schema = %s
                    AND table_name LIKE any(%s);
                    """, (tile_schema, (t, )))
        resultset = cur.fetchall()
    tile_views = [tile[0] for tile in resultset]

    return(tile_views)


def clip_2Dtiles(conn, tile_schema, tiles, poly, clip_prefix):
    """Creates views for the clipped tiles.

    Parameters
    ----------
    conn : psycopg2 Connection object
    tile_schema : str
    tiles : list
    poly : Shapely polygon
    clip_prefix : str

    Returns
    -------
    list
        Name of the views of the clipped tiles.

    """
    tile_schema = sql.Identifier(tile_schema)
    tiles_clipped = []

    with conn.cursor() as cur:
        for tile in tiles:
            t = clip_prefix + tile
            tiles_clipped.append(t)
            view = sql.Identifier(t)
            tile_view = sql.Identifier(tile)
            cur.execute(
                sql.SQL("""
                CREATE OR REPLACE VIEW {tile_schema}.{view} AS
                    SELECT
                        a.gid,
                        a.identificatie,
                        a.geovlak
                    FROM
                        {tile_schema}.{tile_view} AS a
                    WHERE
                        st_within(a.geovlak, %s::geometry)"""
                        ).format(tile_schema=tile_schema, view=view,
                                 tile_view=tile_view), [poly.wkb_hex])
    try:
        conn.commit()
        print(str(len(tiles_clipped)) +
              " views with prefix '{}' are created in schema {}.".format(clip_prefix, tile_schema))
    except:
        print("Cannot create view {tile_schema}.{clip_prefix}{tile}".format(
            tile_schema=tile_schema, clip_prefix=clip_prefix))
        conn.rollback()

    return(tiles_clipped)


def union_2Dtiles(conn, tile_schema, tiles_clipped, clip_prefix):
    """Union the clipped tiles into a single view.

    Parameters
    ----------
    conn : psycopg2 Connection object
    tile_schema : str
    tiles_clipped : list
    clip_prefix : str

    Returns
    -------
    str
        Name of the united view.

    """
    # Check if there are enough tiles to unite
    assert len(tiles_clipped) > 1, "Need at least 2 tiles for union"

    tile_schema = sql.Identifier(tile_schema)
    u = "{clip_prefix}union".format(clip_prefix=clip_prefix)
    union_view = sql.Identifier(u)
    sql_query = sql.SQL("CREATE OR REPLACE VIEW {tile_schema}.{view} AS ").format(
        tile_schema=tile_schema, view=union_view)

    for tile in tiles_clipped[:-1]:
        view = sql.Identifier(tile)
        sql_subquery = sql.SQL("""SELECT gid, identificatie, geovlak
                               FROM {tile_schema}.{view}
                               UNION ALL """).format(tile_schema=tile_schema, view=view)

        sql_query = sql_query + sql_subquery
    # The last statement
    view = sql.Identifier(tiles_clipped[-1])
    sql_subquery = sql.SQL("""SELECT gid, identificatie, geovlak
                           FROM {tile_schema}.{view};""").format(tile_schema=tile_schema, view=view)
    sql_query = sql_query + sql_subquery

    with conn.cursor() as cur:
        cur.execute(sql_query)
    try:
        conn.commit()
        print("View {} created in schema {}.".format(u, tile_schema))
    except:
        print("Cannot create view {tile_schema}.{u}".format(
            tile_schema=tile_schema, u=u))
        conn.rollback()
        return(False)

    return(u)


def drop_2Dtiles(conn, tile_schema, views_to_drop):
    """Drops Views in a given schema.
    
    Note
    ----
    Used for dropping the views created by clip_2Dtiles() and union_2Dtiles().

    Parameters
    ----------
    conn : psycopg2 Connection object
    tile_schema : str
    views_to_drop : list

    Returns
    -------
    bool

    """
    tile_schema = sql.Identifier(tile_schema)
    with conn.cursor() as cur:
        for view in views_to_drop:
            view = sql.Identifier(view)
            cur.execute(
                sql.SQL("DROP VIEW IF EXISTS {tile_schema}.{view} CASCADE;").format(tile_schema=tile_schema, view=view))
    try:
        conn.commit()
        print("Dropped {} in schema {}.".format(views_to_drop, tile_schema))
    except:
        print("Cannot drop views ", views_to_drop)
        conn.rollback()
        return(False)

    return(True)


def clip_PCtile(poly):
    # TODO: clip pointcloud tiles to extent if necessary
    return()
