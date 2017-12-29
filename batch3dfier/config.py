# -*- coding: utf-8 -*-

"""Configure batch3dfier with the input data."""

import os.path
from subprocess import call

from shapely.geometry import shape
from shapely import geos
from psycopg2 import sql
import fiona


def call_3dfier(db, tile, schema_tiles,
                pc_file_name, pc_tile_case, pc_dir,
                table_index_pc, fields_index_pc,
                table_index_footprint, fields_index_footprint, uniqueid,
                extent_ewkb, clip_prefix, prefix_tile_footprint,
                yml_dir, tile_out, output_format, output_dir,
                path_3dfier, thread):
    """Call 3dfier with the YAML config created by yamlr().

    Note
    ----
    For the rest of the parameters see batch3dfier_config.yml.

    Parameters
    ----------
    db : db Class instance
    tile : str
        Name of of the 2D tile.
    schema_tiles : str
        Schema of the footprint tiles.
    pc_file_name : str
        Naming convention for the pointcloud files. See 'dataset_name' in batch3dfier_config.yml.
    pc_tile_case : str
        How the string matching is done for pc_file_name. See 'tile_case' in batch3dfier_config.yml.
    pc_dir : str
        Directory of the pointcloud files. See 'dataset_dir' in batch3dfier_config.yml.
    thread : str
        Name/ID of the active thread.
    extent_ewkb : str
        EWKB representation of 'extent' in batch3dfier_config.yml.
    clip_prefix : str
        Prefix for naming the clipped/united views. This value shouldn't be a substring of the pointcloud file names.
    prefix_tile_footprint : str or None
        Prefix prepended to the footprint tile view names. If None, the views are named as
        the values in fields_index_fooptrint['unit_name'].

    Returns
    -------
    list
        The tiles that are skipped because no corresponding pointcloud file
        was found in 'dataset_dir' (YAML)

    """
    pc_tiles = find_pc_tiles(db, table_index_pc, fields_index_pc,
                             table_index_footprint, fields_index_footprint,
                             extent_ewkb, tile_footprint=tile,
                             prefix_tile_footprint=prefix_tile_footprint)

    pc_path = find_pc_files(pc_tiles, pc_dir, pc_file_name, pc_tile_case)

    # prepare output file name
    if not tile_out:
        tile_out = tile.replace(clip_prefix, '', 1)

    # Call 3dfier ------------------------------------------------------------

    if pc_path:
        # Needs a YAML per thread so one doesn't overwrite it while the other
        # uses it
        yml_name = thread + "_config.yml"
        yml_path = os.path.join(yml_dir, yml_name)
        config = yamlr(dbname=db.dbname, host=db.host, user=db.user,
                       pw=db.password, schema_tiles=schema_tiles,
                       bag_tile=tile, pc_path=pc_path,
                       output_format=output_format, uniqueid=uniqueid)
        # Write temporary config file
        try:
            with open(yml_path, "w") as text_file:
                text_file.write(config)
        except BaseException:
            print("Error: cannot write _config.yml")
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
        except BaseException:
            print("\nCannot run 3dfier on tile " + tile)
            tile_skipped = tile
    else:
        print(
            "\nPointcloud file(s) " +
            str(pc_tiles) +
            " not available. Skipping tile.\n")
        tile_skipped = tile
        return({'tile_skipped': tile_skipped,
                'out_path': None})

    return({'tile_skipped': None,
            'out_path': output_path})


def yamlr(dbname, host, user, pw, schema_tiles,
          bag_tile, pc_path, output_format, uniqueid):
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

    # !!! Do not correct the indentation of the config template, otherwise it
    # results in 'YAML::TypedBadConversion<std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> > >'
    # because every line is indented as here
    config = """
input_polygons:
  - datasets:
      - "PG:dbname={dbname} host={host} user={user} password={pw} schemas={schema_tiles} tables={bag_tile}"
    uniqueid: {uniqueid}
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
    thinning: 0

options:
  building_radius_vertex_elevation: 2.0
  radius_vertex_elevation: 1.0
  threshold_jump_edges: 0.5

output:
  format: {output_format}
  building_floor: true
  vertical_exaggeration: 0
        """.format(dbname=dbname,
                   host=host,
                   user=user,
                   pw=pw,
                   schema_tiles=schema_tiles,
                   bag_tile=bag_tile,
                   uniqueid=uniqueid,
                   pc_path=pc_dataset,
                   output_format=output_format)
    return(config)


def find_pc_files(pc_tiles, pc_dir, pc_file_name, pc_tile_case):
    """Find pointcloud files in the file system when given a list of pointcloud tile names
    """
    # Prepare AHN file names -------------------------------------------------
    if pc_tile_case == "upper":
        tiles = [pc_file_name.format(tile=t.upper()) for t in pc_tiles]
    elif pc_tile_case == "lower":
        tiles = [pc_file_name.format(tile=t.lower()) for t in pc_tiles]
    elif pc_tile_case == "mixed":
        tiles = [pc_file_name.format(tile=t) for t in pc_tiles]
    else:
        raise "Please provide one of the allowed values for pc_tile_case."
    # use the tile list in tiles to parse the pointcloud file names
    pc_path = [os.path.join(pc_dir, pc_tile) for pc_tile in tiles]

    if all([os.path.isfile(p) for p in pc_path]):
        return(pc_path)
    else:
        return(None)


def find_pc_tiles(db, table_index_pc, fields_index_pc,
                  table_index_footprint=None, fields_index_footprint=None,
                  extent_ewkb=None, tile_footprint=None,
                  prefix_tile_footprint=None):
    """Find pointcloud tiles in tile index that intersect the extent or the footprint tile.

    Parameters
    ----------
    prefix_tile_footprint : str or None
        Prefix prepended to the footprint tile view names. If None, the views are named as
        the values in fields_index_fooptrint['unit_name'].

    """
    if extent_ewkb:
        tiles = get_2Dtiles(db, table_index_pc, fields_index_pc, extent_ewkb)
    else:
        schema_pc_q = sql.Identifier(table_index_pc['schema'])
        table_pc_q = sql.Identifier(table_index_pc['table'])
        field_pc_geom_q = sql.Identifier(fields_index_pc['geometry'])
        field_pc_unit_q = sql.Identifier(fields_index_pc['unit_name'])

        schema_ftpr_q = sql.Identifier(table_index_footprint['schema'])
        table_ftpr_q = sql.Identifier(table_index_footprint['table'])
        field_ftpr_geom_q = sql.Identifier(fields_index_footprint['geometry'])
        field_ftpr_unit_q = sql.Identifier(fields_index_footprint['unit_name'])

        if prefix_tile_footprint:
            tile_footprint = tile_footprint.replace(
                prefix_tile_footprint, '', 1)

        tile_q = sql.Literal(tile_footprint)

        query = sql.SQL("""
                            SELECT
                            {table_pc}.{field_pc_unit}
                        FROM
                            {schema_pc}.{table_pc},
                            {schema_ftpr}.{table_ftpr}
                        WHERE
                            {table_ftpr}.{field_ftpr_unit} = {tile}
                            AND st_intersects(
                                {table_pc}.{field_pc_geom},
                                {table_ftpr}.{field_ftpr_geom}
                            );
                        """).format(table_pc=table_pc_q,
                                    field_pc_unit=field_pc_unit_q,
                                    schema_pc=schema_pc_q,
                                    schema_ftpr=schema_ftpr_q,
                                    table_ftpr=table_ftpr_q,
                                    field_ftpr_unit=field_ftpr_unit_q,
                                    tile=tile_q,
                                    field_pc_geom=field_pc_geom_q,
                                    field_ftpr_geom=field_ftpr_geom_q)

        resultset = db.getQuery(query)
        tiles = [tile[0] for tile in resultset]

    return(tiles)


def extent_to_ewkb(db, table_index, file):
    """Reads a polygon from a file and returns its EWKB.

    I didn't find a simple way to safely get SRIDs from the input geometry
    with Shapely, therefore it is obtained from the database and the CRS of the
    polygon is assumed to be the same as of the tile indexes.

    Parameters
    ----------
    db : db Class instance
    table_index : dict
        {'schema' : str, 'table' : str} of the table of tile index.
    file : str
        Path to the polygon for clipping the input.
        Must be in the same CRS as the table_index.

    Returns
    -------
    [Shapely polygon, EWKB str]
    """
    schema = sql.Identifier(table_index['schema'])
    table = sql.Identifier(table_index['table'])

    query = sql.SQL("""SELECT st_srid(geom) AS srid
                    FROM {schema}.{table}
                    LIMIT 1;""").format(schema=schema, table=table)
    srid = db.getQuery(query)[0][0]

    assert srid is not None

    # Get clip polygon and set SRID
    with fiona.open(file, 'r') as src:
        poly = shape(src[0]['geometry'])
        # Change a the default mode to add this, if SRID is set
        geos.WKBWriter.defaults['include_srid'] = True
        # set SRID for polygon
        geos.lgeos.GEOSSetSRID(poly._geom, srid)
        ewkb = poly.wkb_hex

    return([poly, ewkb])


def get_2Dtiles(db, table_index, fields_index, ewkb):
    """Returns a list of tiles that overlap the output extent.

    Parameters
    ----------
    db : db Class instance
    table_index : dict
        {'schema' : str, 'table' : str} of the table of tile index.
    fields_index : dict
        {'primary_key' : str, 'geometry' : str, 'unit_name' : str}
        primary_key: Name of the primary_key field in table_index.
        geometry: Name of the geometry field in table_index.
        unit: Name of the field in table_index that contains the index unit names.
    ewkb : str
        EWKB representation of a polygon.

    Returns
    -------
    [tile IDs]
        Tiles that are intersected by the polygon that is provided in 'extent' (YAML).

    """
    schema = sql.Identifier(table_index['schema'])
    table = sql.Identifier(table_index['table'])
    field_idx_geom_q = sql.Identifier(fields_index['geometry'])
    field_idx_unit_q = sql.Identifier(fields_index['unit_name'])

    ewkb_q = sql.Literal(ewkb)
    # TODO: user input for a.unit
    query = sql.SQL("""
                SELECT {table}.{field_idx_unit}
                FROM {schema}.{table}
                WHERE st_intersects({table}.{field_idx_geom}, {ewkb}::geometry);
                """).format(schema=schema,
                            table=table,
                            field_idx_unit=field_idx_unit_q,
                            field_idx_geom=field_idx_geom_q,
                            ewkb=ewkb_q)
    resultset = db.getQuery(query)
    tiles = [tile[0] for tile in resultset]

    print("Nr. of tiles in clip extent: " + str(len(tiles)))

    return(tiles)


def get_2Dtile_area(db, table_index):
    """Get the area of a 2D tile.

    Note
    ----
    Assumes that all tiles have equal area. Area is in units of the tile CRS.

    Parameters
    ----------
    db : db Class instance
    table_index : list of str
        {'schema' : str, 'table' : str} of the table of tile index.

    Returns
    -------
    float

    """
    schema = sql.Identifier(table_index['schema'])
    table = sql.Identifier(table_index['table'])

    query = sql.SQL("""
                SELECT public.st_area(geom) AS area
                FROM {schema}.{table}
                LIMIT 1;
                """).format(schema=schema, table=table)
    area = db.getQuery(query)[0][0]

    return(area)


def get_2Dtile_views(db, schema_tiles, tiles):
    """Get View names of the 2D tiles. It tries to find views in schema_tiles
    that contain the respective tile ID in their name.

    Parameters
    ----------
    db : db Class instance
    schema_tiles: str
        Name of the schema where the 2D tile views are stored.
    tiles : list
        Tile IDs

    Returns
    -------
    list
        Name of the view that contain the tile ID as substring.

    """
    # Get View names for the tiles
    t = ["%" + str(tile) + "%" for tile in tiles]
    t = sql.Literal(t)
    schema_tiles = sql.Literal(schema_tiles)
    query = sql.SQL("""SELECT table_name
                        FROM information_schema.views
                        WHERE table_schema = {}
                        AND table_name LIKE any({});
                        """).format(schema_tiles, t)
    resultset = db.getQuery(query)
    tile_views = [tile[0] for tile in resultset]

    return(tile_views)


def clip_2Dtiles(db, user_schema, schema_tiles, tiles, poly, clip_prefix,
                 fields_view):
    """Creates views for the clipped tiles.

    Parameters
    ----------
    db : db Class instance
    user_schema: str
    schema_tiles : str
    tiles : list
    poly : Shapely polygon
    clip_prefix : str

    Returns
    -------
    list
        Name of the views of the clipped tiles.

    """
    user_schema = sql.Identifier(user_schema)
    schema_tiles = sql.Identifier(schema_tiles)
    tiles_clipped = []

    fields_all = fields_view['all']
    field_geom_q = sql.Identifier(fields_view['geometry'])

    for tile in tiles:
        t = clip_prefix + tile
        tiles_clipped.append(t)
        view = sql.Identifier(t)
        tile_view = sql.Identifier(tile)
        fields_q = parse_sql_select_fields(tile, fields_all)
        wkb = sql.Literal(poly.wkb_hex)
        query = sql.SQL("""
            CREATE OR REPLACE VIEW {user_schema}.{view} AS
                SELECT
                    {fields}
                FROM
                    {schema_tiles}.{tile_view}
                WHERE
                    st_within({tile_view}.{geom}, {wkb}::geometry)"""
                        ).format(user_schema=user_schema,
                                 schema_tiles=schema_tiles,
                                 view=view,
                                 fields=fields_q,
                                 tile_view=tile_view,
                                 geom=field_geom_q,
                                 wkb=wkb)
        db.sendQuery(query)
    try:
        db.conn.commit()
        print(
            str(
                len(tiles_clipped)) +
            " views with prefix '{}' are created in schema {}.".format(
                clip_prefix,
                user_schema))
    except BaseException:
        print("Cannot create view {user_schema}.{clip_prefix}{tile}".format(
            schema_tiles=schema_tiles, clip_prefix=clip_prefix))
        db.conn.rollback()

    return(tiles_clipped)


def union_2Dtiles(db, user_schema, tiles_clipped, clip_prefix, fields_view):
    """Union the clipped tiles into a single view.

    Parameters
    ----------
    db : db Class instance
    user_schema : str
    tiles_clipped : list
    clip_prefix : str

    Returns
    -------
    str
        Name of the united view.

    """
    # Check if there are enough tiles to unite
    assert len(tiles_clipped) > 1, "Need at least 2 tiles for union"

    user_schema = sql.Identifier(user_schema)
    u = "{clip_prefix}union".format(clip_prefix=clip_prefix)
    union_view = sql.Identifier(u)
    sql_query = sql.SQL("CREATE OR REPLACE VIEW {user_schema}.{view} AS ").format(
        user_schema=user_schema, view=union_view)

    fields_all = fields_view['all']

    for tile in tiles_clipped[:-1]:
        view = sql.Identifier(tile)
        fields_q = parse_sql_select_fields(tile, fields_all)
        sql_subquery = sql.SQL("""SELECT {fields}
                               FROM {user_schema}.{view}
                               UNION ALL """).format(fields=fields_q,
                                                     user_schema=user_schema,
                                                     view=view)

        sql_query = sql_query + sql_subquery
    # The last statement
    tile = tiles_clipped[-1]
    view = sql.Identifier(tile)
    fields_q = parse_sql_select_fields(tile, fields_all)
    sql_subquery = sql.SQL("""SELECT {fields}
                           FROM {user_schema}.{view};
                           """).format(fields=fields_q,
                                       user_schema=user_schema,
                                       view=view)
    sql_query = sql_query + sql_subquery

    db.sendQuery(sql_query)

    try:
        db.conn.commit()
        print("View {} created in schema {}.".format(u, user_schema))
    except BaseException:
        print("Cannot create view {}.{}".format(user_schema, u))
        db.conn.rollback()
        return(False)

    return(u)


def get_view_fields(db, user_schema, tile_views):
    """Get the fields in a 2D tile view

    Parameters
    ----------
    tile_views : list of str

    Returns
    -------
    {'all' : list, 'geometry' : str}

    """
    if len(tile_views) > 0:
        schema_q = sql.Literal(user_schema)
        view_q = sql.Literal(tile_views[0])

        resultset = db.getQuery(sql.SQL("""
                            SELECT
                                column_name
                            FROM
                                information_schema.columns
                            WHERE
                                table_schema = {schema}
                                AND table_name = {view};
                            """).format(schema=schema_q,
                                        view=view_q))
        f = [field[0] for field in resultset]

        geom_res = db.getQuery(sql.SQL("""
                            SELECT
                                f_geometry_column
                            FROM
                                public.geometry_columns
                            WHERE
                                f_table_schema = {schema}
                                AND f_table_name = {view};
                            """).format(schema=schema_q,
                                        view=view_q))
        f_geom = geom_res[0][0]

        fields = {}
        fields['all'] = f
        fields['geometry'] = f_geom

        return(fields)

    else:
        return(None)


def parse_sql_select_fields(table, fields):
    """Parses a list of field names into "table"."field" to insert into a SELECT ... FROM table

    Parameters
    ----------
    fields : list of str

    Returns
    -------
    psycopg2.sql.Composable

    """
    s = []
    for f in fields:
        s.append(sql.SQL('.').join([sql.Identifier(table), sql.Identifier(f)]))
    sql_fields = sql.SQL(', ').join(s)

    return(sql_fields)


def drop_2Dtiles(db, user_schema, views_to_drop):
    """Drops Views in a given schema.

    Note
    ----
    Used for dropping the views created by clip_2Dtiles() and union_2Dtiles().

    Parameters
    ----------
    db : db Class instance
    user_schema : str
    views_to_drop : list

    Returns
    -------
    bool

    """
    user_schema = sql.Identifier(user_schema)

    for view in views_to_drop:
        view = sql.Identifier(view)
        query = sql.SQL("DROP VIEW IF EXISTS {user_schema}.{view} CASCADE;").format(
            user_schema=user_schema, view=view)
        db.sendQuery(query)
    try:
        db.conn.commit()
        print("Dropped {} in schema {}.".format(views_to_drop, user_schema))
        # sql.Identifier("tile_index").as_string(dbs.conn)
        return(True)
    except BaseException:
        print("Cannot drop views ", views_to_drop)
        db.conn.rollback()
        return(False)