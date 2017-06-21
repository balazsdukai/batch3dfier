/* The batch3dfier package was developed for the case of 3dfying 
 * the building footprints in the BAG dataset 
 * (https://www.kadaster.nl/basisregistratie-gebouwen).
 * Thus some database table and filed names are hard-coded and rely on the 
 * setup in this script.
 * 
 * tile index: 
 * A dataset of rectangular polygons that partition the BAG dataset.
 * In this case the AHN tile index was used, which is found here:
 * http://www.ahn.nl/binaries/content/assets/ahn-nl/downloads/ahn_subunits.zip
 * 
 * Author: Balázs Dukai (balazs.dukai@gmail.com)
 * Date: 20.06.2017
 */

/* Create a BAG database from PostgreSQL dump found at
 * http://data.nlextract.nl/bag/postgis/
 */

/* Schema to store the tile index */
CREATE SCHEMA IF NOT EXISTS ahn3;



/* Load the tile index into the schema. E.g.:
 * 
 * ogr2ogr -f PostgreSQL PG:"dbname=bag active_schema=ahn3 host=localhost \
 * port=5432 user=bag password=pw" ahn_units.shp -skip-failure -lco FID=id \
 * -lco GEOMETRY_NAME=geom
 * 
 * The table must have at least the following configuration and names: */

--CREATE TABLE ahn3.ahn_units (
--    id int4 NOT NULL DEFAULT nextval('ahn3.ahn_units_id_seq'::regclass),
--    geom geometry,
--    unit varchar(5)
--    CONSTRAINT ahn_units_pkey PRIMARY KEY (id)
--);



/* Prepare tile polygon boundaries*/
ALTER TABLE
    ahn3.ahn_units ADD COLUMN geom_border geometry;

-- update tiles to include the lower/left boundary
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
    
CREATE INDEX units_geom_border_idx ON ahn3.ahn_units USING gist (geom_border);

SELECT populate_geometry_columns('ahn3.ahn_units'::regclass);

VACUUM ANALYZE ahn3.ahn_units;



/* Prepare BAG building footprint centroids */

CREATE TABLE bagactueel.pand_centroid AS
SELECT gid, st_centroid(geovlak)::geometry(point, 28992) AS geom
FROM bagactueel.pandactueelbestaand;

SELECT populate_geometry_columns('bagactueel.pand_centroid'::regclass);

CREATE
    INDEX pand_centroid_geom_idx ON
    bagactueel.pand_centroid
        USING gist(geom);
        
VACUUM ANALYZE bagactueel.pand_centroid;


