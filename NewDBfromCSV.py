import postgresql
import glob
# -*- coding: utf-8 -*-

db = postgresql.open('pq://Jo:tttttt@localhost:5432/DL')

print("Granting user rights")
createDB = db.execute("""

GRANT ALL ON DATABASE "DL" TO "Jo";
GRANT ALL ON ALL TABLES IN SCHEMA public TO "Jo";
GRANT ALL ON TABLE spatial_ref_sys TO "Jo";

""")


print("Creating tables")
createDB = db.execute("""
--CREATE TABLESPACE pg_dl
--  OWNER postgres
--  LOCATION E'C:\\Data\\OSM\\PostGIS_Tablespace';

DROP TABLE IF EXISTS stops CASCADE;

--DROP EXTENSION postgis CASCADE;
--CREATE EXTENSION postgis;

DROP TABLE IF EXISTS places;
CREATE TABLE IF NOT EXISTS places ( placeid int NOT NULL PRIMARY KEY, placeidentifier text, placedescription text )
 WITH ( OIDS=FALSE );
ALTER TABLE places OWNER TO postgres;

DROP TABLE IF EXISTS calendar;
CREATE TABLE calendar ( vscid int NOT NULL PRIMARY KEY, vsid bigint, vscdate date, vscday text )
 WITH ( OIDS=FALSE );
ALTER TABLE calendar OWNER TO postgres;

DROP TABLE IF EXISTS routes;
CREATE TABLE routes
 ( routeid int NOT NULL PRIMARY KEY, routeidentifier text, routedescription text, routepublicidentifier text, routeversion text, routeservicetype text, routeservicemode text )
  WITH ( OIDS=FALSE );
ALTER TABLE routes OWNER TO postgres;

DROP TABLE IF EXISTS trips;
CREATE TABLE trips
 ( tripid bigint NOT NULL PRIMARY KEY, routeid int, vscid int, tripnoteidentifier text, tripnotetext text, tripstart text, tripend text, tripshiftstart integer, tripshiftend integer, tripnoteidentifier2 text, tripnotetext2 text, placeidstart bigint, placeidend bigint, naturalkey text )
   WITH ( OIDS=FALSE );
ALTER TABLE trips OWNER TO postgres;

DROP TABLE IF EXISTS segments;
CREATE TABLE segments
 ( segmentid bigint NOT NULL PRIMARY KEY, tripid bigint, stopid int, segmentsequence int, segmentstart text, segmentend text, segmentshiftstart integer, segmentshiftend integer )
  WITH ( OIDS=FALSE );
ALTER TABLE segments OWNER TO postgres;
""")

routescount = db.prepare('SELECT COUNT(*) FROM routes;')
segmentscount = db.prepare('SELECT COUNT(*) FROM segments;')
tripscount = db.prepare('SELECT COUNT(*) FROM trips;')

print("Copying data into tables")
createDB = db.execute("""

COPY places FROM 'C:/Data/De Lijn/places.csv' DELIMITERS ';' CSV HEADER;
COPY calendar FROM 'C:/Data/De Lijn/calendar.csv' DELIMITERS ';' CSV HEADER;
COPY routes FROM 'C:/Data/De Lijn/routes.csv' DELIMITERS ';' CSV HEADER;
COPY trips FROM 'C:/Data/De Lijn/trips.csv' DELIMITERS ';' CSV HEADER;
COPY segments FROM 'C:/Data/De Lijn/segments.csv' DELIMITERS ';' CSV HEADER;

""")

print("Creating tables for stops")
createDB = db.execute("""

DROP TABLE IF EXISTS stops;
--DROP TABLE IF EXISTS stops_TEC;
DROP TABLE IF EXISTS stops_DL;

CREATE TABLE stops
 ( stopid INT NOT NULL PRIMARY KEY, stopidentifier text, description text, street text, municipality text, parentmunicipality text, x INT, y INT, stopisaccessible BOOLEAN, stopispublic BOOLEAN )
  WITH ( OIDS=FALSE );
ALTER TABLE stops OWNER TO postgres;

COPY stops FROM 'C:/Data/De Lijn/stops.csv' DELIMITERS ';' CSV HEADER;

ALTER TABLE stops
 ADD COLUMN description_normalised text,
 ADD COLUMN lat DOUBLE PRECISION, 
 ADD COLUMN lon DOUBLE PRECISION,
 ADD COLUMN route_ref text,
 ADD COLUMN bustram text;
 
SELECT AddGeometryColumn ('public','stops','geomdl',4326,'POINT',2);

CREATE TABLE stops_DL
 ( stopsPK int NOT NULL PRIMARY KEY,
   last_change_timestamp timestamp,
   stopidentifier text,                 -- this corresponds to ref in OSM
   description text,                    -- this corresponds to name in OSM
   street text,
   municipality text,
   parentmunicipality text,
   stopisaccessible boolean,
   stopispublic boolean, 
   route_ref text,                      -- this is calculated
   geomDL geometry)
   WITH ( OIDS=FALSE );
ALTER TABLE stops OWNER TO postgres;

--DROP TABLE IF EXISTS stops_TEC;
--CREATE TABLE stops_TEC
-- ( stopidentifier text NOT NULL PRIMARY KEY, descriptionNL text, descriptionFR text, municipalityNL text, municipalityFR text, country text, streetNL text, streetFR text, ARI text, stopisaccessible INT, x INT, y INT, stopispublic INT, UIC text)
--  WITH ( OIDS=FALSE );
--ALTER TABLE stops_TEC OWNER TO postgres;

""")

# for fn in glob.glob('C:/Data/TEC/*.STP'):
    # db.execute("""COPY stops_TEC FROM '""" + fn + """' DELIMITERS '|' CSV;""")

print("Create index on routes (routeidentifier), routes (routeversion), trips(routeid) and on segments (tripid)")
createDB = db.execute("""

CREATE INDEX ix_routeidentifier ON routes (routeidentifier);
CREATE INDEX ix_routeversion ON routes (routeversion);
CREATE INDEX ix_tripsrouteid ON trips(routeid);
CREATE INDEX ix_segmentstripid ON segments (tripid);
""")


print("Vacuum analyze to gather statistics for efficient use of indexes")
createDB = db.execute("""

VACUUM ANALYZE --VERBOSE;

""")

print(routescount(), tripscount(), segmentscount())
print("Remove older route versions")
createDB = db.execute("""

    WITH currentversions AS (SELECT rte1.routeid FROM routes rte1
                             WHERE rte1.routeversion = (SELECT MAX(rte2.routeversion)
                                                        FROM routes rte2
                                                        JOIN trips ON trips.routeid=rte2.routeid -- we want the highest version in the routes table for which there are actual trips
                                                        WHERE rte1.routeidentifier=rte2.routeidentifier))
    DELETE FROM routes rte
      WHERE rte.routeid NOT IN (SELECT routeid from currentversions);
 """)

print("Remove trips for older route versions")
createDB = db.execute("""
DELETE FROM trips trp
 WHERE NOT EXISTS
   (SELECT trp2.routeid FROM trips trp2
     JOIN routes rte ON trp2.routeid=rte.routeid
      AND trp.routeid=rte.routeid);
""")

print("Remove segments for older route versions")
createDB = db.execute("""
DELETE FROM segments sgt
 WHERE NOT EXISTS (SELECT sgt2.segmentid FROM segments sgt2
                     JOIN trips trp ON sgt2.tripid=trp.tripid
                      AND sgt2.segmentid=sgt.segmentid);
""")

print("Creating indexes")
createDB = db.execute("""

CREATE INDEX ix_stopidentifier ON stops (stopidentifier);
--CREATE INDEX ix_tecstopidentifier ON stops_tec (stopidentifier);
CREATE INDEX ix_description ON stops (description);
CREATE INDEX ix_routepublicidentifier ON routes (routepublicidentifier);
CREATE INDEX ix_segmentstopid ON segments (stopid);

""")

print("Vacuum analyze to gather statistics for efficient use of indexes")
createDB = db.execute("""

VACUUM ANALYZE --VERBOSE;

""")

print(routescount(), tripscount(), segmentscount())

print("Creating stored procedures")
createDB = db.execute("""
CREATE OR REPLACE FUNCTION AllLinesPassingAtaStop(stopidentifierparameter text) RETURNS text AS $BODY$
  DECLARE outlines text :='';
    l record;
    line text;
    bustram text:='';
  BEGIN
    FOR l IN SELECT distinct(lpad(rte.routepublicidentifier, 5, '0')) AS publicidentifier, rte.routeservicemode AS mode
				FROM public.trips    trp
				JOIN public.routes   rte      ON rte.routeid=trp.routeid AND
                                                 rte.routepublicidentifier NOT LIKE 'F%'
				JOIN public.segments seg      ON seg.tripid=trp.tripid
				JOIN public.stops    stp      ON seg.stopid=stp.stopid
				WHERE
				  stp.stopidentifier = stopidentifierparameter
                ORDER BY lpad(rte.routepublicidentifier, 5, '0')
    LOOP
      line := l.publicidentifier;
      line := trim(leading '(' FROM line);
      IF line = '00000'
      THEN line:= '0';
      ELSE line := trim(leading '0' FROM line);
      END IF;
      line := trim(trailing ')' FROM line);
      outlines := outlines || ';' || line;
      IF l.mode='0'
      THEN bustram:= bustram || 'b';
      ELSE bustram:= bustram || 't';
      END IF;
    END LOOP;

    RETURN trim(both ';' FROM outlines) || ',' || bustram;
  END $BODY$
LANGUAGE plpgsql VOLATILE COST 100;
ALTER FUNCTION AllLinesPassingAtaStop(text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION AllLinesPassingAtaStop(text) TO public;

CREATE OR REPLACE FUNCTION filloutlines() RETURNS void AS $BODY$
  DECLARE
    l record;
    res text;
    coords geometry;
    vlat double precision;
    vlon double precision;

  BEGIN
    DROP INDEX IF EXISTS ix_geomDL;
    FOR l IN SELECT stopid, stopidentifier, x, y FROM stops
      LOOP res := AllLinesPassingAtaStop(l.stopidentifier);
        coords := st_transform(st_setSRID(st_Point(l.x, l.y), 31370),4326);
        vlat := st_y(coords);
        vlon := st_x(coords);

      UPDATE stops
        SET route_ref=split_part(res,',',1),
          bustram=split_part(res,',',2),
          geomDL = coords,
          lat = vlat,
          lon = vlon
        WHERE stops.stopid=l.stopid;
	  RAISE NOTICE  '% set to %',l.stopidentifier, res;
     END LOOP;
   END 
$BODY$ LANGUAGE plpgsql VOLATILE COST 11;
ALTER FUNCTION filloutlines() OWNER TO postgres;
GRANT EXECUTE ON FUNCTION filloutlines() TO public;

""")

print("Converting from Lambert72 and adding route_ref to stops table")

filloutlines = db.proc('filloutlines()')
print(filloutlines())

# -- COPY stops (stopid, stopidentifier, description, street, municipality, parentmunicipality, lat, lon, route_ref) TO 'C:/Data/De Lijn/De Lijnstops.csv' DELIMITERS '#' CSV;

print("Creating index on description and spatial column containing coordinates from De Lijn")
createDB = db.execute("""

CREATE INDEX ix_geomDL ON stops USING gist(geomDL);

""")

createDB = db.execute("""
DROP TABLE IF EXISTS zones;

CREATE TABLE zones
(
  zoneid serial NOT NULL,
  zone text,
  geomzone geometry,
  CONSTRAINT zones_pkey PRIMARY KEY (zoneid)
)
WITH (
  OIDS=FALSE
);
--SELECT AddGeometryColumn ('public','zones','geomzone',4326,'POLYGON',2);
CREATE INDEX ix_geomzone ON zones USING gist(geomzone);
ALTER TABLE zones
  OWNER TO postgres;
GRANT ALL ON TABLE zones TO postgres;
GRANT ALL ON TABLE zones TO "Jo";
""")
print("Vacuum analyze to gather statistics for efficient use of indexes")


createDB = db.execute("""

VACUUM ANALYZE --VERBOSE;

""")

print("Creating a stored procedure for later use")
createDB = db.execute("""

CREATE OR REPLACE FUNCTION AllTripsForARouteVerbose(routeidentifierparam text) RETURNS table(tripid int,routeidentifier text,routedescription text,tripstart text,start text,terminus text) AS $BODY$
  BEGIN
    RETURN QUERY 

		SELECT DISTINCT
		  trp.tripid,
		  rte.routeidentifier,
		  rte.routedescription,
		  trp.tripstart,
		  (SELECT 
			st.description
			FROM 
			  public.stops st
			JOIN public.segments seg1 ON seg1.stopid = st.stopid AND seg1.tripid = trp.tripid
			WHERE 
			  seg1.segmentsequence = (SELECT MIN(seg2.segmentsequence) FROM public.segments seg2 WHERE seg2.tripid = trp.tripid)) AS Start,
		  (SELECT 
			st.description
			FROM 
			  public.stops st
			JOIN public.segments seg1 ON seg1.stopid = st.stopid AND seg1.tripid = trp.tripid
			WHERE 
			  seg1.segmentsequence = (SELECT MAX(seg2.segmentsequence) FROM public.segments seg2 WHERE seg2.tripid = trp.tripid)) AS Terminus
		FROM public.trips    trp
		JOIN public.routes   rte      ON rte.routeid=trp.routeid
		JOIN public.segments seg      ON seg.tripid=trp.tripid
		JOIN public.stops    stp      ON seg.stopid=stp.stopid
		WHERE
		  rte.routeidentifier = routeidentifierparam
		ORDER BY
		  trp.tripstart ASC;
  END; $BODY$
LANGUAGE plpgsql VOLATILE COST 100;
ALTER FUNCTION AllTripsForARouteVerbose(text) OWNER TO postgres;

""")

#import IntegrateStopsFromOSM
#IntegrateStopsFromOSM.main('C:/Data/OSM/Overpass API queries/PT.osm')

import SaveStopsFromOSMasCSV
SaveStopsFromOSMasCSV.main()

#import UpdateZonesFromPDF
#UpdateZonesFromPDF.main('C:/Data/De Lijn/zones.txt')

import DeLijnData_in_Postgis_2_OSM
DeLijnData_in_Postgis_2_OSM.main()

import CreateWikiReport
CreateWikiReport.main()