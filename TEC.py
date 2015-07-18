import postgresql
import glob, os, zipfile
#import conversion

# -*- coding: utf-8 -*-

db = postgresql.open('pq://Jo:tttttt@localhost:5432/DL')

print("Removing files from previous unzip")
for g in ['/data/tec/*.BLK', '/data/tec/*.CAR', '/data/tec/*.HRA', '/data/tec/*.NTE', '/data/tec/*.OPR', '/data/tec/*.STP', '/data/tec/*.VAL', '/data/tec/*.VER']:
    print(g)
    for f in glob.glob(g):
       os.unlink (f)

for f in glob.glob('/data/tec/TEC*.zip'):
    print(f)
    fh=zipfile.ZipFile(f)
    print(fh)
    fh.extractall('/data/tec')
   
print("Creating table for zones")
createDB = db.execute("""

DROP TABLE IF EXISTS zones;
CREATE TABLE zones
 ( stopidentifier text NOT NULL PRIMARY KEY, zone text)
  WITH ( OIDS=FALSE );
ALTER TABLE zones OWNER TO postgres;

""")

db.execute("""COPY zones FROM 'C:/Data/TEC/zones_TEC.csv' DELIMITERS '|' CSV;""")

# print("Create index on zones")
# createDB = db.execute("""
# DROP INDEX IF EXISTS ix_citiesnormalised;
# CREATE INDEX ix_citiesnormalised ON zones(zone);
# """)

print("Creating table for cities")
createDB = db.execute("""

DROP TABLE IF EXISTS cities;
CREATE TABLE cities
 ( AllCaps text NOT NULL PRIMARY KEY, Normalised text)
  WITH ( OIDS=FALSE );
ALTER TABLE cities OWNER TO postgres;

""")

db.execute("""COPY cities FROM 'C:/Data/De Lijn/BelgianCities.csv' DELIMITERS '|' CSV;""")

print("Create index on cities (normalised)")
createDB = db.execute("""
DROP INDEX IF EXISTS ix_citiesnormalised;
CREATE INDEX ix_citiesnormalised ON cities(Normalised);
""")

print("Creating table for stops")
createDB = db.execute("""

DROP TABLE IF EXISTS stops_TEC;
CREATE TABLE stops_TEC
 ( stopidentifier text NOT NULL PRIMARY KEY, descriptionNL text, descriptionFR text, municipalityNL text, municipalityFR text, country text, streetNL text, streetFR text, ARI text, stopisaccessible INT, x INT, y INT, stopispublic INT, UIC text)
  WITH ( OIDS=FALSE );
ALTER TABLE stops_TEC OWNER TO postgres;

""")

stopsFN = 'C:/Data/TEC/TECstops.txt'
with open(stopsFN, 'w') as stopsFH:
    for fn in glob.glob('C:/Data/TEC/*.STP'):
        print(fn)
        shortFN = fn.split("\\")[-1]
        entity = shortFN[3]
        print(fn, shortFN, entity)
        with open(fn, 'r') as currentSTPfile:
            for line in currentSTPfile:
                entity4thisStop = line[0]
                #print (entity4thisStop)
                if entity4thisStop == entity:
                    stopsFH.write(line)
                    #print (line)
                
db.execute("""COPY stops_TEC FROM '""" + stopsFN + """' DELIMITERS '|' CSV ENCODING 'LATIN1';""")
    
print("Altering table for stops")
createDB = db.execute("""
ALTER TABLE stops_TEC
 ADD COLUMN description_normalised text,
 ADD COLUMN lat DOUBLE PRECISION, 
 ADD COLUMN lon DOUBLE PRECISION,
 ADD COLUMN osm_zone text,
 ADD COLUMN route_ref text,
 ADD COLUMN bustram text,
 ADD COLUMN zoneid integer;

SELECT AddGeometryColumn ('public','stops_tec','geomtec',4326,'POINT',2);
SELECT AddGeometryColumn ('public','stops_tec','geomosm',4326,'POINT',2);
""")

createDB = db.execute("""
DROP TABLE IF EXISTS routes_TEC;
CREATE TABLE routes_TEC
 ( routeid text NOT NULL PRIMARY KEY, routename text, routedescription1 text, routedescription2 text, routepublicidentifier text )
  WITH ( OIDS=FALSE );
ALTER TABLE routes_TEC OWNER TO postgres;
""")

createDB = db.execute("""
DROP TABLE IF EXISTS trips_TEC;
CREATE TABLE trips_TEC
 ( tripid text NOT NULL PRIMARY KEY, routeid text, direction integer, mode integer, type integer )
   WITH ( OIDS=FALSE );
ALTER TABLE trips_TEC OWNER TO postgres;
""")

createDB = db.execute("""
DROP TABLE IF EXISTS segments_TEC;
CREATE TABLE segments_TEC
 ( segmentid serial NOT NULL PRIMARY KEY, tripid text, stopid text, segmentsequence int, time text)
  WITH ( OIDS=FALSE );
ALTER TABLE segments_TEC OWNER TO postgres;
""")

for fo in glob.glob('C:/Data/TEC/*.CAR'):
    print(fo)
    prefix=fo[-13]
    fnr=fo.replace('CAR', 'rCSV')
    fhr=open(fnr, 'w')
    fnt=fo.replace('CAR', 'tCSV')
    fht=open(fnt, 'w')
    for line in open(fo,'r'):
        if len(line)>2:
            if line[0] == '@':
                route_id, name, desc1, desc2, public_id, dummy1, dummy2 = line[1:].split('|')
                fhr.write(prefix + route_id + '|' + name + '|' + desc1 + '|' + desc2 + '|' + public_id + '\n')

            else:
                trip_id, route_id, dir, mode, type = line.split('|')
                fht.write(prefix + trip_id + '|' + prefix + route_id + '|' + dir + '|' + mode + '|' + type)
    fhr.close()
    fht.close()
    db.execute("""COPY routes_TEC (routeid, routename, routedescription1, routedescription2, routepublicidentifier) FROM '""" + fnr + """' DELIMITER '|' CSV ENCODING 'LATIN1';""")
    db.execute("""COPY trips_TEC (tripid, routeid, direction, mode, type) FROM '""" + fnt + """' DELIMITER '|' CSV ENCODING 'LATIN1';""")

for fo in glob.glob('C:/Data/TEC/*.HRA'):
    print(fo)
    prefix=fo[-13]
    tripid=None
    fn=fo.replace('HRA', 'CSV')
    fh=open(fn, 'w')
    for line in open(fo,'r'):
        #print(line, len(line))
        if len(line)>6:
            if line[0] == '#':
                tripid=line[1:].strip()
                sequence=1
                continue
            elif line[0] in '.><':
                stop_id, time        = line[1:].split('|')
                fh.write(prefix + tripid + '|' + stop_id + '|' + str(sequence) + '|' + time)
                sequence+=1
            elif line[0] == '+':
                stop_id, time, time2 = line[1:].split('|')
                fh.write(prefix + tripid + '|' + stop_id + '|' + str(sequence) + '|' + time + '\n')
                sequence+=1

    db.execute("""COPY segments_TEC (tripid, stopid, segmentsequence, time) FROM '""" + fn + """' DELIMITER '|' CSV ENCODING 'LATIN1';""")

                
print("Create index on routes (routeidentifier), trips(routeid) and on segments (tripid)")
createDB = db.execute("""
DROP INDEX IF EXISTS ix_tripsrouteidtec;
DROP INDEX IF EXISTS ix_segmentstripidtec;
DROP INDEX IF EXISTS ix_segmentsstopidtec;
CREATE INDEX ix_tripsrouteidtec ON trips_tec(routeid);
CREATE INDEX ix_segmentstripidtec ON segments_tec (tripid);
CREATE INDEX ix_segmentsstopidtec ON segments_tec (stopid);
""")

print("Vacuum analyze to gather statistics for efficient use of indexes")
createDB = db.execute("""

VACUUM ANALYZE --VERBOSE;

""")

print("Creating stored procedures")
createDB = db.execute("""
CREATE OR REPLACE FUNCTION AllLinesPassingAtaTECStop(stopidentifierparameter text) RETURNS text AS $BODY$
  DECLARE outlines text :='';
    l record;
    line text;
    bustram text:='';
  BEGIN
    FOR l IN SELECT distinct(lpad(rte.routepublicidentifier, 5, '0')) AS publicidentifier
				FROM public.trips_TEC    trp
				JOIN public.routes_TEC   rte      ON rte.routeid=trp.routeid
				JOIN public.segments_TEC seg      ON seg.tripid=trp.tripid
				JOIN public.stops_TEC    stp      ON seg.stopid=stp.stopidentifier
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
    END LOOP;

    RETURN trim(both ';' FROM outlines) || ',' || bustram;
  END $BODY$
LANGUAGE plpgsql VOLATILE COST 100;
ALTER FUNCTION AllLinesPassingAtaTECStop(text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION AllLinesPassingAtaTECStop(text) TO public;

CREATE OR REPLACE FUNCTION filloutlines() RETURNS void AS $BODY$
  DECLARE
    l record;
    res text;
    b text;
    a int4;
    coords geometry;
    vlat double precision;
    vlon double precision;
    cityallcaps text;
    normalisedname text;
    description text;

  BEGIN
    DROP INDEX IF EXISTS ix_geomTEC;
    FOR l IN SELECT stopidentifier, descriptionfr, x, y FROM stops_TEC
      LOOP res := AllLinesPassingAtaTECStop(l.stopidentifier);
        coords := st_transform(st_setSRID(st_Point(l.x, l.y), 31370),4326);
        vlat := st_y(coords);
        vlon := st_x(coords);
        description := replace(l.descriptionfr, 'LE ROEULX', 'LE_ROEULX');
        description := replace(description, 'LE MESNIL', 'LE_MESNIL');
        description := replace(description, 'LE BRULY', 'LE_BRULY');
        description := replace(description, 'LE ROUX', 'LE_ROUX');
        description := replace(description, 'LE ROEULX', 'LE_ROEULX');
        description := replace(description, 'LA BOUVERIE', 'LA_BOUVERIE');
        description := replace(description, 'LA CALAMINE', 'LA_CALAMINE');
        description := replace(description, 'LA LOUVIERE', 'LA_LOUVIERE');
        description := replace(description, 'LA HESTRE', 'LA_HESTRE');
        description := replace(description, 'LA ROCHE-EN-ARDENNE', 'LA_ROCHE-EN-ARDENNE');
        description := replace(description, 'LA GLANERIE', 'LA_GLANERIE');
        description := replace(description, 'LA GLEIZE', 'LA_GLEIZE');
        description := replace(description, 'PETIT RY', 'PETIT_RY');
        description := replace(description, 'Eglise', 'Église');
        description := replace(description, 'Ecole', 'École');
        description := replace(description, 'St-', 'Saint-');
        description := replace(description, 'Ste-', 'Sainte-');
        description := replace(description, 'Av.', 'Avenue');
        cityallcaps := split_part(description,' ',1);
        SELECT normalised INTO normalisedname FROM cities WHERE AllCaps = cityallcaps;

      UPDATE stops_TEC
        SET route_ref=split_part(res,',',1),
          bustram=split_part(res,',',2),
          geomTEC = coords,
          lat = vlat,
          lon = vlon,
          description_normalised = replace(description,cityallcaps,normalisedname)
        WHERE stops_TEC.stopidentifier=l.stopidentifier;
	  RAISE NOTICE  '% set to %',l.stopidentifier, res;
     END LOOP;
   END 
$BODY$ LANGUAGE plpgsql VOLATILE COST 11;
ALTER FUNCTION filloutlines() OWNER TO postgres;
GRANT EXECUTE ON FUNCTION filloutlines() TO public;

CREATE OR REPLACE FUNCTION filloutzones() RETURNS void AS $BODY$
  DECLARE
    l record;
    res text;
    vzone text;

  BEGIN
    FOR l IN SELECT description_normalised FROM stops_TEC WHERE osm_zone IS NULL
      LOOP 
        SELECT osm_zone INTO vzone FROM stops_TEC s WHERE s.description_normalised = l.description_normalised;

      UPDATE stops_TEC
        SET osm_zone=vzone
        WHERE stops_TEC.description_normalised=l.description_normalised;
	  -- RAISE NOTICE  '% set to %'l.zone,vzone;
     END LOOP;
   END 
$BODY$ LANGUAGE plpgsql VOLATILE COST 11;
ALTER FUNCTION filloutzones() OWNER TO postgres;
GRANT EXECUTE ON FUNCTION filloutzones() TO public;

""")

print("Converting from Lambert72 and adding route_ref to stops table")

filloutlines = db.proc('filloutlines()')
print(filloutlines())

print("Adding zones")
createDB = db.execute("""UPDATE public.stops_tec s
SET osm_zone=(SELECT z.zone FROM zones z WHERE s.stopidentifier = z.stopidentifier);

DROP INDEX IF EXISTS ix_zonestec;
CREATE INDEX ix_zonestec ON zones (zone);
""")

filloutzones = db.proc('filloutzones()')
print(filloutzones())