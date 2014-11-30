import postgresql, xml.sax, re, delijnosmlib
# -*- coding: utf-8 -*-

sixdigitsRE= re.compile(r'\d\d\d\d\d\d')
TECstopRE= re.compile(r'[BCNHXL].{4,7}')
db = postgresql.open('pq://Jo:tttttt@localhost:5432/DL')

print("Creating table for stops from OSM")
createDB = db.execute("""
DROP TABLE IF EXISTS stops_OSM;
CREATE TABLE stops_OSM
 ( stopsPK serial NOT NULL PRIMARY KEY,
   lat DOUBLE PRECISION, 
   lon DOUBLE PRECISION, 
   last_change_timestamp timestamp,
   ref text,                            -- this corresponds to stopidentifier
   refDL text,
   refTECB text,
   refTECC text,
   refTECH text,
   refTECN text,
   refTECL text,
   refTECX text,
   name text,
   namenl text,
   namefr text,
   nameen text,
   namede text,
   nameDL text,
   nameTEC text,   
   operator text,
   route_ref text,
   route_refDL text,
   route_refTECB text,
   route_refTECC text,
   route_refTECH text,
   route_refTECN text,
   route_refTECL text,
   route_refTECX text,
   zone text,
   zoneDL text,
   zoneTEC text,
   source text,
   node_ID text,                        -- over time a stop may have used different nodes
   version int,
   last_modified_by text)
  WITH ( OIDS=FALSE );
ALTER TABLE stops OWNER TO postgres;

SELECT AddGeometryColumn ('public','stops_osm','geomosm',4326,'POINT',2);

""")

refdlexists = db.prepare("""SELECT node_ID, description, stopidentifier, name, namenl, namefr
                             FROM stops, stops_OSM
                             WHERE stopidentifier = $1
                               AND stopidentifier = ref;""")
reftecexists = db.prepare("""SELECT node_ID, description_normalised, stopidentifier, name, namenl, namefr
                              FROM stops_tec, stops_OSM
                              WHERE stopidentifier = $1
                                AND stopidentifier = ref;""")

class OSMContentHandler(xml.sax.ContentHandler):
    def __init__(self, txtfile):
        xml.sax.ContentHandler.__init__(self)
        self.nodeattributes = {}
        self.tags = {}
        self.refs = {}
        self.txtfile = txtfile
        #print("Purge OSM data from database first")
        #purgeOSMdata()
        #purgeOSMdTEC()
        print("Writing to intermediary file")

    def startElement(self, tagname, attrs):
        if tagname == "tag":
            self.tags[attrs.getValue("k")] = attrs.getValue("v")
        elif tagname == "node":
            self.nodeattributes = attrs
            self.tags = {}

    def endElement(self, tagname):
         # initialise to NULL value
        attributes = ['lat', 'lon', 'id', 'timestamp', 'version', 'user']
        interestingTags = ['name', 'name:De_Lijn', 'name:TEC', 'name:nl', 'name:fr', 'name:en', 'name:de', 'ref', 'ref:De_Lijn', 'ref:TECB', 'ref:TECC', 'ref:TECH', 'ref:TECN', 'ref:TECL', 'ref:TECX', 'route_ref', 'route_ref:De_Lijn', 'route_ref:TECB', 'route_ref:TECC', 'route_ref:TECH', 'route_ref:TECN', 'route_ref:TECL', 'route_ref:TECX', 'zone', 'zone:De_Lijn', 'zone:TEC', 'operator', 'source' ]

        if tagname == "node":
            if  'highway' in self.tags \
               and self.tags['highway'] in ['bus_stop', 'bus_station'] \
             or 'railway' in self.tags \
               and self.tags['railway'] in ['tram_stop', 'station'] \
             or 'public_transport' in self.tags \
               and self.tags['public_transport'] in ['platform']:
                if 'action' in self.nodeattributes and self.nodeattributes["action"] == 'delete': return

                if 'ref' in self.tags:
                    if ';' in self.tags['ref']: print('some bus stop nodes seem to have been merged' + self.tags['ref'])

                    if self.tags['ref'] in self.refs:
                        print (self.tags['ref'] + ' is used twice, better fix this')
                        return
                    else:
                       self.refs[self.tags['ref']]=None
                line = ''
                for attribute in attributes:
                    if attribute in self.nodeattributes:
                        line += self.nodeattributes[attribute] + '|'
                    else:
                        line += r'\N' + '|'

                for tag in interestingTags:
                    if tag in self.tags:
                        line += self.tags[tag].replace("'","''").replace('''\\''','''\\\\''')  + '|'
                    else:
                        line += r'\N' + '|'

                self.txtfile.write(line[:-1] + '\n')   

def main():
    sourceFileName = 'C:/Data/OSM/Overpass API queries/PT.osm'
    source = open(sourceFileName, encoding='utf-8')
    targetFileName = r'C:/Data/osm/PTstops.txt'
    with open(targetFileName, mode='w', encoding='utf-8') as txtfile:
        xml.sax.parse(source, OSMContentHandler(txtfile))
    print("Adding trigger function to update geom_OSM")   
    db.execute("""
    CREATE OR REPLACE FUNCTION calculate_geomOSM() RETURNS trigger AS $body$
    BEGIN
        -- Check that empname and salary are given
        IF NEW.lat IS NULL THEN
            RAISE EXCEPTION 'lat cannot be null';
        END IF;
        IF NEW.lon IS NULL THEN
            RAISE EXCEPTION 'lon cannot be null';
        END IF;

        NEW.geomOSM = ST_SetSRID(ST_MakePoint(NEW.lon, NEW.lat),4326);

        RETURN NEW;
    END;
    $body$ LANGUAGE plpgsql;

    CREATE TRIGGER calculate_geomOSM BEFORE INSERT OR UPDATE ON stops_OSM
    FOR EACH ROW EXECUTE PROCEDURE calculate_geomOSM();  
    
    """)
    print("Stops from " + sourceFileName + " written to intermediary file")
    db.execute("""COPY stops_OSM (lat, lon, node_ID, last_change_timestamp, version, last_modified_by,
                                  name, nameDL, nameTEC, namenl, namefr, nameen, namede,
								  ref, refDL, refTECB, refTECC, refTECH, refTECN, refTECL, refTECX,
								  route_ref, route_refDL, route_refTECB, route_refTECC,
								  route_refTECH, route_refTECN, route_refTECL, route_refTECX,
								  zone, zoneDL, zoneTEC,
								  operator, source)
                FROM '""" + targetFileName + """'
                WITH DELIMITER '|';""")
    print("Stops from " + sourceFileName + " uploaded to database")   
    print("Adding indexes")   
    db.execute("""  CREATE INDEX ix_node_ID ON stops_OSM (node_ID);
                    CREATE INDEX ix_ref ON stops_OSM (ref);
                    CREATE INDEX ix_refDL ON stops_OSM (refDL);
                    CREATE INDEX ix_refTECB ON stops_OSM (refTECB);
                    CREATE INDEX ix_refTECC_ID ON stops_OSM (refTECC);
                    CREATE INDEX ix_refTECH ON stops_OSM (refTECH);
                    CREATE INDEX ix_refTECN ON stops_OSM (refTECN);
                    CREATE INDEX ix_refTECL ON stops_OSM (refTECL);
                    CREATE INDEX ix_refTECX ON stops_OSM (refTECX);""")

if __name__ == "__main__":
    main()