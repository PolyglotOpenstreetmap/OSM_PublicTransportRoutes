import postgresql, xml.sax, re, delijnosmlib, OSM_Data_Model
# -*- coding: utf-8 -*-

sixdigitsRE= re.compile(r'\d\d\d\d\d\d')
TECstopRE= re.compile(r'[BCNHXL].{4,7}')
db = postgresql.open('pq://Jo:tttttt@localhost:5432/DL')

nodeexists = db.prepare("""SELECT stopidentifier FROM stops WHERE OSM_node_ID = $1
                           UNION
                           SELECT stopidentifier FROM stops_tec WHERE OSM_node_ID = $1;""")
# refexists = db.prepare("""SELECT OSM_node_ID, description
                           # FROM stops
                           # WHERE stopidentifier::TEXT = $1
                          # UNION
                          # SELECT OSM_node_ID, description_normalised
                           # FROM stops_tec
                           # WHERE stopidentifier = $1;""")
refdlexists = db.prepare("""SELECT OSM_node_ID, description
                             FROM stops
                             WHERE stopidentifier = $1;""")
reftecexists = db.prepare("""SELECT OSM_node_ID, description_normalised
                              FROM stops_tec
                              WHERE stopidentifier = $1;""")
purgeOSMdata = db.prepare("""UPDATE stops SET OSM_node_ID = NULL,
                                              OSM_name = NULL,
                                              OSM_city = NULL,
                                              OSM_street = NULL,
                                              OSM_operator = NULL,
                                              OSM_route_ref = NULL,
                                              OSM_source = NULL,
                                              OSM_last_modified_by_user = NULL,
                                              OSM_last_modified_timestamp = NULL,
                                              OSM_zone = NULL
                                           WHERE
                                              OSM_node_ID IS NOT NULL;""")
purgeOSMdTEC = db.prepare("""UPDATE stops_tec SET OSM_node_ID = NULL,
                                              OSM_name = NULL,
                                              OSM_city = NULL,
                                              OSM_street = NULL,
                                              OSM_operator = NULL,
                                              OSM_route_ref = NULL,
                                              OSM_source = NULL,
                                              OSM_last_modified_by_user = NULL,
                                              OSM_last_modified_timestamp = NULL,
                                              OSM_zone = NULL
                                           WHERE
                                              OSM_node_ID IS NOT NULL;""")

updateOSMdata = db.prepare("""UPDATE stops SET OSM_node_ID = $1,
                                              OSM_name = $2,
                                              OSM_city = $3,
                                              OSM_street = $4,
                                              OSM_operator = $5,
                                              OSM_route_ref = $6,
                                              OSM_source = $7,
                                              OSM_last_modified_by_user = $8,
                                              OSM_last_modified_timestamp = $9::TEXT::TIMESTAMP,
                                              OSM_zone = $10,
                                              description_normalised = $11,
                                              geomOSM = ST_SetSRID(ST_MakePoint($12::TEXT::DOUBLE PRECISION, $13::TEXT::DOUBLE PRECISION),4326)
                                           WHERE
                                              stopidentifier = $14::TEXT;;""")
updateOSMdataTEC = db.prepare("""UPDATE stops_tec SET OSM_node_ID = $1,
                                              OSM_name = $2,
                                              OSM_city = $3,
                                              OSM_street = $4,
                                              OSM_operator = $5,
                                              OSM_route_ref = $6,
                                              OSM_source = $7,
                                              OSM_last_modified_by_user = $8,
                                              OSM_last_modified_timestamp = $9::TEXT::TIMESTAMP,
                                              OSM_zone = $10,
                                              description_normalised = $11,
                                              geomOSM = ST_SetSRID(ST_MakePoint($12::TEXT::DOUBLE PRECISION, $13::TEXT::DOUBLE PRECISION),4326)
                                           WHERE
                                              stopidentifier = $14::TEXT;""")

class OSMContentHandler(xml.sax.ContentHandler):
    def __init__(self):
        xml.sax.ContentHandler.__init__(self)
        self.nodeattributes = {}
        self.tags = {}
        print("Purge OSM data from database first")
        purgeOSMdata()
        purgeOSMdTEC()
        print("Updating database")

    def startElement(self, tagname, attrs):
        if tagname == "tag":
            self.tags[attrs.getValue("k")] = attrs.getValue("v")
        elif tagname == "node":
            self.nodeattributes = attrs
            self.tags = {}

    def endElement(self, tagname):
        name = operator = ref = route_ref = source = zone = addr_city = addr_street = user = timestamp = dl_ref = tec_ref = None
        #updatequery = "UPDATE stops SET "

        if tagname == "node":
            if 'highway' in self.tags and self.tags['highway'] in ['bus_stop', 'bus_station'] or 'railway' in self.tags and self.tags['railway'] in ['tram_stop', 'station'] or 'public_transport' in self.tags and self.tags['public_transport'] in ['platform']:
                #if self.nodeattributes["id"][0]=='-': print('Negative node id, upload your data first, then save in JOSM'); sys.exit()
                if 'action' in self.nodeattributes and self.nodeattributes["action"]== 'delete': return

                # First try to find out which record to update
                # Do we have a ref? 
                if 'ref' in self.tags and self.tags['ref']!='noref':
                    if ';' in self.tags['ref']: print('some bus stop nodes seem to have been merged' + self.tags['ref'])
                    dl_ref = re.match(sixdigitsRE, self.tags['ref'])
                    tec_ref = re.match(TECstopRE, self.tags['ref'])
                    if dl_ref or tec_ref:
                        ref = self.tags['ref']
                        # try:
                            # if tec_ref: print (ref, dl_ref, tec_ref, reftecexists(ref))
                        # except:
                            # print (ref, dl_ref, tec_ref)
                        refalreadyexists = None; description = "Not found"
                        if dl_ref: refalreadyexists, description = refdlexists(ref)[0]
                        elif tec_ref:
                            reftecexistsresult = reftecexists(ref)
                            if reftecexistsresult: refalreadyexists, description = reftecexistsresult[0]
                        #print (ref + ' ' + str(refalreadyexists))
                        if refalreadyexists:
                            print (str(refalreadyexists) + ' ' + ref + ' is used twice, better fix this')

                    #if nodeexists(str(self.nodeattributes["id"])): print(self.nodeattributes["id"] + " already exists in the database"); pass
                if 'name' in self.tags:
                    name = self.tags['name'].replace("'","''") # updatequery += "OSM_name = '" + self.tags['name'].replace("'","''") + "',"
                if 'operator' in self.tags:
                    operator = self.tags['operator'] # updatequery += "OSM_operator = '" + self.tags['operator'] + "',"
                if 'route_ref' in self.tags:
                    route_ref = self.tags['route_ref'] # updatequery += "OSM_route_ref = '" + self.tags['route_ref'] + "',"
                if 'source' in self.tags:
                    source = self.tags['source'] # updatequery += "OSM_source = '" + self.tags['source'] + "',"
                if 'addr:city' in self.tags:
                    addr_city = self.tags['addr:city'].replace("'","''") # updatequery += "OSM_city = '" + self.tags['addr:city'].replace("'","''") + "',"
                if 'addr:street' in self.tags:
                    addr_street = self.tags['addr:street'].replace("'","''") # updatequery += "OSM_street = '" + self.tags['addr:street'].replace("'","''") + "',"
                if 'zone' in self.tags:
                    zone = self.tags['zone'] # updatequery += "OSM_zone = '" + self.tags['zone'] + "',"
                if 'user' in self.nodeattributes:
                    user = self.nodeattributes["user"]
                    #updatequery += "OSM_last_modified_by_user = '" + self.nodeattributes["user"] + "',"
                if 'timestamp' in self.nodeattributes:
                    timestamp = self.nodeattributes["timestamp"]
                    #updatequery += "OSM_last_modified_timestamp = '" + self.nodeattributes["timestamp"] + "',"
                #csvfile.write(self.nodeattributes['id'] + '#' + ref + '#' + zone + '#' + self.nodeattributes['lat'] + '#' + self.nodeattributes['lon'] + '#' + name + '#' + addr_city + '#' + addr_street + '#' + operator + '#' + route_ref + '#' + source + '\n')   
                if True: # updatequery[-1] == ',':
                    if dl_ref:
                        
                        updateOSMdata(self.nodeattributes["id"], 
                                      name,
                                      addr_city,
                                      addr_street,
                                      operator,
                                      route_ref,
                                      source,
                                      user,
                                      timestamp,
                                      zone,
                                      delijnosmlib.nameconversion((description),zone),
                                      self.nodeattributes['lon'],
                                      self.nodeattributes['lat'],
                                      ref)

                    elif tec_ref:
                        
                        updateOSMdataTEC(self.nodeattributes["id"], 
                                          name,
                                          addr_city,
                                          addr_street,
                                          operator,
                                          route_ref,
                                          source,
                                          user,
                                          timestamp,
                                          zone,
                                          delijnosmlib.nameconversion((description),zone),
                                          self.nodeattributes['lon'],
                                          self.nodeattributes['lat'],
                                          ref)

                        '''
                        updatequery += """geomOSM = ST_SetSRID(ST_MakePoint(%s, %s),4326),
                                          OSM_node_ID = '%s'
                                          WHERE stopidentifier = '%s';
                                       """ % (self.nodeattributes['lon'], self.nodeattributes['lat'],
                                              self.nodeattributes["id"],
                                              ref)
                        #print (updatequery)
                        '''
                        #db.execute(updatequery)
                        #quit()
def main(sourceFileName):
    source = open(sourceFileName, encoding='utf-8')
    xml.sax.parse(source, OSMContentHandler())
    print("Database updated with stops from " + sourceFileName)
 
if __name__ == "__main__":
    main('C:/Data/OSM/Overpass API queries/PT.osm')