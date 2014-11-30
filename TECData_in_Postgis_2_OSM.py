#!/bin/python
# -*- coding: utf-8 -*-
import postgresql, delijnosmlib

db = postgresql.open('pq://Jo:tttttt@localhost:5432/DL')
TECStops = db.prepare("""SELECT    geomtec,
                                   stopidentifier,
                                   descriptionfr AS description,
                                   description_normalised,
                                   streetfr AS street,
                                   municipalityfr AS municipality,
                                   tec.route_ref,
                                   bustram,
                                   osm.zone,
                                   osm.name,
                                   osm.node_ID,
                                   round(ST_X(geomtec)::numeric, 6) AS lon,
                                   round(ST_Y(geomtec)::numeric, 6) AS lat
                              FROM stops_tec AS tec
                              JOIN stops_osm AS osm ON stopidentifier = COALESCE(refTECL, refTECX, refTECN, refTECH, refTECB, refTECC, ref)
                              GROUP BY stopidentifier, description,osm.node_ID,osm.zone,osm.name
                              ORDER BY geomtec;""")

updatedescription_normalised = db.prepare("""UPDATE stops_tec
                                              SET description_normalised = $2
                                              WHERE descriptionfr = $1;""")
def main():
    with open('C:/Data/TEC/Stops TECdata.osm', mode='w', encoding='utf-8') as osmfile:
        osmfile.write("<?xml version='1.0' encoding='UTF-8'?>\n")
        osmfile.write(' <osm version="0.6" upload="no" generator="Python script">\n')
        osmfile.write(' <note>The data included in this document is from http://geoportail.wallonie.be. The data is made available under CC BY 4.0.</note>\n')
        osmfile.write('  <changeset>\n')
        osmfile.write('   <tag k="source" v="TEC"/>\n')
        osmfile.write('  </changeset>\n')
        stopslist = TECStops()
        identifiers = {}; descriptions = {}; counter = 100000
        for row in stopslist:
            counter +=1
            description = str(row['description'])
            osmname = str(row['name'])
            description_normalised = row['description_normalised']
            zone=str(row['zone'])
            #print (description + ' ' + description_normalised + ' ' + osmname)
            if not(description_normalised):
                name=delijnosmlib.nameconversion((description),zone)
                updatedescription_normalised(description,name)
            else:
                name = description_normalised
            if osmname != 'None' and not(name in identifiers) and osmname != name:
                identifiers[name]=''          
            if osmname != 'None' and not ('Perron' in osmname):
                on=osmname.replace("''","'")
                ds=delijnosmlib.omitlargercitynames(description,zone)
                if not(ds in descriptions) and on != ds and on != description:
                    descriptions[ds]=[description, on]

            name=delijnosmlib.xmlsafe(name).replace("<","").replace(">","")
            ref=str(row['stopidentifier'])
            street = city = route_ref = ''
            if row['street']: street=delijnosmlib.xmlsafe(str(row['street']))
            if row['municipality']: city=delijnosmlib.xmlsafe(str(row['municipality']))
            lat=str(row['lat'])
            lon=str(row['lon'])
            if row['route_ref']: route_ref=delijnosmlib.xmlsafe(row['route_ref'])
            if row['bustram']:
                bustram=str(row['bustram'])
            else:
                bustram='b'
            osmfile.write("  <node id='-" + str(counter) + "' visible='true' lat='" + lat + "' lon='" + lon + "' timestamp='2011-03-09T00:36:24Z' >" + '\n')
            if not '"' in name:
                osmfile.write('    <tag k="name" v="' + name + '" />' + "\n")
            else:
                osmfile.write("    <tag k='name' v='" + name + "' />" + '\n')
            osmfile.write('    <tag k="ref:TEC' + ref[0] + '" v="' + ref +'" />' + "\n")
            # if street or city:
                # osmfile.write('    <tag k="created_by" v="' + street.replace("<","").replace(">","") + ' ' + city.replace("<","").replace(">","") + '" />' + "\n")
            if row['zone']:
                osmfile.write('    <tag k="zone:TEC" v="' + row['zone'] + '" />' + "\n")
            if not(row['node_id']):
                osmfile.write('    <tag k="odbl" v="' + 'new' + '" />' + "\n")
            osmfile.write('    <tag k="operator" v="TEC" />' + "\n")
            osmfile.write('    <tag k="network" v="TEC' + ref[0] + '" />' + "\n")

            if route_ref:
                osmfile.write('    <tag k="route_ref:TEC' + ref[0] + '" v="' + route_ref + '" />' + "\n")
            if bustram and bustram != 'None':
                osmfile.write('    <tag k="public_transport" v="platform" />' + "\n")
                if 't' in bustram:
                    osmfile.write('    <tag k="tram" v="yes" />' + "\n")
                    osmfile.write('    <tag k="railway" v="tram_stop" />' + "\n")
                else: #if 'b' in bustram:
                    osmfile.write('    <tag k="bus" v="yes" />' + "\n")
                    osmfile.write('    <tag k="highway" v="bus_stop" />' + "\n")
            osmfile.write('  </node>' + "\n")
        osmfile.write('</osm>')

if __name__ == "__main__":
    main()