#!/bin/python
# -*- coding: utf-8 -*-
import postgresql, delijnosmlib

db = postgresql.open('pq://Jo:tttttt@localhost:5432/DL')
DeLijnStops = db.prepare("""SELECT geomdl,
                                       stopidentifier,
                                       description,
                                       description_normalised,
                                       street,
                                       municipality,
                                       dl.route_ref,
                                       bustram,
                                       osm.zone,
                                       osm.name,
                                       osm.node_ID,
                                       round(ST_X(geomdl)::numeric, 6) AS lon,
                                       round(ST_Y(geomdl)::numeric, 6) AS lat
                                  FROM stops AS dl
                                  JOIN stops_osm AS osm ON stopidentifier = COALESCE(refDL, ref)
                                  WHERE description !~* 'dummy|afgeschaft'
                                  GROUP BY geomdl,stopidentifier, description,description_normalised,street,municipality,osm.node_ID,dl.route_ref,bustram,osm.zone,osm.name
                                  ORDER BY geomdl;""")

updatedescription_normalised = db.prepare("""UPDATE stops
                                              SET description_normalised = $2
                                              WHERE description = $1;""")
def main():
    with open('C:/Data/De Lijn/Haltes De Lijndata.osm', mode='w', encoding='utf-8') as osmfile:
        osmfile.write("<?xml version='1.0' encoding='UTF-8'?>\n")
        osmfile.write(' <osm version="0.6" upload="no" generator="Python script">\n')
        osmfile.write('  <changeset>\n')
        osmfile.write('   <tag k="source" v="De Lijn"/>\n')
        osmfile.write('  </changeset>\n')
        stopslist = DeLijnStops()
        identifiers = {}; descriptions = {}
        deLijnNetworks = ['','DLAn','DLOV','DLVB','DLLi','DLWV']
        #deLijnNetworks = ['','Antwerpen','Oost-Vlaanderen','Vlaams-Brabant','Limburg','West-Vlaanderen']
        for row in stopslist:
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
            # if not(name in identifiers) and osmname != name and (' ' in osmname) and (' ' in name) and (str(row['osm_zone']) not in ('01','20')) and not ('erron' in name) and not ('oeren' in name) and not ('Dokter' in name) and not (' nummer' in name) and not (' naar ' in name) and not ('École' in name) and not ('steenweg' in name) and not ('Goor' in name) and not ('str.' in name) and not ('Ziekenhuis' in name) and not ('Koninklijk' in name) and not ('Ernest Claes' in name) and not ('Heilig' in name) and not ('Lieve-Vrouw' in name) and not ('Siméon' in name) and not ('Streuvels' in name) and not ('Moelingen' in name) and not ("'t" in name) and not ("'s" in name):
            if osmname != 'None' and not(name in identifiers) and osmname != name:
                identifiers[name]=''
                # try:
                    #print (description + ' -> ' + osmname + ' -> ' + name)
                    #print(r'''    (r"(?ui)\b''' + description.split(' ',1)[1].replace(' ','\s*').replace('.','\.*').replace('straat','').replace('laan','').replace('plein','').replace('lei','') + '''","''' + osmname.split(' ',1)[1].replace("''","'").replace('straat','').replace('laan','').replace('plein','').replace('lei','') + '''"),''')
                    # print(r'''    (r"(?ui)\b''' + description.replace(' ','\s*').replace('.','\.*').replace('straat','').replace('laan','').replace('plein','').replace('lei','') + '''","''' + osmname.replace("''","'").replace('straat','').replace('laan','').replace('plein','').replace('lei','') + '''"),''')
                # except:
                    # print('''your command shell is too lame to print UTF-8''')                
            if osmname != 'None' and not ('Perron' in osmname):
                on=osmname.replace("''","'")
                ds=delijnosmlib.omitlargercitynames(description,zone)
                if not(ds in descriptions) and on != ds and on != description:
                    descriptions[ds]=[description, on]

            name=delijnosmlib.xmlsafe(name)
            ref=row['stopidentifier']
            street = city = city2 = route_ref = ''
            if row['street']: street=delijnosmlib.xmlsafe(str(row['street']))
            if row['municipality']: city=delijnosmlib.xmlsafe(str(row['municipality']))
            lat=str(row['lat'])
            lon=str(row['lon'])
            if row['route_ref']: route_ref=delijnosmlib.xmlsafe(row['route_ref'])
            if row['bustram']: bustram=str(row['bustram'])

            osmfile.write("  <node id='-" + ref + "' visible='true' lat='" + lat + "' lon='" + lon + "' timestamp='2011-03-09T00:36:24Z' >" + '\n')
            if not '"' in name:
                osmfile.write('    <tag k="name" v="' + name + '" />' + "\n")
            else:
                osmfile.write("    <tag k='name' v='" + name + "' />" + '\n')
            osmfile.write('    <tag k="ref:De_Lijn" v="' + ref +'" />' + "\n")
            if street or city:
                osmfile.write('    <tag k="created_by" v="' + street + ' ' + city + '" />' + "\n")
            #print( row)
            if row['zone']:
                osmfile.write('    <tag k="zone:De_Lijn" v="' + row['zone'] + '" />' + "\n")
            if not(row['node_id']):
                osmfile.write('    <tag k="odbl" v="' + 'new' + '" />' + "\n")
            osmfile.write('    <tag k="operator" v="De Lijn" />' + "\n")
            osmfile.write('    <tag k="network" v="' + deLijnNetworks[int(ref[0])] + '" />' + "\n")

            if route_ref:
                osmfile.write('    <tag k="route_ref:De_Lijn" v="' + route_ref + '" />' + "\n")
            if bustram and bustram != 'None':
                osmfile.write('    <tag k="public_transport" v="platform" />' + "\n")
                if 'b' in bustram:
                    osmfile.write('    <tag k="bus" v="yes" />' + "\n")
                    osmfile.write('    <tag k="highway" v="bus_stop" />' + "\n")
                if 't' in bustram:
                    osmfile.write('    <tag k="tram" v="yes" />' + "\n")
                    osmfile.write('    <tag k="railway" v="tram_stop" />' + "\n")
            osmfile.write('  </node>' + "\n")
        osmfile.write('</osm>')
        # with open('C:/Data/De Lijn/unit_tests_raw_new.py', mode='w', encoding='utf-8') as utfile:
            # newdict = {}
            # for key in descriptions:
                # print (descriptions[key][0] + ' ' + descriptions[key][1])
                # newdict[descriptions[key][0]] = descriptions[key][1]
            # print(repr(newdict))
            # for description in sorted(newdict.keys()):
                # print(description)
                # utfile.write('''        ("''' + description + '''", "''' + newdict[description] + '''"),\n''')

if __name__ == "__main__":
    main()