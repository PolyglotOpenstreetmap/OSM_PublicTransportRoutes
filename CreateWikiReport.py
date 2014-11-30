#!/bin/python
# -*- coding: utf-8 -*-
import postgresql, datetime, delijnosmlib, re
from urllib.parse import urlencode

feestbusRE=re.compile(r'''(F\d+)''')
feestbusREsub=re.compile(r'''(?:;)?F\d+(;)?''')
ignorevandeRE=re.compile(r'''(?u)\s((?:[Vv]an)*\s*(?:[Oo]p)*\s*(?:[Dd]e(r|n)*)*)\s''')
db = postgresql.open('pq://Jo:tttttt@localhost:5432/DL')
locationdiffers = db.prepare("""SELECT round(ST_Distance_Sphere(geomdl, geomosm)),
                                       stopidentifier, 
                                       description,
                                       description_normalised,
                                       name,
                                       dl.route_ref dl_route_ref, 
                                       osm.route_refDL osm_route_ref,
                                       bustram,
                                       zone,
                                       node_id,
                                       last_modified_by,
                                       now() - last_change_timestamp AS dayswithoutchanges,
                                       round(ST_X(ST_Centroid(ST_ShortestLine(geomdl, geomosm)))::numeric, 3) AS x,
                                       round(ST_Y(ST_Centroid(ST_ShortestLine(geomdl, geomosm)))::numeric, 3) AS y,
                                       round(ST_X(geomosm)::numeric, 3) AS lon,
                                       round(ST_Y(geomosm)::numeric, 3) AS lat
                                  FROM stops AS dl
                                  JOIN stops_osm AS osm ON stopidentifier = refDL
                                  WHERE ST_Distance_Sphere(geomdl, geomosm) > 35.0
                                  ORDER BY ST_Distance_Sphere(geomdl, geomosm) DESC;""")

otherdifferences = db.prepare("""SELECT stopidentifier, 
                                       description,
                                       description_normalised,
                                       name,
                                       dl.route_ref dl_route_ref, 
                                       osm.route_refDL osm_route_ref,
                                       bustram,
                                       zone,
                                       node_id,
                                       last_modified_by,
                                       now() - last_change_timestamp AS dayswithoutchanges,
                                       round(ST_X(geomosm)::numeric, 3) AS lon,
                                       round(ST_Y(geomosm)::numeric, 3) AS lat
                                  FROM stops AS dl
                                  JOIN stops_osm AS osm ON stopidentifier = refDL
                                  WHERE description != name OR dl.route_ref != osm.route_ref OR (zone IS NULL AND geomosm IS NOT NULL)  -- OR 
                                  --WHERE description LIKE '%.%' AND geomosm IS NOT NULL
                                  ORDER BY zone DESC, stopidentifier;""")
def straatlaanpleinlei(longstring):
    return longstring.replace('.','\.*').replace('straat','').replace('laan','').replace('plein','').replace('lei','')

def main():
    targetFileName = 'C:/Data/De Lijn/WikiReport.txt'
    with open(targetFileName, mode='w', encoding='utf-8') as wikifile:
        wikifile.write('''
Instructions on how this list was created can be found here:

http://wiki.openstreetmap.org/w/index.php?title=WikiProject_Belgium/De_Lijndata#Feedback_to_De_Lijn_about_stops_which_are_more_than_a_certain_distance_from_what_is_in_their_DB

==Stops which differ in location==
{| class="wikitable" align="left" style="margin:0 0 2em 2em;"
|-
|+De Lijn Haltes
|-
!Afstand
!Nummer
!Haltenaam
!name
!Straat
!straat
!stad
!Bediende lijnen
!route_ref
!zone
''')
        stopslist = locationdiffers()
        i=1
        while i< len(stopslist):
            j=i+1
            #print(len(stopslist[j:]))
            while j< len(stopslist[j:])+i:
                #print (stopslist[i]['name'], stopslist[j], stopslist[j]['name'])
                if stopslist[i]['description'] and stopslist[j]['description'] and stopslist[i]['description'][:16]==stopslist[j]['description'][:16]:
                    # print('flipping for ' + stopslist[j]['name'] + ' ' + str(i) + ' ' + str(j))
                    stopslist.insert(i+1, stopslist[j])
                    del stopslist[j+1]
                    i += 1
                    break
                j += 1
            i += 1
        for row in stopslist:
            wikifile.write('|-\r\n')
            josmRClink = '' ;tags2add = '&addtags='
            # print (row['dayswithoutchanges'])
            if not(row['last_modified_by']=='Polyglot')  or row['dayswithoutchanges'] > datetime.timedelta(30):
                if row['dl_route_ref'] != row['osm_route_ref']:
                    tags2add += 'route_ref:De_Lijn=' + str(row['dl_route_ref']) + '|'
                if row['description'] != row['name']:
                    #print ('test' + str(row['description']))
                    #for c in str(row['description']):
                    #    print(repr(c), ord(c))
                    tags2add += "name=" + str(row['description']).replace(' ','%20') + '|'
                #if row['street']: tags2add += "addr:street=|"
                #if row['city']: tags2add += "addr:city=|"
                josmRClink = '[http://localhost:8111/load_and_zoom?left=' + str(float(row['lon']) - 0.01) + '&right=' + str(float(row['lon']) + 0.01) + '&top=' + str(float(row['lat']) + 0.005) + '&bottom=' + str(float(row['lat']) - 0.005) + '&select=node' + str(row['node_id']) + tags2add + ' ' + str(row['stopidentifier']) + ']'
            else:
                josmRClink = str(row['stopidentifier'])
            wikifile.write('|align="right" | [http://tools.geofabrik.de/mc/?mt0=mapnik&mt1=googlemap&lon=' + str(row['x']) + '&lat=' + str(row['y']) + '&zoom=18 ' + str(int(row['round'])) + ']m||align="right" |' + josmRClink + '||align="right" | ' + str(row['description']) + '||align="right" | ' + str(row['name']) + '||align="right" | '  + str(row['dl_route_ref']) + '||align="right" | ' + str(row['osm_route_ref']) + '||align="right" | ' + str(row['zone']) + '\r\n')

        # stopslist = otherdifferences()
        # print ('query ready')
        i=1
        while i< len(stopslist):
            j=i+1
            #print(len(stopslist[j:]))
            while j< len(stopslist[j:])+i:
                #print (stopslist[i]['name'], stopslist[j], stopslist[j]['name'])
                if stopslist[i]['description'] and stopslist[j]['description'] and stopslist[i]['description'][:16]==stopslist[j]['description'][:16]:
                    #print('flipping for ' + stopslist[j]['name'] + ' ' + str(i) + ' ' + str(j))
                    stopslist.insert(i+1, stopslist[j])
                    del stopslist[j+1]
                    i += 1
                    break
                j += 1
            i += 1
        # print ('rows sorted')
        wikifile.write('''|-\r\n|}\r\n{| class="wikitable" align="left" style="margin:0 0 2em 2em;"
|-
|+De Lijn Haltes
|-
!Nummer
!Haltenaam<br/>name
!zone
!Bediende lijnen<br/>!route_ref
''')
        identifiers = {}
        for row in stopslist:
            josmRClink = '' ;tags2add = '&addtags='; dl_route_ref=''; osm_route_ref=''
            #print (row['dayswithoutchanges'])
            description = delijnosmlib.nameconversion(str(row['description']),'')
            name= str(row['name']).replace("''","'")
            if row['bustram']: bustram=str(row['bustram'])
            #if '.'  in str(row['description']):
            if not(name in identifiers) and str(row['description_normalised']) != name and (' ' in str(row['description_normalised'])) and '.' in str(row['description_normalised']) and not('.' in name) and not ('erron' in name) and not ('oeren' in name) and not ('Dokter' in name) and not (' nummer' in name) and not (' naar ' in name) and not ('École' in name) and not ('steenweg' in name) and not ('Goor' in name) and not ('str.' in name) and not ('Ziekenhuis' in name) and not ('Koninklijk' in name) and not ('Ernest Claes' in name) and not ('Heilig' in name) and not ('Lieve-Vrouw' in name) and not ('Siméon' in name) and not ('Streuvels' in name) and not ('Moelingen' in name) and not ("'t" in name) and not ("'s" in name):
                identifiers[name]=''

                print(r'''    (r"(?ui)\b''' + str(row['description']).split(' ',1)[1].replace(' ','\s*').replace('.','\.*').replace('straat','').replace('laan','').replace('plein','').replace('lei','') + '''","''' + name.replace("''","'").replace('straat','').replace('laan','').replace('plein','').replace('lei','') + '''"),''')
            name_different = route_ref_different = False
            if row['osm_route_ref']:
                route_ref=feestbusREsub.sub(' ',str(row['dl_route_ref']),10).strip()
                if str(row['osm_route_ref'])!= route_ref: route_ref_different = True

            if ignorevandeRE.sub(' ',name,0)!=ignorevandeRE.sub(' ',str(row['description_normalised']),0): name_different = True

            if name_different or route_ref_different:
                tags2add += 'route_ref:De_Lijn=' + str(row['dl_route_ref']) + '|'
                tags2add += "name=" + delijnosmlib.urlsafe(str(row['description_normalised'])) + '|'
                #if row['street']: tags2add += "addr:street=|"
                #if row['city']: tags2add += "addr:city=|"
                tags2add += "addr:country=|"
                tags2add += "addr:postcode=|"
                tags2add += "source=|"
                if bustram and bustram != 'None':
                    tags2add += "public_transport=platform|"
                    if 'b' in bustram:
                        tags2add += "bus=yes|"
                        tags2add += "highway=bus_stop|"
                    if 't' in bustram:
                        tags2add += "tram=yes|"
                        tags2add += "railway=tram_stop|"
                
                #print ( str(row['description']) + ' ' + row['lon'] + ' ' + row['lat'])
                extent = 0.0025
                josmRClink = '[http://localhost:8111/load_and_zoom?left=' + str(round(float(row['lon']) - extent,3)) + '&right=' + str(round(float(row['lon']) + extent,3)) + '&top=' + str(round(float(row['lat']) + extent,3)) + '&bottom=' + str(round(float(row['lat']) - extent,3)) + '&select=node' + str(row['node_id']) + tags2add + ' ' + str(row['stopidentifier']) + ']'
                wikifile.write('|-\r\n')
                wikifile.write('|align="right" | ' + josmRClink )
                wikifile.write('||align="right" | ' + str(row['description']) + '<br/>' + name  + '<br/>' + str(row['description_normalised']))
                wikifile.write('||align="right" | ' + str(row['zone']))
                wikifile.write('||align="right" | ' + str(row['dl_route_ref']) + '<br/>' + dl_route_ref + '<br/>' + str(row['osm_route_ref']))
                wikifile.write(r'||align="right" |    (500,0,r"(?ui)\b' + straatlaanpleinlei(str(row['description'])).replace(' ','\s*') + '","' + straatlaanpleinlei(name) + '"),\r\n')
                wikifile.write('<br/>       ("' + str(row['description']) + '", "' + name + '", "' + str(row['zone']) + '"),\n')
               
        wikifile.write('|-\r\n|}\r\n')
		
if __name__ == "__main__":
    main()
