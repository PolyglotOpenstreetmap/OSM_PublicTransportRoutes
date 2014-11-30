#!/bin/python
# -*- coding: utf-8 -*-
import postgresql, random, re, pickle, sys, delijnosmlib
from urllib.parse import urlencode
import argparse

firstgroupsRE = re.compile(r'^(\d+;){3,4}')
lastgroupsRE = re.compile(r'(;\d+){3,4}$')


removePerronRE=re.compile(r"""(?xiu)
                              (?P<name>[\s*\S]+?)
                              (?P<perron>\s*perron\s*\d*)?
                              $
					       """) # case insensitive search removing Perron #

db = postgresql.open('pq://Jo:tttttt@localhost:5432/DL')
allrouteidentifiersQUERY = db.prepare("""   SELECT 
                                              routes.routeidentifier
                                            FROM 
                                              public.routes
                                            ORDER BY
                                              routes.routeidentifier;""")
  
allrouteidsTEC_QUERY = db.prepare("""   SELECT 
                                              routes_tec.routeid AS id
                                            FROM 
                                              public.routes_tec
                                            ORDER BY
                                              id;""")
  
routeidentifiersQUERY = db.prepare("""  SELECT DISTINCT
                                          rte.routeidentifier, rte.routedescription, rte.routepublicidentifier, rte.routeversion, rte.routeid
                                        FROM public.routes   rte
                                        WHERE
                                          rte.routepublicidentifier ILIKE $1
                                          AND rte.routedescription !~* 'Feestbus'
                                        ORDER BY
                                          rte.routeidentifier;""")

routeidsTEC_QUERY = db.prepare("""  SELECT DISTINCT
                                          rte.routeid AS id, rte.routename, rte.routepublicidentifier, 1, 1
                                        FROM public.routes_tec   rte
                                        WHERE
                                          rte.routepublicidentifier ILIKE $1
                                        ORDER BY
                                          id;""")

tripids = db.prepare("""                SELECT DISTINCT
                                          trp.tripid,
                                          rte.routeservicetype AS type,
                                          rte.routeservicemode AS bustram,
                                          rte.routedescription AS routedescription,
                                          (SELECT 
                                              st.description_normalised
                                              FROM 
                                                public.stops st
                                              JOIN public.segments seg1 ON seg1.stopid = st.stopid AND seg1.tripid = trp.tripid
                                              WHERE 
                                                seg1.segmentsequence = (SELECT MIN(seg2.segmentsequence) FROM public.segments seg2 WHERE seg2.tripid = trp.tripid)) AS fromstop,
                                          (SELECT 
                                              st.description_normalised
                                              FROM 
                                                public.stops st
                                              JOIN public.segments seg3 ON seg3.stopid = st.stopid AND seg3.tripid = trp.tripid
                                              WHERE 
                                                seg3.segmentsequence = (SELECT MAX(seg4.segmentsequence) FROM public.segments seg4 WHERE seg4.tripid = trp.tripid)) AS tostop
                                        FROM public.trips    trp
                                        JOIN public.routes   rte      ON rte.routeid=trp.routeid
                                        JOIN public.segments seg      ON seg.tripid=trp.tripid
                                        JOIN public.stops    stp      ON seg.stopid=stp.stopid
                                        WHERE
                                          rte.routeidentifier = $1
                                        ORDER BY fromstop, tostop;""")

tripidsTEC = db.prepare("""             SELECT DISTINCT
                                          trp.tripid,
                                          1 AS type,
                                          1 AS bustram,
                                          rte.routename AS routedescription,
                                          (SELECT 
                                              st.description_normalised
                                              FROM 
                                                public.stops_tec st
                                              JOIN public.segments_tec seg1 ON seg1.stopid = st.stopidentifier AND seg1.tripid = trp.tripid
                                              WHERE 
                                                seg1.segmentsequence = (SELECT MIN(seg2.segmentsequence) FROM public.segments_tec seg2 WHERE seg2.tripid = trp.tripid)) AS fromstop,
                                          (SELECT 
                                              st.description_normalised
                                              FROM 
                                                public.stops_tec st
                                              JOIN public.segments_tec seg3 ON seg3.stopid = st.stopidentifier AND seg3.tripid = trp.tripid
                                              WHERE 
                                                seg3.segmentsequence = (SELECT MAX(seg4.segmentsequence) FROM public.segments_tec seg4 WHERE seg4.tripid = trp.tripid)) AS tostop
                                        FROM public.trips_tec    trp
                                        JOIN public.routes_tec   rte      ON rte.routeid=trp.routeid
                                        JOIN public.segments_tec seg      ON seg.tripid=trp.tripid
                                        JOIN public.stops_tec    stp      ON seg.stopid=stp.stopidentifier
                                        WHERE
                                          rte.routeid = $1
                                        ORDER BY fromstop, tostop;""")

nodeIDsofStops = db.prepare("""SELECT DISTINCT
                                  stposm.node_id,
                                  stpdl.description_normalised,
                                  stpdl.description,
                                  stpdl.stopidentifier,
                                  trp.tripstart,
                                  seg.segmentsequence,
                                  stpdl.route_ref
                                FROM trips     trp
                                JOIN routes    rte      ON rte.routeid=trp.routeid
                                JOIN segments  seg      ON seg.tripid=trp.tripid
                                JOIN stops     stpdl    ON seg.stopid=stpdl.stopid
                                                         AND stpdl.description !~* 'dummy|afgeschaft'
                                JOIN stops_osm stposm   ON stpdl.stopidentifier = COALESCE(stposm.refDL, stposm.ref)
                                WHERE
                                  trp.tripid = $1
                                ORDER BY
                                  trp.tripstart ASC,
                                  seg.segmentsequence ASC;""")

nodeIDsofTECStops = db.prepare("""SELECT DISTINCT
                                  stposm.node_id,
                                  stptec.description_normalised,
                                  stptec.descriptionfr as description,
                                  stptec.stopidentifier,
                                  seg.time,
                                  seg.segmentsequence,
                                  stptec.route_ref
                                FROM trips_tec    trp
                                JOIN routes_tec   rte      ON rte.routeid=trp.routeid
                                JOIN segments_tec seg      ON seg.tripid=trp.tripid
                                JOIN stops_tec    stptec      ON seg.stopid=stptec.stopidentifier
                                JOIN stops_osm stposm   ON stptec.stopidentifier = COALESCE(stposm.refTECL, stposm.refTECX, stposm.refTECN, stposm.refTECH, stposm.refTECB, stposm.refTECC, stposm.ref)
                                WHERE
                                  trp.tripid = $1
                                ORDER BY
                                  seg.time ASC,
                                  seg.segmentsequence ASC;""")

def main():
    parser = argparse.ArgumentParser(description='Create route relations')
    parser.add_argument('--allroutes', '-a', action='store_true',
                       help="process all routes and pickle a dictionary of it")
    parser.add_argument('--route', '-r', action='store_true',
                       help="add a De Lijn route ref directly")

    args = parser.parse_args()
    if args.allroutes:
        routesdict = {}; distinctroutes = {}
        for line in allrouteidentifiersQUERY():
            l = line[0]
            tripslist = tripids(l)
            stopnames = {}
            for row in tripslist:
                stops_as_string = ','
                stopslist = nodeIDsofStops(row['tripid'])
                for stop in stopslist:
                    stopnames[stop[0]] = stop[1]
                    if stop[0]:
                        if stop[0] != stops_as_string.split(',')[-2]:
                            stops_as_string += stop[0] + ','
                    else:
                        stops_as_string += '"' + stop[1] + '",'
                stops_as_string = stops_as_string[1:-1]
                notfound=True
                for sequence in distinctroutes.keys():
                    notfound=True
                    if len(stops_as_string)<len(sequence) and stops_as_string in sequence: notfound=False; break
                    if len(sequence)<len(stops_as_string) and sequence in stops_as_string:
                        del distinctroutes[sequence]
                        break
                if notfound: distinctroutes[stops_as_string] = [row['fromstop'],row['tostop'],row['type'],row['bustram']]
            routesdict[line] = distinctroutes
            print(l, end=', ')
            sys.stdout.flush()
            #print(routesdict)
            #input ()
        pickle.dump( routesdict, open( "C:/Data/De Lijn/DeLijnRoute.pickle", "wb" ) )
        return
    pub_ID=input('Enter line number(s) you want to create OSM route relations for: ')
    if ',' in pub_ID:
        for osmid in pub_ID.split(','):
            print(osmid)
            processRoute(osmid,'*')
    else:
        processRoute(pub_ID,'3214')

def processRoute(osmid,fn):
    operator = 'De Lijn'
    if osmid[0] in 'tT':
        operator = 'TEC'
        osmid=osmid[1:]
    print(osmid)
    if operator == 'TEC':
        routeidentifiers = routeidsTEC_QUERY(osmid)
    else:
        routeidentifiers = routeidentifiersQUERY(osmid)
    if len(routeidentifiers)<2:
        print(routeidentifiers)
        print("Auto selecting: %s %s Version %s (%s)", routeidentifiers[0][2], routeidentifiers[0][1], routeidentifiers[0][3], routeidentifiers[0][4])
        line=routeidentifiers[0][0]
    else:
        for i,route in enumerate(routeidentifiers):
            print(i+1,route[0], route[1])
        selected=input('Select a line: ')
        line=routeidentifiers[int(selected)-1][0]
    if fn == '*': fn = line
    targetFileName = 'C:/Data/De Lijn/RoutesFor' + fn + '.osm'
    targetWP_nl_FN = 'C:/Data/De Lijn/RouteSchemaFor' + fn + '.txt'
    distinctroutes = {}
    print ("Calculating stop lists for:  " + line)
    with open(targetFileName, mode='w', encoding='utf-8') as osmroutesfile:
        with open(targetWP_nl_FN, mode='w', encoding='utf-8') as WP_nl_file:
            #print(tripids.string)
            if operator == 'TEC':
                tripslist = tripidsTEC(line)
            else:
                tripslist = tripids(line)
            # print(tripslist)
            stopnames = {}
            stoprefs = {}
            stoprouterefs = {}
            for row in tripslist:
                # print(row)
                stops_as_string = ','
                if operator == 'TEC':
                    stopslist = nodeIDsofTECStops(row['tripid'])
                else:
                    stopslist = nodeIDsofStops(row['tripid'])
                for stop in stopslist:
                    # print(stop)
                    stopnames[stop[0]] = stop[1]
                    stoprefs[stop[0]] = stop[3]
                    stoprouterefs[stop[0]] = stop[6]

                    #print(stops_as_string)
                    #print(stop[0], stops_as_string.split(',')[-1], stop[1])
                    if stop[0]:
                        if stop[0] != stops_as_string.split(',')[-2]:
                            stops_as_string += stop[0] + ','
                    else:
                        stops_as_string += '"' + str(stop[3]) + ';' + stop[1] + '",'
                stops_as_string = stops_as_string[1:-1]
                notfound=True
                for sequence in distinctroutes.keys():
                    notfound=True
                    if len(stops_as_string)<len(sequence) and stops_as_string in sequence: notfound=False; break
                    if len(sequence)<len(stops_as_string) and sequence in stops_as_string:
                        del distinctroutes[sequence]
                        break
                if notfound: distinctroutes[stops_as_string] = [row['fromstop'],row['tostop'],row['type'],row['bustram']]
            #print(distinctrouteslist)
            osmroutesfile.write("<?xml version='1.0' encoding='UTF-8'?>\r")
            osmroutesfile.write("<osm version='0.6' upload='true' generator='Python'>\r")
            WP_nl_file.write("{| {{Prettytable-SP}}\r")
            WP_nl_file.write("{{SP-kop|Buslijn " + osmid + " " + delijnosmlib.xmlsafe(row['routedescription']) + "}}\r")
            WP_nl_file.write("{{SP-data\r")
            WP_nl_file.write("| LENGTE = \r")
            WP_nl_file.write("| DIENST= [[Afbeelding:De Lijn logo.png|35px]]\r")
            WP_nl_file.write("| DIENSTTYPE= Busdienst\r")
            WP_nl_file.write("| OPEN= \r")
            WP_nl_file.write("| STATUS= in gebruik\r")
            WP_nl_file.write("| LIJNNUMMER= " + line + "\r")
            WP_nl_file.write("| TYPE=\r")
            WP_nl_file.write("}}\r")

            i=1; routeslist = []
            for stopssequence in distinctroutes:
                print(distinctroutes[stopssequence])
                fromstop,tostop,type,bustram = distinctroutes[stopssequence]
                madeUpId = str(random.randint(100000, 900000))
                routeslist.append(madeUpId)
                osmroutesfile.write("<relation id='-" + madeUpId + "' timestamp='2013-02-13T03:23:07Z' visible='true' version='1'>\r")
                if i>1: WP_nl_file.write("|}\r|-\r") 
                if not(fromstop): fromstop='Naamloos'
                if not(tostop): tostop='Naamloos'
                WP_nl_file.write("{{SP-kop|Buslijn " + osmid + " " + delijnosmlib.xmlsafe(re.search(removePerronRE,fromstop).group('name')) + " - " + delijnosmlib.xmlsafe(re.search(removePerronRE,tostop).group('name')) + "}}\r")
                WP_nl_file.write("{{SP-tabel1}}\r")                
                print('\n' + str(i) + "  " + osmid + " " + fromstop + " - " + tostop)
                counter=0; RTreferrer=''
                for osmstopID in stopssequence.split(','):
                    # print(counter)
                    if counter==1:
                        WP_nl_file.write("{{SP3||uKBHFa|" + symbol + "|" + RTreferrer)
                    elif counter>1:
                        WP_nl_file.write("{{SP3||uHST|" + symbol + "|" + RTreferrer)
                    counter+=1
                    # print ('osmstopID: ' + osmstopID)
                    if osmstopID[0] == '"':
                        osmroutesfile.write('  <member type="node" ref="' + stopssequence.split(',')[0].replace('"', '').strip() + '" role="' + ' ' + osmstopID.replace('"', '').strip() + '"/>\r')
                        print('                                 ' + osmstopID + ' MISSING!!!!!!!!!!!!!')
                    else:
                        osmroutesfile.write("  <member type='node' ref='" + osmstopID + "' role='platform'/>\r")
                        try:
                            print('  ' + stopnames[osmstopID])
                        except:
                            e = sys.exc_info()[0]
                            print( "Error: %s" % e )
                    # print(stoprouterefs)
                    # print(stoprouterefs.keys())
                    # print(osmstopID)
                    symbol = ''
                    if osmstopID in stoprouterefs:
                        routerefs = stoprouterefs[osmstopID]
                        if len(routerefs) > 35:
                            firstmatch = re.match(firstgroupsRE,routerefs)
                            lastmatch = re.search(lastgroupsRE,routerefs)
                            if firstmatch and lastmatch:
                                routerefs = firstmatch.group(0)[:-1] + '...' + lastmatch.group(0)[1:]
                        #city, name=stopnames[osmstopID].split(" ",1)
                        RTreferrer = "|<small>" + stopnames[osmstopID] + "<br>[http://mijnlijn.be/" + str(stoprefs[osmstopID]) + " " + routerefs + "]</small>}}\r"
                        if ' Station ' in stopnames[osmstopID] and not 'Oud Station' in stopnames[osmstopID]:
                            symbol='TRAIN'
                        if 'Zaventem Luchthaven' in stopnames[osmstopID]:
                            symbol='FLUG'
                WP_nl_file.write("{{SP3||uKBHFe|" + symbol + "|" + RTreferrer)

                osmroutesfile.write('''  <tag k="type" v="route" />\r''')
                osmroutesfile.write('''  <tag k="odbl" v="tttttt" />\r''')
                osmroutesfile.write('''  <tag k="public_transport:version" v="2" />\r''')
                print (bustram)
                if int(bustram)==1 and not(operator == 'TEC'):
                    osmroutesfile.write('''  <tag k="route" v="tram" />\r''')
                else:
                    osmroutesfile.write('''  <tag k="route" v="bus" />\r''')
                #print(fromstop, tostop)
                #print(re.search(removePerronRE,tostop).group(1))
                #print(re.search(removePerronRE,fromstop).group(1))
                
                osmroutesfile.write('''  <tag k="name" v="''' + operator + ''' ''' + osmid + ''' ''' + delijnosmlib.xmlsafe(re.search(removePerronRE,fromstop).group('name')) + ''' - ''' + delijnosmlib.xmlsafe(re.search(removePerronRE,tostop).group('name')) + '''" />\r''')
                osmroutesfile.write('''  <tag k="ref" v="''' + osmid + '''" />\r''')
                osmroutesfile.write('''  <tag k="from" v="''' + delijnosmlib.xmlsafe(fromstop) + '''" />\r''')
                osmroutesfile.write('''  <tag k="to" v="''' + delijnosmlib.xmlsafe(tostop) + '''" />\r''')
                osmroutesfile.write('''  <tag k="operator" v="''' + operator + '''" />\r''')
                servicetypes =  ['regular','express','school','special','special','belbus']
                servicetypesOSM=['',       'express','school','',       '',       'on_demand']
                #servicetype=servicetypes[int(type)]
                if servicetypesOSM[int(type)] and not(operator == 'TEC'):
                    osmroutesfile.write('''  <tag k="bus" v="''' + servicetypesOSM[int(type)] + '''" />\r''')
                osmroutesfile.write('''</relation>\r\r''')
                i+=1
            osmroutesfile.write("<relation id='-" + str(random.randint(100000, 900000)) + "' timestamp='2013-02-13T03:23:07Z' visible='true' version='1'>\r")
            osmroutesfile.write('''  <tag k="type" v="route_master" />\r''')

            WP_nl_file.write("|}\r")
            WP_nl_file.write("|}\r")
            WP_nl_file.write("'''Buslijn " + osmid + " " + delijnosmlib.xmlsafe(row['routedescription']) + "'''\r")
            WP_nl_file.write("==Geschiedenis==\r")
            WP_nl_file.write("==Route==\r")
            WP_nl_file.write("* [http://overpass-turbo.eu/s/2XS Dynamische kaart op Openstreetmap met mogelijkheid tot inzoomen, omgeving bekijken en export als GPX]\r")
            WP_nl_file.write("==Externe verwijzingen==\r")
            WP_nl_file.write("* [http://www.delijn-aanpassingen.be/files/linefiles/" + line + "/Haltelijst/" + line + ".pdf haltelijst]\r")
            WP_nl_file.write("* [http://www.delijn-aanpassingen.be/files/linefiles/" + line + "/Lijntraject%20op%20plattegrond/" + line + ".pdf routeplan]\r")
            WP_nl_file.write("* [http://www.delijn.be Website De Lijn]\r\r")
            WP_nl_file.write("{{Appendix}}\r\r")
            WP_nl_file.write("[[Categorie:START-lijnen]]\r")
            WP_nl_file.write("[[Categorie:Leuvense stadsbussen]]\r")
            WP_nl_file.write("[[Categorie:Leuvense streekbussen]]\r")
            WP_nl_file.write("[[Categorie:Vervoer in Vlaams-Brabant]]\r")
            WP_nl_file.write("[[Categorie:Buslijn in België]]\r")
            try:
                if int(bustram)==1 and not(operator == 'TEC'):
                    osmroutesfile.write('''  <tag k="route_master" v="tram" />\r''')
                else:
                    osmroutesfile.write('''  <tag k="route_master" v="bus" />\r''')
            except NameError:
                pass
            osmroutesfile.write('''  <tag k="name" v="''' + delijnosmlib.xmlsafe(row['routedescription']) +'''" />\r''')
            osmroutesfile.write('''  <tag k="ref" v="''' + osmid + '''" />\r''')
            if operator == 'TEC':
                osmroutesfile.write('''  <tag k="ref:TEC" v="''' + line + '''" />\r''')
            else:
                osmroutesfile.write('''  <tag k="ref:De_Lijn" v="''' + line + '''" />\r''')
            osmroutesfile.write('''  <tag k="operator" v="''' + operator + '''" />\r''')
            try:
                if servicetypesOSM[int(type)] and not(operator == 'TEC'):
                    osmroutesfile.write('''  <tag k="bus" v="''' + servicetypesOSM[int(type)] + '''" />\r''')
            except NameError:
                pass
            for routeId in routeslist:
                osmroutesfile.write("  <member type='relation' ref='-" + routeId + "' role=''/>\r")
                
            osmroutesfile.write('''</relation>\r\r''')
            
            osmroutesfile.write("</osm>\r")


if __name__ == "__main__":
    main()
  
