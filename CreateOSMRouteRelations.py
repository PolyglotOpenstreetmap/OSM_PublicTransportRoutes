#!/bin/python
# -*- coding: utf-8 -*-
import postgresql, random, re, pickle, sys, delijnosmlib, zipfile, zlib
import argparse
import urllib.parse
import urllib.request

firstgroupsRE = re.compile(r'^(\d+;){3,4}')
lastgroupsRE = re.compile(r'(;\d+){3,4}$')


removePerronRE=re.compile(r"""(?xiu)
                              (?P<name>[\s*\S]+?)
                              (?P<perron>\s*-?\s*perron\s*\d(\sen\s\d)*)?
                              $
					       """) # case insensitive search to help remove (- )Perron #

db = postgresql.open('pq://Jo:tttttt@localhost:5432/DL')
allrouteidentifiersQUERY = db.prepare("""   SELECT 
                                              rte.routeidentifier,
                                              rte.routepublicidentifier AS routepublicidentifier
                                            FROM 
                                              public.routes rte
                                            WHERE
                                              rte.routepublicidentifier !~* 'F'
                                              AND rte.routedescription !~* 'Feestbus'
                                            ORDER BY
                                              rte.routeidentifier
                                            LIMIT 10000;""")
  
allrouteidsTEC_QUERY = db.prepare("""   SELECT 
                                              routes_tec.routeid AS id,
                                              routes_tec.routepublicidentifier AS routepublicidentifier
                                            FROM 
                                              public.routes_tec
                                            ORDER BY
                                              id
                                            LIMIT 10000;""")
  
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
                                              st1.description_normalised
                                              FROM 
                                                public.stops st1
                                              JOIN public.segments seg1 ON seg1.stopid = st1.stopid AND seg1.tripid = trp.tripid
                                              WHERE 
                                                seg1.segmentsequence = (SELECT MIN(seg2.segmentsequence) FROM public.segments seg2 WHERE seg2.tripid = trp.tripid)) AS fromstop,
                                          (SELECT 
                                              st2.description_normalised
                                              FROM 
                                                public.stops st2
                                              JOIN public.segments seg3 ON seg3.stopid = st2.stopid AND seg3.tripid = trp.tripid
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
                                              st1.description_normalised
                                              FROM 
                                                public.stops_tec st1
                                              JOIN public.segments_tec seg1 ON seg1.stopid = st1.stopidentifier AND seg1.tripid = trp.tripid
                                              WHERE 
                                                seg1.segmentsequence = (SELECT MIN(seg2.segmentsequence) FROM public.segments_tec seg2 WHERE seg2.tripid = seg1.tripid)) AS fromstop,
                                          (SELECT 
                                              st2.description_normalised
                                              FROM 
                                                public.stops_tec st2
                                              JOIN public.segments_tec seg3 ON seg3.stopid = st2.stopidentifier AND seg3.tripid = trp.tripid
                                              WHERE 
                                                seg3.segmentsequence = (SELECT MAX(seg4.segmentsequence) FROM public.segments_tec seg4 WHERE seg4.tripid = seg3.tripid)) AS tostop
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
    parser.add_argument('--josm', '-j', action='store_true',
                       help="don't send result to JOSM remote control")

    args = parser.parse_args()
    if args.allroutes:
        entities = {'1': 'Entiteit Antwerpen',
                    '2': 'Entiteit Oost-Vlaanderen',
                    '3': 'Entiteit Vlaams-Brabant',
                    '4': 'Limburg',
                    '5': 'Entiteit West-Vlaanderen',
                    'B': 'Entité Brabant-Wallon',
                    'C': 'Entité Charleroi',
                    'H': 'Entité Hainaut',
                    'L': 'Entité Liège-Verviers',
                    'N': 'Entité Namur',
                    'X': 'Entité Luxembourg'}
        wikireportFN = 'C:/Data/De Lijn/OSMWikiReportDeLijnTEC.txt'
        with open(wikireportFN, mode='w', encoding='utf-8') as wikireport_file:
            routesdict = {}; distinctroutes = {}; preventity = '0'
            operatorfunctions = [ ( 'De Lijn',
                                    'DL',
                                    allrouteidentifiersQUERY,
                                    tripids,
                                    nodeIDsofStops),
                                  ( 'TEC',
                                    'TEC_',
                                    allrouteidsTEC_QUERY,
                                    tripidsTEC,
                                    nodeIDsofTECStops),]
            for op, o, routeslistfunc, tripslistfunc, stopslistfunc in operatorfunctions:
                firstTime = True
                wikireport_file.write('==' + op + '==\n')
                for line in routeslistfunc():
                    print ('line: ', line)
                    l = line[0]; entity = l[0]
                    print (l, entity, entities[entity])
                    if entity != preventity:
                        wikireport_file.write('===' + entities[entity] + '===\n')
                        preventity = entity
                        if firstTime:
                            firstTime = False
                        else:
                            wikireport_file.write('|}\n')
                        wikireport_file.write('''{| class="wikitable sortable"
! line
! JOSM RC link
! reference
! Continuous?
! All variants match
|-
''')   
                    escapedname, n = processRoute(line[0], o + l, False, operatorid = True, operator=op)
                    if not(escapedname): continue
                    wikireport_file.write('''|style="text-align:right;"|''' + line[1] + '''
|[http://localhost:8111/import?new_layer=true&url=https://dl.dropboxusercontent.com/u/42418402/PT_lines/''' + urllib.parse.quote(n) + '''.osm.zip ''' + escapedname + ''']
|style="text-align:right;"|''' + line[0] + '''
|
|
|-
''')

                    stopnames = {}
                    for row in tripslistfunc(l):
                        stops_as_string = ','
                        for stop in stopslistfunc(row['tripid']):
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
                    wikireport_file.write('|}\n')
                    sys.stdout.flush()
                    #print(routesdict)
                    #input ()
            pickle.dump( routesdict, open( "C:/Data/De Lijn/DeLijnRoute.pickle", "wb" ) )
        return
    pub_ID=input('Enter line number(s) you want to create OSM route relations for: ')
    if ',' in pub_ID:
        for osmid in pub_ID.split(','):
            print(osmid)
            processRoute(osmid,'*',not(args.josm))
    else:
        processRoute(pub_ID,'3214',not(args.josm))

def processRoute(osmid,fn,josmRC, operatorid = False, operator = 'De Lijn'):
    print ('osmid in processroute ' + osmid)
    if operatorid:
        line = osmid
        targetFileName = fn
    else:
        operator = 'De Lijn'
        if osmid[0] in 'tT':
            operator = 'TEC'
            osmid=osmid[1:]
        if operator == 'TEC':
            routeidentifiers = routeidsTEC_QUERY(osmid)
        else:
            routeidentifiers = routeidentifiersQUERY(osmid)
        if len(routeidentifiers)<2:
            print(routeidentifiers)
            index = 0
            print("Auto selecting: {} {} Version {} ({})".format(routeidentifiers[index][2], routeidentifiers[index][1], routeidentifiers[index][3], routeidentifiers[index][4]))
            line=routeidentifiers[0][0]
        else:
            for i,route in enumerate(routeidentifiers):
                print(i+1,route[0], route[1])
            selected=input('Select a line: ')
            index = int(selected)-1
            line=routeidentifiers[index][0]
        if fn == '*': fn = line
        print("Calculating stop lists for:  : {} {} Version {} ({})".format(routeidentifiers[index][2], routeidentifiers[index][1], routeidentifiers[index][3], routeidentifiers[index][4]))
        targetFileName = 'C:/Data/De Lijn/RoutesFor' + fn + '.osm'
        
    print ('osmid ' + osmid)
    print ('line ' + line)
    targetWP_nl_FN = 'C:/Data/De Lijn/RouteSchemaFor' + fn + '.txt'
    distinctroutes = {}
    
    osmroutes = WP_nl = ''

    dlnetworks = ['An', 'OV', 'VB', 'Li', 'WV']
    # print(line)
    if operator == 'TEC':
        network = 'TEC' + line[0]
        tripslist = tripidsTEC(line)
    else:
        tripslist = tripids(line)
        network = 'DL' + dlnetworks[int(line[0])-1]
    if not(tripslist): return None, None
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
    osmroutes += "<?xml version='1.0' encoding='UTF-8'?>\r"
    osmroutes += "<osm version='0.6' upload='true' generator='Python'>\r"
    # WP_nl += "{| {{Prettytable-SP}}\r"
    # WP_nl += "{{SP-kop|Buslijn " + osmid + " " + delijnosmlib.xmlsafe(row['routedescription']) + "}}\r"
    # WP_nl += "{{SP-data\r"
    # WP_nl += "| LENGTE = \r"
    # WP_nl += "| DIENST= [[Afbeelding:De Lijn logo.png|35px]]\r"
    # WP_nl += "| DIENSTTYPE= Busdienst\r"
    # WP_nl += "| OPEN= \r"
    # WP_nl += "| STATUS= in gebruik\r"
    # WP_nl += "| LIJNNUMMER= " + line + "\r"
    # WP_nl += "| TYPE=\r"
    # WP_nl += "}}\r"

    i=1; routeslist = []
    for stopssequence in distinctroutes:
        try:
            print(distinctroutes[stopssequence])
        except:
            e = sys.exc_info()[0]
            print( "Error: %s" % e )
        fromstop,tostop,type,bustram = distinctroutes[stopssequence]
        madeUpId = str(random.randint(90000, 100000))
        routeslist.append(madeUpId)
        osmroutes += "<relation id='-" + madeUpId + "' timestamp='2013-02-13T03:23:07Z' visible='true' version='1'>\r"
        if i>1: WP_nl += "|}\r|-\r"
        if not(fromstop): fromstop='Naamloos'
        if not(tostop): tostop='Naamloos'
        WP_nl += "{{SP-kop|Buslijn " + osmid + " " + delijnosmlib.xmlsafe(re.search(removePerronRE,fromstop).group('name')) + " - " + delijnosmlib.xmlsafe(re.search(removePerronRE,tostop).group('name')) + "}}\r"
        WP_nl += "{{SP-tabel1}}\r"       
        try:
            print('\n' + str(i) + "  " + osmid + " " + fromstop + " - " + tostop)
        except:
            e = sys.exc_info()[0]
            print( "Error: %s" % e )
        counter=0; RTreferrer=''
        osm_objects = ''
        for osmstopID in stopssequence.split(','):
            osm_objects += 'n' + osmstopID + ','
            # print(counter)
            if counter==1:
                WP_nl += "{{SP3||uKBHFa|" + symbol + "|" + RTreferrer
            elif counter>1:
                WP_nl += "{{SP3||uHST|" + symbol + "|" + RTreferrer
            counter+=1
            # print ('osmstopID: ' + osmstopID)
            if osmstopID and osmstopID[0] == '"':
                osmroutes += '  <member type="node" ref="' + stopssequence.split(',')[0].replace('"', '').strip() + '" role="' + ' ' + osmstopID.replace('"', '').strip() + '"/>\r'
                print('                                 ' + osmstopID + ' MISSING!!!!!!!!!!!!!')
            else:
                osmroutes += "  <member type='node' ref='" + osmstopID + "' role='platform'/>\r"
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
        WP_nl += "{{SP3||uKBHFe|" + symbol + "|" + RTreferrer

        osmroutes += '''  <tag k="type" v="route" />\r'''
        osmroutes += '''  <tag k="odbl" v="tttttt" />\r'''
        osmroutes += '''  <tag k="public_transport:version" v="2" />\r'''
        print (bustram)
        if int(bustram)==1 and not(operator == 'TEC'):
            osmroutes += '''  <tag k="route" v="tram" />\r'''
        else:
            osmroutes += '''  <tag k="route" v="bus" />\r'''
        #print(fromstop, tostop)
        #print(re.search(removePerronRE,tostop).group(1))
        #print(re.search(removePerronRE,fromstop).group(1))
        
        osmroutes += '''  <tag k="name" v="''' + operator + ''' ''' + osmid + ''' ''' + delijnosmlib.xmlsafe(re.search(removePerronRE,fromstop).group('name')) + ''' - ''' + delijnosmlib.xmlsafe(re.search(removePerronRE,tostop).group('name')) + '''" />\r'''
        osmroutes += '''  <tag k="ref" v="''' + osmid + '''" />\r'''
        osmroutes += '''  <tag k="from" v="''' + delijnosmlib.xmlsafe(fromstop) + '''" />\r'''
        osmroutes += '''  <tag k="to" v="''' + delijnosmlib.xmlsafe(tostop) + '''" />\r'''
        osmroutes += '''  <tag k="operator" v="''' + operator + '''" />\r'''
        osmroutes += '''  <tag k="network" v="''' + network + '''" />\r'''
        servicetypes =  ['regular','express','school','special','special','belbus']
        servicetypesOSM=['',       'express','school','',       '',       'on_demand']
        #servicetype=servicetypes[int(type)]
        if (type and
             int(type) in servicetypesOSM
             and servicetypesOSM[int(type)]
             and not(operator == 'TEC')):
            osmroutes += '''  <tag k="bus" v="''' + servicetypesOSM[int(type)] + '''" />\r'''
        osmroutes += '''</relation>\r\r'''
        i+=1
    osmroutes += "<relation id='-" + str(random.randint(100000, 900000)) + "' timestamp='2013-02-13T03:23:07Z' visible='true' version='1'>\r"
    osmroutes += '''  <tag k="type" v="route_master" />\r'''

    # WP_nl += "|}\r"
    # WP_nl += "|}\r"
    # WP_nl += "'''Buslijn " + osmid + " " + delijnosmlib.xmlsafe(row['routedescription']) + "'''\r"
    # WP_nl += "==Geschiedenis==\r"
    # WP_nl += "==Route==\r"
    # WP_nl += "* [http://overpass-turbo.eu/s/2XS Dynamische kaart op Openstreetmap met mogelijkheid tot inzoomen, omgeving bekijken en export als GPX]\r"
    # WP_nl += "==Externe verwijzingen==\r"
    # WP_nl += "* [http://www.delijn-aanpassingen.be/files/linefiles/" + line + "/Haltelijst/" + line + ".pdf haltelijst]\r"
    # WP_nl += "* [http://www.delijn-aanpassingen.be/files/linefiles/" + line + "/Lijntraject%20op%20plattegrond/" + line + ".pdf routeplan]\r"
    # WP_nl += "* [http://www.delijn.be Website De Lijn]\r\r"
    # WP_nl += "{{Appendix}}\r\r"
    # WP_nl += "[[Categorie:START-lijnen]]\r"
    # WP_nl += "[[Categorie:Leuvense stadsbussen]]\r"
    # WP_nl += "[[Categorie:Leuvense streekbussen]]\r"
    # WP_nl += "[[Categorie:Vervoer in Vlaams-Brabant]]\r"
    # WP_nl += "[[Categorie:Buslijn in België]]\r"
    mode = ''
    try:
        if int(bustram)==1 and not(operator == 'TEC'):
            mode = 'tram'
        else:
            mode = 'bus'
    except NameError:
        pass
    osmroutes += '''  <tag k="route_master" v="''' + mode + '''" />\r'''
    osmroutes += '''  <tag k="name" v="''' + delijnosmlib.xmlsafe(row['routedescription']) +'''" />\r'''
    osmroutes += '''  <tag k="ref" v="''' + osmid + '''" />\r'''
    if operator == 'TEC':
        osmroutes += '''  <tag k="ref:TEC" v="''' + line + '''" />\r'''
    else:
        osmroutes += '''  <tag k="ref:De_Lijn" v="''' + line + '''" />\r'''
    osmroutes += '''  <tag k="operator" v="''' + operator + '''" />\r'''
    osmroutes += '''  <tag k="network" v="''' + network + '''" />\r'''
    try:
        if (type and
             int(type) in servicetypesOSM
             and servicetypesOSM[int(type)]
             and not(operator == 'TEC')):
            osmroutes += '''  <tag k="bus" v="''' + servicetypesOSM[int(type)] + '''" />\r'''
    except NameError:
        pass
    for routeId in routeslist:
        osmroutes += "  <member type='relation' ref='-" + routeId + "' role=''/>\r"
        
    osmroutes += '''</relation>\r\r'''
    
    osmroutes += "</osm>\r"

    if operatorid:
        route_master_description = row['routedescription']
        n = targetFileName + ' ' + network + ' ' + mode + ' ' + osmid + ' ' + route_master_description
        n = n.replace('/','').replace('\\','')

        zfile = zipfile.ZipFile('C:/Data/Dropbox/Public/PT_lines/' + n + '.osm.zip', compression = zipfile.ZIP_DEFLATED, mode='w')
        zfile.writestr(n + '.osm', osmroutes)
        zfile.close()
        return route_master_description, n
    else:
        with open(targetFileName, mode='w', encoding='utf-8') as osmroutesfile:
            osmroutesfile.write(osmroutes)
        with open(targetWP_nl_FN, mode='w', encoding='utf-8') as WP_nl_file:
            WP_nl_file.write(WP_nl)

    if josmRC:
        with open(targetFileName, mode='r', encoding='utf-8') as osmroutesfile:
            contents=osmroutesfile.read()

        values = { 'data': contents.replace('\n','').replace('\r','')}
        data = urllib.parse.urlencode(values)
        # data = data.encode('utf-8') # data should be bytes
        url = 'http://localhost:8111/load_data?' + data

        req = urllib.request.Request(url, method='GET')
        response = urllib.request.urlopen(req)
        the_page = response.read()
        print(response)
        
        print(the_page)

    print("Calculated stop lists for:  : {} {} Version {} ({})".format(routeidentifiers[index][2], routeidentifiers[index][1], routeidentifiers[index][3], routeidentifiers[index][4]))

if __name__ == "__main__":
    main()
  
