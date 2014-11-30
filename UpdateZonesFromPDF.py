import postgresql, re, sys

newZoneRE=re.compile(r'(?i)Zone\s(?P<zone>\d+)\s(?P<city>\w+)')
stopNameRE= re.compile(r'\d+\s(?P<name>.*)')
db = postgresql.open('pq://Jo:tttttt@localhost:5432/DL')

updatezone = db.prepare("""UPDATE stops SET OSM_zone = $2
                                        WHERE description = $1
										AND osm_zone IS NULL;""")

def main(sourceFileName):
    with open(sourceFileName, encoding='utf-8') as zonesfile:
        for line in zonesfile:
            #print(line)
            newZone = newZoneRE.search(line)
            if newZone:
                zone = newZone.group('zone')
                city = newZone.group('city')
                print(); print(zone + ' ' + city)
                continue
            else:
                name = stopNameRE.search(line)
                if name:
                    description=city + ' ' + name.group('name')
                    print(description + ' to zone:' + zone)
                    updatezone(description,zone)

if __name__ == "__main__":
    main('C:/Data/De Lijn/zones.txt')