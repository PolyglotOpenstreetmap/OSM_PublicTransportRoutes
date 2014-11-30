cd "C:\Data\De Lijn"

python.exe .\dlTEC.py
python.exe .\FetchUnzipAndRecodeLatest.py

pause

python.exe .\SaveStopsFromOSMasCSV.py

python.exe .\DeLijnData_in_Postgis_2_OSM.py
python.exe .\TECData_in_Postgis_2_OSM.py

python.exe .\CreateWikiReport.py

del "C:\Data\DropBox\Public\DL.osm.zip"
del "C:\Data\DropBox\Public\TEC.osm.zip"

"C:\Program Files\7-Zip\7z.exe" u -y "C:\Data\DropBox\Public\DL.osm.zip" "Haltes De Lijndata.osm"
"C:\Program Files\7-Zip\7z.exe" u -y "C:\Data\DropBox\Public\TEC.osm.zip" "C:\Data\TEC\Stops TECdata.osm"

pause