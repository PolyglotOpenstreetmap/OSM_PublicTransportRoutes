#!/bin/python
# -*- coding: utf-8 -*-
import os, sys, re, zipfile, ftplib
import argparse
zipre = re.compile('\d\d\d\d-\d\d-\d\d\.zip$')


parser = argparse.ArgumentParser(description='Fetch data from FTP server of De Lijn, unzip it and recode to UTF-8')
parser.add_argument('--skipdownload', '-d', action='store_true',
                   help="Don't contact the FTP server, work with the most recent local file")
parser.add_argument('--dontcallsuccessor', '-s', action='store_true',
                   help="don't call NewDBfromCSV.py when done")

args = parser.parse_args()

""" Fetch the latest zip file from the ftp site of De Lijn """

class Callback(object):
    '''This prints a nice progress status on the command line'''
    def __init__(self, totalsize, fp):
        self.totalsize = totalsize
        self.fp = fp
        self.received = 0

    def __call__(self, data):
        self.fp.write(data)
        self.received += len(data)
        print('\r%i%% complete' % (100.0*self.received/self.totalsize), end='\r')

if not(args.skipdownload):
    print ('Reading credentials from "credentials.txt"')
    with open("credentials.txt") as credentials:
        username, password = credentials.readlines()
        #print (username, password)

    print ("Opening connection to FTP site of De Lijn")
    ftp=ftplib.FTP(host='poseidon.delijn.be', user=username, passwd=password)
    print ("CD to current")
    ftp.cwd('current')
    print ("Get name of file")
    fn = ftp.nlst()[0]
    size = ftp.size(fn)
    if not(fn in os.listdir()):
        # Only download if a newer file is available
        print (fn + " found, downloading latest version of De Lijndata")
        with open(fn, 'wb') as fh:
            w = Callback(size, fh)
            #ftp.set_pasv(0)
            ftp.retrbinary('RETR %s' % fn, w, 32768)

        ftp.quit()
    else:
        print('Latest version already present, nothing to do')
        sys.exit()
""" Unzip the latest file we have available in the current directory """

files = os.listdir()
zipfn=''
for file in files:
    if re.match(zipre, file):
        if file > zipfn:
            zipfn = file

zfile = zipfile.ZipFile(zipfn)
print(); print(); print("Found " + zipfn)
for name in zfile.namelist():
    """Recode csv-file with textual content to UTF-8 """
    (dirname, filename) = os.path.split(name)
    print("Decompressing " + filename)
    fd = open(name,"wb")
    fd.write(zfile.read(name).decode('latin-1').replace('\r','').replace('"','').encode('utf-8'))
    fd.close()

if not(args.dontcallsuccessor): import NewDBfromCSV
