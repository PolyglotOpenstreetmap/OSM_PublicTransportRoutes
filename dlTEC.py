import urllib.request, re, os, sys

zipfnRE=re.compile(r'''<A HREF="(.+?)">.+<A HREF="/Current%20BLTAC/(.+?)">''')
response = urllib.request.urlopen('http://beltac.tec-wl.be/Current%20BLTAC/')
html = str(response.read())

fn=zipfnRE.search(html).groups(0)[1]

if not(fn in os.listdir('C:/data/tec')):
    # Only download if a newer file is available
    print (fn + " found, downloading latest version of TEC data")

    urllib.request.urlretrieve('http://beltac.tec-wl.be/Current%20BLTAC/' + fn, 'C:/data/tec/' + fn)
else:
    print('Latest version already present, nothing to do')
    sys.exit()

import TEC