"""
backfill pickle files into mongo

progress:

meas.12322 - meas.12389 bz2 pickles  -> meas1
meas.12390 - meas.12619 dir pickles  -> meas2
meas.12620 - meas.12786 plain        -> meas3

new stuff is arriving on bang/freeway. needs to move to whatever the
64bit one is.

2011-01-29 db has this range:


1278633900
1296336300



"""
import os, sys, bz2
sys.path.insert(0, ".")
from pymongo import Connection
import cPickle
from freeway.ingest.ingestd import mongoSave

def stripVdsFields(stations):
    for station in stations:
        # these won't have 'fetchTime'

        # i used to embed all the VDS keys in here, which was very large
        for k in ['abs_pm', 'freeway_dir', 'freeway_id',
                  'latitude', 'longitude', 'name', 'type']:
            del station[k]
    

db = Connection('slash', 27017)['freeway']

if 0:
    for prefix in range(12322,12389+1):
        measDir = "spool/meas.%d" % prefix
        for filename in sorted(os.listdir(measDir)):
            print "reading", filename
            
            t, stations = cPickle.loads(bz2.decompress(open(os.path.join(measDir, filename)).read()))
            stripVdsFields(stations)
            mongoSave(db, t, None, stations, collection='meas1')


if 1:
    for prefix in range(12390,12619+1):
        measDir = "spool/meas.%d" % prefix
        for filename in sorted(os.listdir(measDir)):
            print "reading", filename
            
            t, stations = cPickle.loads(bz2.decompress(open(os.path.join(measDir, filename)).read()))
            stripVdsFields(stations)
            mongoSave(db, t, None, stations, collection='meas1')
    

if 0:
    for prefix in range(12620,12786+1):
        measDir = "spool/meas.%d" % prefix
        for filename in sorted(os.listdir(measDir)):
            print "reading", filename
            t, stations = cPickle.load(open(os.path.join(measDir, filename)))
            stripVdsFields(stations)
            mongoSave(db, t, None, stations, collection='meas3')


