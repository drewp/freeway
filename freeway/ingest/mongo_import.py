"""
backfill pickle files into mongo
"""
import os, sys
sys.path.insert(0, ".")
from pymongo import Connection
import cPickle
from freeway.ingest.ingestd import mongoSave

db = Connection('bang', 27017)['freeway']

measDir = "spool/meas.12783"
for filename in os.listdir(measDir):
    print "reading", filename
    t, stations = cPickle.load(open(os.path.join(measDir, filename)))
    for station in stations:
        # these won't have 'fetchTime'

        # i used to embed all the VDS keys in here, which was very large
        for k in ['abs_pm', 'freeway_dir', 'freeway_id',
                  'latitude', 'longitude', 'name', 'type']:
            del station[k]

    mongoSave(db, t, None, stations)


