"""
gather data into the fixed file format
"""
import glob, sys
from freeway.db import MeasPickle, MeasFixedFile

digit = sys.argv[1]
mon = MeasPickle()
mff = MeasFixedFile("/home/drewp/freeway/12390")

wrote = 0
acc = []
for rec in mon.readMeas(sorted(glob.glob("/home/drewp/freeway/untar/meas.12%s*/*" % digit))):
    acc.append(rec)

    if len(acc) > 10000:
        acc.sort(key=lambda meas: meas['dataTime'])
        for rec in acc:
            mff.writeMeas(rec)
            wrote += 1
            if wrote % 10000 == 0:
                print "wrote", wrote
        acc = []

for rec in acc:
    mff.writeMeas(rec)
