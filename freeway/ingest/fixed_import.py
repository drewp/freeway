"""
gather data into the fixed file format

/tmp/freeway stopped at a crash, i'm resuming in freeway3/
meas.13205 is incomplete, need to resume again

"""
from freeway.db import Db, MeasFixedFile

old = Db()
mff = MeasFixedFile("/home/drewp/freeway/13495")

wrote = 0
acc = []
for rec in old.allMonetMeas():
    acc.append(rec)

    if len(acc) > 8000:
        acc.sort(key=lambda meas: meas['dataTime'])
        for rec in acc:
            mff.writeMeas(rec)
            wrote += 1
            if wrote % 10000 == 0:
                print "wrote", wrote
        acc = []

for rec in acc:
    mff.writeMeas(rec)
