from __future__ import division
import time, logging, os, struct, datetime
from dateutil.tz import tzutc
import pymongo
from freeway.lib.memoize import lru_cache
log = logging.getLogger()

def logTime(func):
    def inner(*args, **kw):
        t1 = time.time()
        try:
            ret = func(*args, **kw)
        finally:
            log.info("Call to %s took %.1f ms" % (
                func.__name__, 1000 * (time.time() - t1)))
        return ret
    return inner

class MeasFixedFile(object):
    """
    file named meas.12786 has any measurements with a dataTime value
    of 12786xxxxx. The measurements are in order but may not be evenly
    spaced.

    The whole file is rewritten when we have to insert. I don't
    currently dedup, so try not to write dups.

    Some values are rounded. Nulls become 0.
    """
    def __init__(self, topDir):
        self.topDir = topDir
        # these 32 bit times will break in 2038
        self.structFormat = "!II6sHHHHHHHBB"
        self.structSize = struct.calcsize(self.structFormat)

        # (name, file) cache, just to save close/open calls while writing
        self.currentFile = None 

    def makeFilename(self, num):
        return os.path.join(self.topDir, "meas.%d" % num)

    def recentMeas(self, vdsFilter=None):
        """iterator through the meas records in backwards order

        optionally limit to only vds_id strings in the given list

        this does not use self.currentFile so it can notice new data
        written by other processes

        may return nothing if things go wrong
        """
        vdsFilter = [str(v) for v in vdsFilter]
        start = int((time.time() // 100000)+1)
        for fileNumber in xrange(start, start - 100, -1):
            filename = self.makeFilename(fileNumber)
            try:
                f = open(filename, "rb") # error here if there's a gap in the files
                f.seek(-self.structSize, 2)
                while True:
                    raw = f.read(self.structSize)
                    if vdsFilter is None or raw[8:8+6] in vdsFilter:
                        yield self.unpack(raw)
                    f.seek(-self.structSize*2, 1) # error here if we seek all the way past 0
            except IOError:
                continue

    def writeMeas(self, m):
        outFile = self.makeFilename(m['dataTime'] // 100000)
        packed = self.pack(m)

        if self.currentFile is not None and self.currentFile[0] == outFile:
            f = self.currentFile[1]
        else:
            if self.currentFile is not None:
                self.currentFile[1].close()

            print "opening", outFile
            try:
                f = open(outFile, "rb+")
            except IOError:
                f = open(outFile, "wb+")
            self.currentFile = outFile, f

        try:
            f.seek(-self.structSize, 2)
            lastRec = self.unpack(f.read(self.structSize))
        except IOError: # zero-length file
            lastRec = {'dataTime':0}
        
        if m['dataTime'] >= lastRec['dataTime']:
            f.write(packed)
        else:
            print "lastRec was %s, new rec is %s, rewriting" % (
                lastRec['dataTime'], m['dataTime'])
            records = self._allRecords(f)
            records.append(packed)
            records.sort()
            f.seek(0, 0)
            for rec in records:
                f.write(rec)

    def _allRecords(self, f):
        f.seek(0, 2)
        num = f.tell() // self.structSize
        f.seek(0, 0)
        return [f.read(self.structSize) for loop in range(num)]

    def merge(self, firstFullPath, otherFullPath, outFullPath):
        """used during migrations"""
        recs1 = set(self._allRecords(open(firstFullPath)))
        recs2 = set(self._allRecords(open(otherFullPath)))
        allRecs = sorted(recs1.union(recs2))
        print "%s had %s; %s had %s; output has %s unique records" % (
            firstFullPath, len(recs1), otherFullPath, len(recs2), len(allRecs))
        f = open(outFullPath, "wb")
        for rec in allRecs:
            f.write(rec)
        f.close()

    def pack(self, meas):
        print "mymeas", repr(meas)
        q = meas.get('q', None) or 0 # sometimes null- stored as 0
        return struct.pack(self.structFormat,
                           meas['dataTime'],
                           meas['readTime'],
                           str(meas['vds_id']),
                           min(meas['flow'], 65535),
                           int(meas['occupancy'] * 65535),
                           int(meas['speed'] * 100),
                           int(meas['vmt'] * 10),
                           int(meas['vht'] * 10),
                           int(q * 10), 
                           int(meas['delay'] * 10),
                           meas['num_samples'],
                           meas['pct_observed'])

    def unpack(self, s):
        d = struct.unpack(self.structFormat, s)
        return dict(
            dataTime=d[0],
            readTime=d[1],
            vds_id=d[2],
            flow=d[3],
            occupancy=d[4] / 65535,
            speed=d[5] / 100,
            vmt=d[6] / 10,
            vht=d[7] / 10,
            q=d[8] / 10,
            delay=d[9] / 10,
            num_samples=d[10],
            pct_observed=d[11])
            
class VdsMongo(object):
    def initVdsMongo(self):
        self.vdsMongo = pymongo.Connection("bang")['freeway']['vds']
        
    def replaceVds(self, rows):
        """
        drop and recreate the whole vds table with these rows,
        specified as dicts

        in an older version my mongo doc looked like this:
        { "_id" : "401391",
          "name" : "101/280/680 i/c on ramp loop to",
          "type" : "ML",
          "abs_pm" : 0.18,
          "pos" : { "longitude" : -121.85467, "latitude" : 37.337881 },
          "freeway_id" : "280",
          "freeway_dir" : "S"
        }
        """
        self.vdsMongo.drop()
        self.vdsMongo.ensure_index([('id', 1), ('abs_pm', 1)])
        self.vdsMongo.insert(rows, safe=True)
                   
    @lru_cache(1000)
    def getVds(self, id):
        doc = self.vdsMongo.find_one({"id":id})
        if doc is None:
            raise ValueError
        return doc

    @logTime
    @lru_cache(1000)
    def vdsInRange(self, freeway_id, pmLow, pmHigh):
        out = []
        for doc in self.vdsMongo.find({'freeway_id':freeway_id,
                                       'abs_pm' : {'$gte' : pmLow,
                                                   '$lte' : pmHigh}}):
            out.append(doc['id'])
        return out

    @lru_cache(1000)
    def pmLabel(self, freeway_id, pm):
        doc = self.vdsMongo.find_one({"freeway_id":freeway_id, 'abs_pm':pm})
        if doc is None:
            raise ValueError(str((freeway_id, pm)))
        return doc['name']
        
class Db(VdsMongo):
    """
    combined database API for clients to use. The meas and vds data
    might come from different kinds of places.
    
    all times are integer unix seconds, even in the db
    """
    def __init__(self):
        self.fixedFile = MeasFixedFile("/opt/freeway")
        self.initVdsMongo()

    @logTime
    def recentMeas(self, vds, limit):
        out = []
        for meas in self.fixedFile.recentMeas(vds):
            out.append(meas)
            if len(out) >= limit:
                return out
        
    def save(self, timestamp, now, samples):
        """
        now (secs) is when we asked for the data
        timestamp (secs) is the timestamp in the pems file we got
        samples is a list of dicts of data
        """
        for m in samples:
            m['dataTime'] = timestamp
            m['readTime'] = now
            self.fixedFile.writeMeas(m)

        
def getDb():
    return Db()
