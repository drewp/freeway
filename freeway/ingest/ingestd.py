import os, time, datetime, dateutil.tz
import cPickle as pickle
from glob import glob
from twisted.internet import reactor
from pymongo import bson, Connection

from loop_data import parse5MinFile
from detector import parseVdsConfig
from memoize import lru_cache

class UpdateLoop(object):
    """
    task that fetches measurements into mongo every 5 minutes
    """
    def __init__(self):
        self.db = Connection('bang', 27017)['freeway']
        self.update()

    def update(self):
        now = time.time()
        try:
            print "fetching:"
            ret = os.system("./ftp_get")
            print "done", ret

            timestamp, samples = parse5MinFile("spool/5minagg_latest.txt.gz")
            print "now %s, datatime %s" % (now, timestamp)
            nextDataTime = timestamp + 5 * 60
            mongoSave(self.db, timestamp, now, samples)
        except Exception, e:
            print "update failed:", e
            nextDataTime = now

        # it would be nice to get in phase with their data, but their
        # timestamp is much older than my current clock (like 9.5
        # mins, today) so I'm not sure what to do
        delay = 5*60# max(5, nextDataTime - now)
        
        print "next download in %s sec" % delay
        reactor.callLater(delay, self.update)

def mongoFromTimestamp(t):
    return datetime.datetime.fromtimestamp(t, dateutil.tz.tzutc())

def mongoSave(db, t, fetchTime, stations):
    for station in stations:
        station['time'] = mongoFromTimestamp(t)
        if fetchTime is not None:
            station['fetchTime'] = mongoFromTimestamp(fetchTime)
        station['num_samples'] = int(station['num_samples'])
        del station['travel_time'] # docs: "not in use"

        for k in ['delay', 'pct_observed', 'vht', 'vmt', 'flow',
                  'occupancy', 'q', 'speed',]:
            try:
                station[k] = float(station[k])
            except ValueError: # empty string?
                pass
        db['meas'].save(station)


def getRecentSets(n=5):
    """most recent time,measurements first, then n-1 more of those pairs"""
    for filename in recentFilenames(n):
        timestamp, meas = loadFile(filename)
        yield timestamp, meas

@lru_cache(40)
def loadFile(filename):
    f = open(filename)
    timestamp, meas = pickle.load(f)
    f.close()
    return timestamp, meas

def filenameForTime(t):
    s = str(int(t))
    w1, w2 = s[:-5], s[-5:]
    return "spool/meas.%s/%s" % (w1, s)

def recentFilenames(n):
    times = []
    for recentDir in sorted(glob("spool/meas.*"))[-2:]:
        times.extend(glob("%s/*" % recentDir))
    times.sort(reverse=True)
    return times[:n]
