import os, time
import cPickle as pickle
from glob import glob
from twisted.internet import task

from loop_data import parse5MinFile
from detector import parseVdsConfig
from memoize import lru_cache

class UpdatingMeasurements(object):
    """set of measurements that keeps itself up to date

    look at self.measurements, self.lastFtpTime, self.lastDataTime

    """
    def __init__(self, runLoop=True, runOnce=True):
        self.vds = parseVdsConfig("vds_config.xml")
        self.measurements = []
        self.lastFtpTime = None
        self.lastDataTime = None
        if runLoop:
            task.LoopingCall(self.update).start(5 * 60)
        else:
            if runOnce:
                self.update()

    def update(self):
        try:
            try:
                last = os.path.getmtime("spool/5minagg_latest.txt.gz")
            except OSError:
                last = None
            if last < time.time() - 295:
                print "fetching:"
                ret = os.system("./ftp_get")
                print "done", ret

            self.lastFtpTime = int(time.time())

            new = []
            timestamp, samples = parse5MinFile("spool/5minagg_latest.txt.gz")
            for sens in samples:
                try:
                    sens.update(self.vds[sens['vds_id']])
                    new.append(sens)
                except KeyError:
                    print "no vds data for vds_id %s" % sens['vds_id']
            self.lastDataTime = timestamp
            self.measurements[:] = new

            outDir = os.path.dirname(filenameForTime(timestamp))
            if not os.path.isdir(outDir):
                os.makedirs(outDir)
            f = open(filenameForTime(timestamp), "w")
            pickle.dump((timestamp, self.measurements), f, protocol=-1)
            f.close()
            
        except Exception, e:
            print "update failed:", e
            self.lastDataTime = None
            self.measurements = []

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
