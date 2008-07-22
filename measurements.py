import os, time
from twisted.internet import task

from loop_data import parse5MinFile
from detector import parseVdsConfig


class UpdatingMeasurements(object):
    """set of measurements that keeps itself up to date

    look at self.measurements, self.lastFtpTime, self.lastDataTime

    """
    def __init__(self):
        self.vds = parseVdsConfig("vds_config.xml")
        self.measurements = []
        self.lastFtpTime = None
        self.lastDataTime = None
        task.LoopingCall(self.update).start(5 * 60)

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
        except Exception, e:
            print "update failed:", e
            self.lastDataTime = None
            self.measurements = []
