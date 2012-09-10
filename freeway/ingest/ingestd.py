import os, time
from twisted.internet import reactor
from loop_data import parse5MinFile

class UpdateLoop(object):
    """
    task that fetches measurements into mongo every 5 minutes
    """
    def __init__(self, db, mock=False):
        self.db = db
        self.mock = mock
        self.update()

    def update(self):
        now = int(time.time())
        try:
            if not self.mock:
                print "fetching:"
                ret = os.system("./ftp_get")
                print "done", ret

            timestamp, samples = parse5MinFile("spool/5minagg_latest.txt.gz")
            print "now %s, datatime %s" % (now, timestamp)
            nextDataTime = timestamp + 5 * 60

            # culling boring sensors to save space
            print "parse", len(samples)
            samples = [s for s in samples if s.get('speed')]
            print "valid", len(samples)
            
            self.db.save(timestamp, now, samples)
        except Exception, e:
            print "update failed:", e
            nextDataTime = now

        # it would be nice to get in phase with their data, but their
        # timestamp is much older than my current clock (like 9.5
        # mins, today) so I'm not sure what to do
        delay = 5*60# max(5, nextDataTime - now)
        
        print "next download in %s sec" % delay
        reactor.callLater(delay, self.update)
