import pickle, bz2
import monetdb.sql
from monetdb.monetdb_exceptions import OperationalError

class MeasPickle(object):
    """
    old pickles cover 12322xxxxx to 12389xxxxx = 67 output files

    still need new pickles (and monet)
    """
    def readMeas(self, files):
        for filename in files:
            tm, pts = pickle.load(
                #bz2.BZ2File(
                open(
                    filename
                    )
                )
            for rec in pts:
                yield self.fixRec(tm, rec)

    def fixRec(self, tm, rec):
        rec['dataTime'] = rec['readTime'] = tm
        for x in ['vht', 'flow', 'occupancy', 'vmt', 'speed', 'q', 'delay']:
            if rec[x] == '':
                rec[x] = 0
            rec[x] = float(rec[x])
        for x in ['num_samples', 'pct_observed']:
            rec[x] = int(float(rec[x]))
        return rec

class MeasMongo(object):
    """
    mongo had 12786xxxxx to 13472xxxxx in about 138GB. FixedFile will
    be about 12GB (4GB compressed).

    in case I screwed up their times, here is a sample record from
    mongodb before I deleted that:
    {"_id" : ObjectId("4c366981b999a713ae00007a"), "delay" : 4.18, "fetchTime" : ISODate("2010-07-09T00:12:49.098Z"), "pct_observed" : 25, "vht" : 8.1, "time" : ISODate("2010-07-09T00:05:00Z"), "flow" : 453, "occupancy" : 0.2398, "q" : 31.4, "vds_id" : "400341", "num_samples" : 20, "vmt" : 253.68, "speed" : 31.4}
    
    """
    def __init__(self):
        import pymongo
        self.coll = pymongo.Connection("bang", tz_aware=True)['freeway']['meas']
        
    def readMeas(self):
        def sec(dt):
            return int((dt - datetime.datetime(1970,1,1,tzinfo=tzutc())
                        ).total_seconds())
        
        for doc in self.coll.find({"time":{"$gt":datetime.datetime.fromtimestamp(1320500000, tzutc())}}):
            doc['dataTime'] = sec(doc['time'])
            doc['readTime'] = sec(doc['fetchTime'])
            for x in ['vht', 'flow', 'occupancy', 'vmt', 'speed', 'q']:
                if doc[x] == "":
                    doc[x] = 0
            yield doc

class VdsMonet(object):

    def replaceVds(self, rows):
        """
        drop and recreate the whole vds table with these rows, specified as dicts
        """
        curs = self.conn.cursor()
        try:
            curs.execute("drop table vds")
            self.conn.commit()
        except OperationalError:
            self.conn.rollback()

        curs.execute("""
            create table vds (
              id varchar(255),
              name varchar(255),
              type varchar(255),
              freeway_id varchar(255),
              freeway_dir varchar(5),
              abs_pm real,
              latitude real,
              longitude real
              )""")
        self.conn.commit()
        for row in rows:
            curs.execute("""insert into vds (
              id,
              name,
              type,
              freeway_id,
              freeway_dir,
              abs_pm,
              latitude,
              longitude
              ) values (
              %(id)s,
              %(name)s,
              %(type)s,
              %(freeway_id)s,
              %(freeway_dir)s,
              %(abs_pm)s,
              %(latitude)s,
              %(longitude)s
              )""", row)
        self.conn.commit()

    @lru_cache(1000)
    def getVds(self, id):
        curs = self.conn.cursor()
        curs.execute("""
          select freeway_id, freeway_dir, abs_pm
          from vds
          where id=%s""", id)
        row = iter(curs).next()
        return dict(zip(['freeway_id', 'freeway_dir', 'abs_pm'], row))

    @logTime
    @lru_cache(1000)
    def vdsInRange(self, freeway_id, pmLow, pmHigh):
        curs = self.conn.cursor()
        curs.execute("""
          select id
          from vds
          where freeway_id = %(freeway_id)s and
          abs_pm >= %(pmLow)s and
          abs_pm <= %(pmHigh)s
          """, {'freeway_id': freeway_id,
                'pmLow': pmLow,
                'pmHigh': pmHigh})
        return [row[0] for row in curs]

    @lru_cache(1000)
    def pmLabel(self, freeway_id, pm):
        curs = self.conn.cursor()
        curs.execute("""
          select name from vds
          where freeway_id = %(freeway_id)s and abs_pm = %(pm)s
          """, {'freeway_id':freeway_id, 'pm':float(pm)})
        return iter(curs).next()[0]

class MeasMonet(object):
    def __init__(self, host='bang', database='freeway'):
        self.conn = monetdb.sql.Connection(host=host, database=database)
