import time, logging
import monetdb.sql
from monetdb.monetdb_exceptions import OperationalError
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

class Db(object):
    """
    all times are integer unix seconds, even in the db
    """
    def __init__(self, host='bang', database='freeway'):
        self.conn = monetdb.sql.Connection(host=host, database=database)
        self.curs = self.conn.cursor()

    @logTime
    def recentMeas(self, vds, limit):
        """
        return the most recent measurement rows for any of the given
        vds_id values. limit applies to the total number of rows

        result rows are a limited dict
        """
        q = """select vds_id, speed, dataTime, readTime
               from meas
               where vds_id in (VDS_CHOICES)
               order by dataTime desc
               limit %s""".replace('VDS_CHOICES',
                                   ",".join("%s" for field in range(len(vds))))
        
        self.curs.execute(q, tuple(vds) + (limit,))
        out = []
        for row in self.curs:
            out.append({'vds_id' : row[0],
                        'speed' : row[1],
                        'dataTime' : row[2],
                        'readTime' : row[3]})
        return out

    def replaceVds(self, rows):
        """
        drop and recreate the whole vds table with these rows, specified as dicts
        """
        curs = self.conn.cursor()
        try:
            curs.execute("drop table vds")
            curs.execute("commit")
        except OperationalError:
            curs.execute("rollback")

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
        curs.execute("commit")
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
        curs.execute("commit")

    @lru_cache(1000)
    def getVds(self, id):
        self.curs.execute("""
          select freeway_id, freeway_dir, abs_pm
          from vds
          where id=%s""", id)
        row = iter(self.curs).next()
        return dict(zip(['freeway_id', 'freeway_dir', 'abs_pm'], row))

    @logTime
    @lru_cache(1000)
    def vdsInRange(self, freeway_id, pmLow, pmHigh):
        self.curs.execute("""
          select id
          from vds
          where freeway_id = %(freeway_id)s and
          abs_pm >= %(pmLow)s and
          abs_pm <= %(pmHigh)s
          """, {'freeway_id': freeway_id,
                'pmLow': pmLow,
                'pmHigh': pmHigh})
        return [row[0] for row in self.curs]

    @lru_cache(1000)
    def pmLabel(self, freeway_id, pm):
        self.curs.execute("""
          select name from vds
          where freeway_id = %(freeway_id)s and abs_pm = %(pm)s
          """, {'freeway_id':freeway_id, 'pm':float(pm)})
        return iter(self.curs).next()[0]

    def resetSchema(self):
        try:
            self.conn.execute("drop table meas")
        except OperationalError:
            pass
        self.conn.execute("""
            create table meas (
              readTime int,
              dataTime int,
              vds_id varchar(10),
              flow real,
              occupancy real,
              speed real,
              vmt real,
              vht real,
              q real,
              delay real,
              num_samples int,
              pct_observed real
              )
            """)
        self.conn.commit()
        print "created meas table"
        
    def save(self, timestamp, now, samples):
        """
        now (secs) is when we asked for the data
        timestamp (secs) is the timestamp in the pems file we got
        samples is a list of dicts of data
        """

        for s in samples:
            self.curs.execute("""INSERT INTO meas (
                  readTime,
                  dataTime,
                  vds_id,
                  flow,
                  occupancy,
                  speed,
                  vmt,
                  vht,
                  q,
                  delay,
                  num_samples,
                  pct_observed
                  ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                         (now, timestamp,
                          s.get('vds_id'),
                          s.get('flow'),
                          s.get('occupancy'),
                          s.get('speed'),
                          s.get('vmt'),
                          s.get('vht'),
                          s.get('q'),
                          s.get('delay'),
                          s.get('num_samples'),
                          s.get('pct_observed')))
        self.curs.execute("commit")

def getDb():
    return Db()
