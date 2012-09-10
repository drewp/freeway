import monetdb.sql

class Db(object):
    """
    all times are integer unix seconds, even in the db
    """
    def __init__(self, host='bang', database='freeway'):
        self.conn = monetdb.sql.Connection(host=host, database=database)
        self.curs = self.conn.cursor()

    def resetSchema(self):
        self.conn.execute("drop table meas")
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
