"""
monetdb install

http://dev.monetdb.org/downloads/deb/ to get packages

run
monetdbd start -n /opt/monet
for a nonforking server

then setup db
monetdb create freeway
monetdb start freeway
monetdb release freeway

"""
from __future__ import division
import monetdb.sql
import pymongo, time, multiprocessing, commands

conn = monetdb.sql.Connection(database='freeway')


def clearData():
    conn.execute("""delete from meas""")

def makeTable():
    conn.execute("""
    create table meas (
      delay real,
      fetchTime timestamp,
      pct_observed int,
      vht real,
      time timestamp,
      flow int,
      occupancy real,
      q real,
      vds_id varchar(10),
      num_samples int,
      vmt real,
      speed real
      )
    """)
    conn.commit()

def copy():
    measIn = pymongo.Connection()['freeway']['meas']
    curs = conn.cursor()
    sent = [0]
    t1 = time.time()
    def insone(doc):
        del doc['_id']
        for f in ['vht', 'occupancy', 'q', 'flow', 'vmt', 'speed']:
            if doc[f] == '':
                doc[f] = 0
        try:
            curs.execute("""INSERT INTO meas (
            delay,
            fetchTime,
            pct_observed ,
            vht ,
            time ,
            flow ,
            occupancy ,
            q ,
            vds_id ,
            num_samples ,
            vmt ,
            speed
            ) values (
            %(delay)s,
            %(fetchTime)s,
            %(pct_observed)s,
            %(vht)s,
            %(time)s,
            %(flow)s,
            %(occupancy)s,
            %(q)s,
            %(vds_id)s,
            %(num_samples)s,
            %(vmt)s,
            %(speed)s
            )""", (doc))
        except Exception:
            print doc
            raise
        sent[0] += 1
        now = time.time()
        if sent[0] % 1000 == 0:
            dps = sent[0] / (now - t1)
            print "%s: sent %s in %s, %s doc/sec, %s hr for all. using %s" % (
                doc['fetchTime'],
                sent[0], now - t1, dps,
                435000000/dps/3600.,
                commands.getoutput("du -hs /opt/monet/freeway").split()[0]
                )
            conn.commit()
    for doc in measIn.find():
        insone(doc)

    conn.commit()

if __name__ == "__main__":
    clearData()
    copy()
