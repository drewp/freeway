
import logging, web, dateutil.tz, datetime
from web.contrib.template import render_genshi
from pymongo import Connection, DESCENDING
render = render_genshi('freeway/webapp', auto_reload=True)

logging.basicConfig()
log = logging.getLogger()

log.setLevel(logging.DEBUG)

db = Connection('bang', 27017)['freeway']

class root(object):
    def GET(self):
        import segment_report
        reload(segment_report)
        import route_history_report
        reload(route_history_report)
        #table = segment_report.table(latestMeas)
        shadows = web.input().get('shadow', 'on') !=  'off'

        diaNorth = route_history_report.Diagram(freewayDir='N', shadows=shadows)
        diaSouth = route_history_report.Diagram(freewayDir='S', shadows=shadows)

        dataTime = diaNorth.latestRow['time'].replace(tzinfo=dateutil.tz.tzutc())
        fetchTime = diaNorth.latestRow['fetchTime'].replace(tzinfo=dateutil.tz.tzutc())
        now = datetime.datetime.now(dateutil.tz.tzlocal())
        
        return render.index(
            #table=table,
            shadows=web.input().get('shadow', 'on') != 'off',
            dataTime=dataTime.astimezone(dateutil.tz.tzlocal()),
            fetchTime=fetchTime.astimezone(dateutil.tz.tzlocal()),
            now=now,
            diaNorth=diaNorth,
            diaSouth=diaSouth,
            )

urls = (r"/", "root",
        )

w = web.application(urls, globals(), autoreload=False)
application = w.wsgifunc()

