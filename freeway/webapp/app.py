
import logging, web, dateutil.tz, datetime
from web.contrib.template import render_genshi
from pymongo import Connection, DESCENDING
render = render_genshi('freeway/webapp', auto_reload=True)
from freeway.webapp.render import toPng
logging.basicConfig()
log = logging.getLogger()

log.setLevel(logging.DEBUG)


class root(object):
    def GET(self):
        db = Connection('bang', 27017)['freeway']

        import segment_report
        reload(segment_report)
        import route_history_report
        reload(route_history_report)
        #table = segment_report.table(latestMeas)
        shadows = web.input().get('shadow', 'on') !=  'off'

        diaNorth = route_history_report.Diagram(db, freewayDir='N', shadows=shadows)
        diaSouth = route_history_report.Diagram(db, freewayDir='S', shadows=shadows)

        dataTime = diaNorth.latestRow['time'].replace(tzinfo=dateutil.tz.tzutc())
        fetchTime = diaNorth.latestRow['fetchTime'].replace(tzinfo=dateutil.tz.tzutc())
        now = datetime.datetime.now(dateutil.tz.tzlocal())

        ua = web.ctx.environ['HTTP_USER_AGENT']
        print ua
        useSvg = (bool(int(web.input().get('svg', '1') != '0')) and
                  'Pre/' not in ua)
        print "svg", useSvg
        return render.index(
            #table=table,
            shadows=web.input().get('shadow', 'on') != 'off',
            dataTime=dataTime.astimezone(dateutil.tz.tzlocal()),
            fetchTime=fetchTime.astimezone(dateutil.tz.tzlocal()),
            now=now,
            useSvg=useSvg,
            diaNorth=diaNorth,
            diaSouth=diaSouth,
            )

class renderedDiagram(object):
    def GET(self, which):
        db = Connection('bang', 27017)['freeway']
        shadows = web.input().get('shadow', 'on') !=  'off'
        import route_history_report
        reload(route_history_report)

        dia = route_history_report.Diagram(
            db, 
            freewayDir={'north' : 'N', 'south' : 'S'}[which],
            shadows=shadows)
        web.header('Content-Type', 'image/png')
        web.header('Cache-Control', 'max-age=150')
        return toPng(dia.render())
        

urls = (r"/", "root",
        r'/(north|south).png', 'renderedDiagram',
        )

w = web.application(urls, globals(), autoreload=False)
application = w.wsgifunc()

if __name__ == '__main__':
    w.run()
