import logging, web, datetime
from dateutil.tz import tzlocal
from web.contrib.template import render_genshi
render = render_genshi('freeway/report', auto_reload=True)
import freeway
print freeway
from freeway.db import getDb
logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)
logging.getLogger("monetdb").setLevel(logging.INFO)


class root(object):
    def GET(self):
        import route_history_report
        reload(route_history_report)
        #import segment_report
        #reload(segment_report)
        #table = segment_report.table(latestMeas)
        shadows = web.input().get('shadow', 'on') !=  'off'

        diaNorth = route_history_report.Diagram(db, freewayDir='N', shadows=shadows)
        diaSouth = route_history_report.Diagram(db, freewayDir='S', shadows=shadows)

        dataTime = datetime.datetime.fromtimestamp(diaNorth.latestRow['dataTime'], tzlocal())
        fetchTime = datetime.datetime.fromtimestamp(diaNorth.latestRow['readTime'], tzlocal())
        now = datetime.datetime.now(tzlocal())

        ua = web.ctx.environ['HTTP_USER_AGENT']
        print ua
        useSvg = (bool(int(web.input().get('svg', '1') != '0')) and
                  'Pre/' not in ua)
        print "svg", useSvg
        return render.index(
            #table=table,
            shadows=web.input().get('shadow', 'on') != 'off',
            dataTime=dataTime,
            fetchTime=fetchTime,
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
        from freeway.report.render import toPng
        return toPng(dia.render())
        

urls = (r"/", "root",
        r'/(north|south).png', 'renderedDiagram',
        )

db = getDb()
w = web.application(urls, globals(), autoreload=False)
application = w.wsgifunc()

if __name__ == '__main__':
    w.run()
