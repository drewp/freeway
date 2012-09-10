#!/usr/bin/python

import sys, time, datetime
from twisted.internet import reactor
import twisted.web
from twisted.python import log
from nevow import rend, appserver, inevow, tags as T, loaders

#from measurements import UpdatingMeasurements
#latestMeas = UpdatingMeasurements(runLoop=True, runOnce=False)

class Main(rend.Page):
    docFactory = loaders.stan([T.raw('''<?xml version="1.0" encoding="utf-8"?>
    <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN"
    "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">'''),
                T.html(xmlns="http://www.w3.org/1999/xhtml")[
            T.head[T.style[T.raw('''
* { font-family: sans-serif; }
table {
border-collapse: collapse;
}
table, td, th {
 border: 1px solid gray;
 padding: 3px;
}
.timing {
  margin-top: 1em;
  border: 1px solid #ccc;
  width: 30em;
}
.timing table, .timing td {
  border: 0;
}
.credit {
  margin-top: 1em;
  border: 1px solid #ccc;
  padding: .3em;
  background: #eee;
}          
.dir-N { background: #cfc; }
.dir-S { background: #fcf; }
            ''')]],
           T.body[
        #T.directive('table'),
        T.directive('hist'),
        T.directive('timing'),
        T.div(class_="credit")[T.a(href="http://pems.dot.ca.gov/")[
        "Data from PeMS, Caltrans"]],
        ]]])

    def renderHTTP(self, ctx):
        req = inevow.IRequest(ctx)
        req.setHeader('Content-type', 'application/xhtml+xml')
        return rend.Page.renderHTTP(self, ctx)

    def render_table(self, ctx, data):
        import segment_report
        reload(segment_report)
        return segment_report.table(latestMeas)

    def render_timing(self, ctx, data):
        return T.div(class_="timing")[
            T.table[
            T.tr[T.td["Data timestamp says "],
                 T.td[datetime.datetime.fromtimestamp(latestMeas.lastDataTime).isoformat()]],
            T.tr[T.td["Last fetched at "],
                 T.td[datetime.datetime.fromtimestamp(latestMeas.lastFtpTime).isoformat()]],
            T.tr[T.td["Page generated at "],
                 T.td[datetime.datetime.fromtimestamp(int(time.time())).isoformat()]],
            ]]

    def render_hist(self, ctx, data):
        import route_history_report
        reload(route_history_report)
        D = route_history_report.Diagram
        shadows = True
        if ctx.arg('shadow') == 'off':
            shadows = False
        return [T.div[T.h2['Southbound'],
                      D(shadows=shadows, freewayDir='S').render()
                      ],
                T.div[T.h2['Northbound'],
                      D(shadows=shadows, freewayDir='N').render()
                      ],
                T.div[T.a(href="?shadow=off")["(turn off shadows)"]]]
                      

if __name__ == '__main__':
    log.startLogging(sys.stdout)
    reactor.listenTCP(8009, appserver.NevowSite(Main()))
    reactor.run()

