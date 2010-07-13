"""
diagram of one route, showing the last few measurements
"""
from __future__ import division
import pickle, time
from nevow import tags as T, flat
import numpy
from numpy import array
from pymongo import Connection, DESCENDING
from memoize import lru_cache

path = T.Proto('path')
line = T.Proto('line')

@lru_cache(1000)
def getVds(db, id):
    return db['vds'].find({'_id' : id}).next()

@lru_cache(1000)
def getLabel(db, freewayId, postMile):
    return db['vds'].find_one({'freeway_id' : freewayId, 'abs_pm' : float(postMile)},
                          ["name"])['name']


#fix flaky graph, add server-rendered rsvg mode





def smoothSequence(x, kernelWidth=5):
    """returned sequence is shorter"""
    kernel = numpy.bartlett(kernelWidth)
    kernel /= kernel.sum()
    return numpy.convolve(x, kernel, mode='valid')
    

class Diagram(object):
   
    def __init__(self, db, shadows=False, **kw):
        """kwargs go to plotPoints"""
        self.db = db
        self.shadows = shadows

        self.width = 900
        self.height = 500
        self.bottomMargin = 80
        
        self.pmLow = 408
        self.pmHigh = 425.5
        self.pixelPerMile = self.width / (self.pmHigh - self.pmLow)

        t1 = time.time()
        self.positions, self.times, self.speeds = self.plotPoints(**kw)
        print "fetch data in %s" % (time.time() - t1)
            
        self.speedRange = (
            0, #self.speeds.min() - 3,
            80, #self.speeds.max() + 3
            )

    def plotPoints(self, freewayId='101',
                   postMileLow=408, postMileHigh=425.5, freewayDir='S'):
        """
        measurementSets is a sequence of lists of measurements from the same time

        output is 3 arrays:

        positions = [pos1, pos2, ...] # (postmile)
        times = [t1, t2, ...]
        speeds = [[pos1t1spd, pos2t1spd, ...],
                  [pos1t2spd, pos2t2spd, ...]] # newest row is ON TOP
        """
        vdsInRange = [row['_id'] for row in
                      self.db['vds'].find({'freeway_id' : '101',
                                      'abs_pm' : {'$gt' : 408, '$lt' : 425.5}},
                                     fields=['_id'])]

        data = list(self.db['meas'].find({'vds_id' : {'$in' : vdsInRange}}
                                    ).sort('time', DESCENDING
                                           ).limit(10 * len(vdsInRange)))
        self.latestRow = data[0]
        measurementSets = {} # time : [meas]
        for row in data:
            measurementSets.setdefault(row['time'], []).append(row)


        positions = []
        times = []
        speeds = []

        allPos = set()
        for measurements in measurementSets.values():
            for m in measurements:
                vds = getVds(self.db, m['vds_id'])
                if (vds['freeway_id'] == freewayId and
                    postMileLow < vds['abs_pm'] < postMileHigh and
                    vds['freeway_dir'] == freewayDir):
                    allPos.add(getVds(self.db, m['vds_id'])['abs_pm'])
        positions = sorted(allPos)
        print "freewayDir %s, %s" % (freewayDir, positions[:5])

        for measurements in measurementSets.values():
            speedAtPos = {} # abs_pm : speed

            for m in measurements:
                # (but consider beyond my strip, to see approaching traffic)
                vds = getVds(self.db, m['vds_id'])
                pos = vds['abs_pm']
                if (vds['freeway_id'] == freewayId and
                    postMileLow < pos < postMileHigh and
                    vds['freeway_dir'] == freewayDir):
                    assert pos in allPos
                    speedAtPos[pos] = m['speed']

            times.append(time.mktime(m['time'].timetuple())) # pick any measurement from the set
            speeds.append([speedAtPos.get(pos, None) for pos in positions])



        return array(positions), array(times), array(speeds)

        
    def render(self):
        defs = []
        elements = []
        for maker in [
            self.makeSpeedGrid,
            self.makePosGrid,
            self.makeSpeedCurves,
            self.makeDirections,
            ]:
            d, e = maker()
            defs.extend(d)
            elements.append(T.Tag('g')[e])

        return flat.flatten(T.Tag('svg')(xmlns="http://www.w3.org/2000/svg",
                            style="background: #ddd;",
                            width=self.width, height=self.height,
                            **{'xmlns:xlink':"http://www.w3.org/1999/xlink"})[
            T.Tag('style')[T.raw(self.style)],
            T.Tag('defs')[defs, T.raw(self.staticDefs)],
            elements])

    style = '''
        path.direction {
          fill:none;
          marker-end:url(#endArrow);
          opacity:0.7;
          stroke:#446600;
          stroke-linejoin:round;
          stroke-width:5;
        }
        path.directionBg {
          fill: none;
          stroke-width: 10;
          stroke:white;
          marker-end: none;
          opacity: .3;
        }
        path.speed {
           fill: none;
           /* mask: url(#Mask); */
        }
        g.speedShadow {
          opacity: .6;
        }
        path.ygrid {
           stroke-width: .2;
           stroke: black;
        }
        path.posGrid {
           stroke-width: .2;
           stroke: green;
        }
        text.mph {
          font-family: Verdana;
          font-size: 12px;
        }
        text.pos {
          font-family: Verdana;
          font-size: 10px;
        }
        '''
    
    staticDefs = '''
        <filter id="shadow">
          <feGaussianBlur stdDeviation="2"/>
        </filter>

        <marker id="endArrow" viewBox="-4 -5 14 10"
           markerUnits="userSpaceOnUse" orient="auto"
           markerWidth="40" markerHeight="30">
          <polyline points="0,0 -1,-3 7,0 -1,3" fill="#030"/>
        </marker>

        <linearGradient id="Gradient" gradientUnits="userSpaceOnUse"
                        x1="200" y1="0" x2="400" y2="0">
          <stop offset="0" stop-color="white" stop-opacity="0"/>
          <stop offset=".5" stop-color="white" stop-opacity="1"/>
          <stop offset="1" stop-color="white" stop-opacity="0"/>
        </linearGradient>

        <mask id="Mask" maskUnits="userSpaceOnUse" x="0" y="0"
              width="800" height="500">
          <rect x="200" y="0" width="200" height="500" fill="url(#Gradient)"/>
        </mask>
        '''

    def makeSpeedGrid(self):
        elements = []
        spread = self.speedRange[1] - self.speedRange[0]
        if spread > 30:
            spacing = 10
        elif spread > 10:
            spacing = 5
        else:
            spacing = 2
        
        for speed in range(90, 0, -spacing):
            try:
                y = self.svgY(speed)
            except TypeError:
                continue
            if not 10 < y < self.height - self.bottomMargin:
                continue
            txt = str(speed)
            if not elements:
                txt += " mph"
            elements.append(T.Tag('text')(x=5, y=y, class_='mph')[txt])
            elements.append(path(d='M50,%.02f L%s,%.02f' % (y,self.width,y),
                                      class_='ygrid'))
        return [], elements

    def makePosGrid(self):
        base = self.height - self.bottomMargin
        elements = [path(d="M50,%s L%s,%s" % (base, self.width, base),
                         class_="posGrid")]
        lastX = None
        for pos in self.positions:
            x = self.svgX(pos)
            elements.append(path(d="M%.02f,%s L%.02f,0" % (x, base, x),
                                 class_="posGrid"))

            if lastX and x - lastX < 20:
                continue
            lastX = x
            tr = "translate(%s %s) rotate(-90)" % (x, base + 75)
            elements.append(T.Tag('text')(transform=tr, class_="pos")["%6.2f %s" % (pos, getLabel(self.db, '101', pos))])
        return [], elements
    
    def makeSpeedCurves(self):
        """main linegraph elements"""
        defs = []
        reversedElements = []
        for rowNum, (timestamp, speedRow) in enumerate(zip(self.times,
                                                           self.speeds)):
            meas = zip(self.positions, speedRow)
            smallestDiffMile = min(p2[0] - p1[0]
                                   for p1, p2 in zip(meas[:-1], meas[1:]))

            pts = []
            for pos, speed in meas:
                try:
                    pts.append((self.svgX(pos), self.svgY(speed)))
                except TypeError:
                    continue

            d = "M%.02f,%.02f" % tuple(pts[0])
            for p1, p2 in zip(pts[:-1], pts[1:]):
                d += " S%.02f,%.02f %.02f,%.02f" % (
                    p2[0] - smallestDiffMile * self.pixelPerMile, p2[1],
                    p2[0], p2[1])

            rowFrac = rowNum / max(1, len(self.times) - 1)
            lineWidth = (1 - rowFrac) * 3 + rowFrac * 0

            pathId = "speed-%s-%s" % (id(self), rowNum)
            defs.append(path(class_="speed", id=pathId, d=d))
            pathInstance = T.Tag('use')(**{'xlink:href':'#'+pathId})

            style = "stroke: #%02x%02x%02x; stroke-width: %s" % (
                255 * (1 - rowFrac) + 96 * rowFrac, 96*rowFrac, 96*rowFrac,
                lineWidth)
            reversedElements.append(T.Tag('g')(style=style)[pathInstance])

            # shadow goes after, since speedPaths gets reversed
            if self.shadows:
                reversedElements.append(T.Tag('g')(class_="speedShadow",
                                             transform="translate(1.5 2)",
                                             filter="url(#shadow)",
                                             stroke='#000',
                                             **{'stroke-width' : lineWidth}
                                             )[pathInstance])

        reversedElements.reverse()
        return defs, reversedElements # oldest on the bottom

    def makeDirections(self):
        """the little sparkline-ish arrows that show how each
        datapoint is changing"""
        elements = []

        for pos, speedCol in zip(self.positions, self.speeds.transpose()):
            #variance = speeds.var(axis=0)
            try:
                smoothedY = smoothSequence(speedCol, kernelWidth=5)
            except TypeError: # nones in the data
                continue

            # speed vector was newest -> oldest
            smoothedY = list(reversed(smoothedY))
            # now it's old->new

            # it looks bad when the very last point has changed
            # direction, but it's completely lost in the smoothed
            # version
            smoothedY.append(speedCol[0])

            dx = 5
            startXOffset = len(smoothedY) * dx / 2

            d = "M%.02f,%.02f" % (self.svgX(pos) - startXOffset,
                                  self.svgY(smoothedY[0]))
            for i, y in enumerate(smoothedY[1:]):
                d += " L%.02f,%.02f" % (self.svgX(pos) - startXOffset + i * dx,
                                        self.svgY(y))
            if self.shadows:
                elements.append(path(d=d, class_="directionBg"))
            elements.append(path(d=d, class_="direction"))
        return [], elements

    def svgX(self, pos):
        return 50 + (pos - self.pmLow) * self.pixelPerMile
    
    def svgY(self, speed):
        smin, smax = self.speedRange
        return (self.height - self.bottomMargin) * (1 - (speed - smin) / (smax - smin))


if __name__ == '__main__':
    open("rhr.svg", 'w').write(flat.flatten(Diagram(shadows=True).render()))
