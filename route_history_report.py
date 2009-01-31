"""
diagram of one route, showing the last few measurements
"""
from __future__ import division
import pickle, time
from nevow import tags as T, flat
from measurements import getRecentSets
import numpy
from numpy import array

path = T.Proto('path')
line = T.Proto('line')

def plotPoints(measurementSets, freewayId='101',
               postMileLow=408, postMileHigh=412.5, freewayDir='S'):
    """
    measurementSets is a sequence of timestamp,measurements

    output is 3 arrays:

    positions = [pos1, pos2, ...] # (postmile)
    times = [t1, t2, ...]
    speeds = [[pos1t1spd, pos2t1spd, ...],
              [pos1t2spd, pos2t2spd, ...]] # newest row is ON TOP
    labels = { postmile : name }
    """
    positions = []
    times = []
    speeds = []
    labels = {}
    for timestamp, measurements in measurementSets:
        meas = []
        for m in measurements:
            # (but consider beyond my strip, to see approaching traffic)
            pos = float(m['abs_pm'])
            if (m['freeway_id'] == freewayId and
                postMileLow < pos < postMileHigh and
                m['freeway_dir'] == freewayDir):
                meas.append((pos, float(m['speed'])))
                if pos not in labels:
                    labels[pos] = m['name']
        meas.sort()

        if not positions:
            positions = [pos for pos, speed in meas]
        else:
            assert positions == [pos for pos, speed in meas]
        times.append(timestamp)
        speeds.append([speed for pos, speed in meas])

    
    return array(positions), array(times), array(speeds), labels

def smoothSequence(x, kernelWidth=5):
    """returned sequence is shorter"""
    kernel = numpy.bartlett(kernelWidth)
    kernel /= kernel.sum()
    return numpy.convolve(x, kernel, mode='valid')
    

class Diagram(object):
   
    def __init__(self, shadows=False, **kw):
        """kwargs go to plotPoints"""
        self.shadows = shadows

        self.width = 900
        self.height = 500
        self.bottomMargin = 80
        
        self.pmLow = 408
        self.pmHigh = 412.5
        self.pixelPerMile = self.width / (self.pmHigh - self.pmLow)

        if 1:
            t1 = time.time()
            data = getRecentSets(10)
            positions, times, speeds, labels = plotPoints(data, **kw)
            print "fetch data in %s" % (time.time() - t1)
            pickle.dump((positions, times, speeds, labels),
                        open("/tmp/fwy.p", 'w'), -1)
        else:
            positions, times, speeds, labels = pickle.load(open("/tmp/fwy.p"))
            
        self.positions, self.times, self.speeds = positions, times, speeds
        self.labels = labels
        self.speedRange = (speeds.min() - 3, speeds.max() + 3)
        
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

        return T.Tag('svg')(xmlns="http://www.w3.org/2000/svg",
                            style="background: #ddd;",
                            width=self.width, height=self.height,
                            **{'xmlns:xlink':"http://www.w3.org/1999/xlink"})[
            T.Tag('style')[T.raw(self.style)],
            T.Tag('defs')[defs, T.raw(self.staticDefs)],
            elements]

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
          font-size: 13px;
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
            y = self.svgY(speed)
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
            tr = "translate(%s %s) rotate(14)" % (x, base + 25)
            elements.append(T.Tag('text')(transform=tr, class_="pos")[
                self.labels[pos]])
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

            pts = [(self.svgX(pos), self.svgY(speed)) for pos, speed in meas]

            d = "M%.02f,%.02f" % tuple(pts[0])
            for p1, p2 in zip(pts[:-1], pts[1:]):
                d += " S%.02f,%.02f %.02f,%.02f" % (
                    p2[0] - smallestDiffMile * self.pixelPerMile, p2[1],
                    p2[0], p2[1])

            rowFrac = rowNum / (len(self.times) - 1)
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
        for pos, speedCol, variance in zip(self.positions,
                                           self.speeds.transpose(),
                                           self.speeds.var(axis=0)):
            smoothedY = smoothSequence(speedCol, kernelWidth=5)

            # speed vector was newest -> oldest
            smoothedY = list(reversed(smoothedY))
            # now it's old->new


            # it looks bad when the very last point has changed
            # direction, but it's completely lost in the smoothed
            # version
            smoothedY.append(speedCol[-1])


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
