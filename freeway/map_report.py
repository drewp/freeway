"""
boring map-layout of green..red dots according to speed
"""
from __future__ import division
import random
from nevow import flat, tags as T
from measurements import UpdatingMeasurements

def interp(x, lo, hi, outLo, outHi):
    return outLo + (outHi - outLo) * max(0, min(1, (x - lo) / (hi - lo)))

def mapHtml():
    width = 600
    height = 900

    dots = []
    for measurement in UpdatingMeasurements().measurements:
        try:
            speed = float(measurement['speed'])
        except ValueError:
            print "no speed", measurement
            continue
        color = "#%02x%02x%02x" % (interp(speed, 50, 0, 0, 255),
                                   interp(speed, 40, 80, 0, 255),
                                   0)

        pos = (interp(float(measurement['longitude']), -122.72, -121.50, 0, width),
               interp(float(measurement['latitude']), 38.41, 36.93, 0, height))

        dotText = u'\u2739'
        if random.random() > .9:
            dotText += ' %d' % speed
        dots.append(
            T.div(style="color: %s; position: absolute; left: %spx; top: %spx" %
                  (color, pos[0], pos[1]))[dotText])

    page = [T.raw('''<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN"
"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">'''),
            T.html(xmlns="http://www.w3.org/1999/xhtml")[
        T.head[T.style[T.raw('''
        * {
        font-size: 90%;
        }
        ''')]],
        T.body[
        T.div(style="width: %spx; height: %spx" % (width, height))[
        dots
        ]]]]
    return flat.flatten(page)

open("map.html", "w").write(mapHtml())
