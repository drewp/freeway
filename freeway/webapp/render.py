"""
render svg on the server for clients that can't do it
"""
from StringIO import StringIO
import rsvg, cairo, tempfile, time, sys

def toPng(svgText, size=(900,500)):
    t1 = time.time()
    inSvg = tempfile.NamedTemporaryFile()
    inSvg.write(svgText)
    inSvg.flush()
    
    s = rsvg.Handle(file=inSvg.name)
    img = cairo.ImageSurface(cairo.FORMAT_ARGB32, size[0], size[1])
    s.render_cairo(cairo.Context(img))
    print >>sys.stderr, "svg cairo render in %.02f ms" % (1000 * (time.time() - t1))
    t2 = time.time()
    out = StringIO()
    img.write_to_png(out)
    print >>sys.stderr, "png compress in %.02f ms" % (1000 * (time.time() - t2))
    return out.getvalue()

