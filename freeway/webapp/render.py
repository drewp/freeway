"""
render svg on the server for clients that can't do it
"""
from StringIO import StringIO
import rsvg, cairo, tempfile

def toPng(svgText, size=(900,500)):
    inSvg = tempfile.NamedTemporaryFile()
    inSvg.write(svgText)
    inSvg.flush()
    
    s = rsvg.Handle(file=inSvg.name)
    img = cairo.ImageSurface(cairo.FORMAT_ARGB32, size[0], size[1])
    s.render_cairo(cairo.Context(img))
    out = StringIO()
    img.write_to_png(out)
    return out.getvalue()

