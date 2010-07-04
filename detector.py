# you have to NOT have ubuntu pkg 'python-stats' installed, since its
# io.py module breaks lxml.
# http://www.mail-archive.com/debian-bugs-rc@lists.debian.org/msg155370.html
# or https://bugs.launchpad.net/ubuntu/+source/lxml/+bug/287895
import lxml.etree

def parseVdsConfig(filename, district=4):
    """gets some data out of the vds file.

    returns a dict of {id : attrs} where attrs is a dict like:

    {'name': 'On 280 400 ft S of 280/17/880 IC', 'type': 'ML',
     'abs_postmile': None, 'longitude': '-121.966099',
     'latitude':'37.316287', 'freeway_id': '280', 'freeway_dir': 'N'}

    docs at
    http://pems.dot.ca.gov/?dnode=Help&content=help_var&tab=var_fmt#cfg
    """
    tree = lxml.etree.parse(open(filename))
    result = {}
    for vds in tree.xpath("/pems/district[@id = %s]/detector_stations/vds" %
                          district):
        d = {}
        for attr in ['name', 'type', 'freeway_id', 'freeway_dir',
                     'abs_pm', 'latitude', 'longitude']:
            d[attr] = vds.get(attr)
        result[vds.get('id')] = d

    return result

if __name__ == '__main__':
    c = parseVdsConfig("vds_config.xml")
    print c['401186']
