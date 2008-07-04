import gzip

def parse5MinFile(filename):
    """
    list of dicts like:

    {'delay': '0', 'pct_observed': '100', 'vht': '2.2', 'flow': '227',
     'occupancy': '.0358', 'q': '69.3', 'travel_time': '',
     'vds_id': '401186', 'num_samples': '50', 'vmt': '150.955',
     'speed': '69.3'}

    docs at:
    http://pems.eecs.berkeley.edu/?dnode=Help&content=help_var&tab=var_fmt
    """
    txt = gzip.open(filename).read()
    lines = txt.splitlines()
    timestamp = lines[0]
    
    fields = ['vds_id', 'flow', 'occupancy', 'speed', 'vmt', 'vht',
              'q', 'travel_time', 'delay', 'num_samples', 'pct_observed']
    result = []
    for line in lines[1:]:
        result.append(dict(zip(fields, line.split(','))))

    return result


if __name__ == '__main__':
    import pprint
    pprint.pprint(parse5MinFile("5minagg_latest.txt.gz")[:3])
