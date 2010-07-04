import gzip, time

def parse5MinFile(filename):
    """    
    (1852985928, # timestamp from file
    {'delay': '0', 'pct_observed': '100', 'vht': '2.2', 'flow': '227',
     'occupancy': '.0358', 'q': '69.3', 'travel_time': '',
     'vds_id': '401186', 'num_samples': '50', 'vmt': '150.955',
     'speed': '69.3'})

    docs at:
    http://pems.dot.ca.gov/?dnode=Help&content=help_var&tab=var_fmt
    """
    txt = gzip.open(filename).read()
    lines = txt.splitlines()
    timestamp = time.mktime(time.strptime(lines[0], "%m/%d/%Y %H:%M:%S"))
    
    fields = ['vds_id', 'flow', 'occupancy', 'speed', 'vmt', 'vht',
              'q', 'travel_time', 'delay', 'num_samples', 'pct_observed']
    result = []
    for line in lines[1:]:
        result.append(dict(zip(fields, line.split(','))))

    return timestamp, result


if __name__ == '__main__':
    import pprint
    pprint.pprint(parse5MinFile("5minagg_latest.txt.gz")[:3])
