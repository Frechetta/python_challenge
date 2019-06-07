import sys
import json
from challenge import geoip, rdap, reader, warehouse

if __name__ == '__main__':
    wh = warehouse.Warehouse()

    path = sys.argv[1]

    print('Reading IPs from file')
    ips = reader.read_ips(path)
    print('Done')
    print(ips)

    print('Retrieving geoip information')
    geo_ip_info = geoip.get_all(ips.keys())
    print('Done')
    print(json.dumps(geo_ip_info, indent=4))

    print('Retrieving rdap information')
    rdap_info = rdap.get_all(ips.keys())
    print('Done')
    print(json.dumps(rdap_info, indent=4))

    print('Writing GeoIP data to warehouse')
    wh.write('geoip', geo_ip_info)
    print('Writing RDAP data to warehouse')
    wh.write('rdap', rdap_info)
