import json
import reader
import geoip
import rdap


if __name__ == '__main__':
    print('Reading IPs from file')
    ips = reader.read_ips('ips.txt')
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
