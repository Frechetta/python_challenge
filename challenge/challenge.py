import sys
from challenge import geoip, rdap, reader, warehouse


def main(args):
    path = args[0]

    print('\nReading IPs from file...')
    ips = reader.read_ips(path)
    print('Done.')
    print(ips)

    print('\nRetrieving GeoIP and RDAP data for IPs and writing them to disk...')

    added_geo_ip_rows = 0
    skipped_geo_ip_rows = 0
    added_rdap_rows = 0
    skipped_rdap_rows = 0

    with warehouse.Warehouse() as wh:
        for ip in ips.keys():
            geo_ip_info = geoip.get(ip)
            added = wh.write('geoip', geo_ip_info)
            if added:
                added_geo_ip_rows += 1
            else:
                skipped_geo_ip_rows += 1

            rdap_info = rdap.get(ip)
            for datum in rdap_info:
                added = wh.write('rdap', datum)
                if added:
                    added_rdap_rows += 1
                else:
                    skipped_rdap_rows += 1

    print('Done.')

    print('\nGeoIP:')
    print('added: {0}, skipped: {1}'.format(added_geo_ip_rows, skipped_geo_ip_rows))

    print('RDAP:')
    print('added: {0}, skipped: {1}'.format(added_rdap_rows, skipped_rdap_rows))


if __name__ == '__main__':
    main(sys.argv[1:])
