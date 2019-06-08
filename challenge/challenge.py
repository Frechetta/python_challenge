import sys
import argparse
import json
import lark
from challenge import geoip, rdap, reader, warehouse, search


class Challenge:
    def __init__(self, args):
        self.args = self._parse_args(args)
        self.warehouse = warehouse.Warehouse()

    def main(self):
        if self.args.path is not None:
            self.read_data(self.args.path)

        self.input_loop()

    @staticmethod
    def _parse_args(args):
        parser = argparse.ArgumentParser(description='Process IPs from a text file and/or query IP data')
        parser.add_argument('path', nargs='?', default=None)
        return parser.parse_args(args)

    def read_data(self, path):
        """
        Read file, pull out IPs, retrieve GeoIP and RDAP info on them, and write data to disk.
        :param path: path of file containing IPs
        :return: None
        """
        print('\nReading IPs from file...')
        ips = reader.read_ips(path)
        print('Done.')
        print(ips)

        print('\nRetrieving GeoIP and RDAP data for IPs and writing them to disk...')

        added_geo_ip_rows = 0
        skipped_geo_ip_rows = 0
        added_rdap_rows = 0
        skipped_rdap_rows = 0

        with self.warehouse.open() as wh:
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
        print(f'added: {added_geo_ip_rows}, skipped: {skipped_geo_ip_rows}')

        print('RDAP:')
        print(f'added: {added_rdap_rows}, skipped: {skipped_rdap_rows}')

    def input_loop(self):
        """
        Loop and accept input for querying the data.
        """
        searcher = search.Search(self.warehouse)

        running = True

        while running:
            try:
                print()

                s = input('> ')
                parts = s.split(' ')
                command = parts[0]

                if command == 'search':
                    query = ' '.join(parts[1:])

                    if not query:
                        print('No query!')
                        continue
                    try:
                        results = searcher.query('search ' + query)
                        print('results:')
                        print(json.dumps(list(results)))
                    except lark.exceptions.ParseError:
                        print('Parse error')
                        continue
                elif command == 'exit' or command == 'quit':
                    running = False
                else:
                    print(f'Unknown command: {command}')
                    continue
            except KeyboardInterrupt:
                running = False

        print()


if __name__ == '__main__':
    Challenge(sys.argv[1:]).main()
