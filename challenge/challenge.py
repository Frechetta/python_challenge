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
        parser.add_argument('-v', '--verbose', action='store_true')
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

        write_stats = {
            'geoip': {
                'added': 0,
                'skipped': 0
            },
            'rdap': {
                'added': 0,
                'skipped': 0
            },
            'ip_rdap': {
                'added': 0,
                'skipped': 0
            }
        }

        with self.warehouse.open() as wh:
            for ip in ips.keys():
                geo_ip_info = geoip.get(ip)

                index = 'geoip'
                added = wh.write(index, geo_ip_info)
                if added:
                    write_stats[index]['added'] += 1
                else:
                    write_stats[index]['skipped'] += 1

                rdap_info = rdap.get(ip)
                for datum in rdap_info:
                    index = 'rdap'
                    added = wh.write(index, datum)
                    if added:
                        write_stats[index]['added'] += 1
                    else:
                        write_stats[index]['skipped'] += 1

                    index = 'ip_rdap'
                    added = wh.write(index, {'ip': ip, 'handle': datum['handle']})
                    if added:
                        write_stats[index]['added'] += 1
                    else:
                        write_stats[index]['skipped'] += 1

        print('Done.')

        for index, stats in write_stats.items():
            print(f'\n{index}')
            added = stats['added']
            skipped = stats['skipped']
            print(f'added: {added}, skipped: {skipped}')

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
                        results = searcher.query('search ' + query, verbose=self.args.verbose)
                        print('results:')
                        for result in results:
                            print(result)
                    except lark.exceptions.ParseError:
                        print('Parse error')
                        continue
                elif command == 'exit' or command == 'quit':
                    running = False
                elif command == '':
                    continue
                else:
                    print(f'Unknown command: {command}')
                    continue
            except KeyboardInterrupt:
                running = False

        print()


if __name__ == '__main__':
    Challenge(sys.argv[1:]).main()
