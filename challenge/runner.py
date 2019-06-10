import sys
import argparse
import lark
import signal
from multiprocessing import Pool, Manager
from functools import partial
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
        num_ips = len(ips)
        print(f'{num_ips} IPs found.')

        print('\nRetrieving GeoIP and RDAP data for IPs...')
        print('0%')

        with Manager() as manager:
            original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)

            pool = Pool(4)

            signal.signal(signal.SIGINT, original_sigint_handler)

            shared = manager.list([{'i': 0, 'p': 0, 'num': num_ips}])
            retrieve = partial(self.retrieve_ip_data, shared)

            try:
                results = pool.map_async(retrieve, ips.keys())
                results = results.get(1800)
            except KeyboardInterrupt:
                print('Terminating')
                pool.terminate()
                sys.exit(0)
            else:
                pool.close()

            pool.join()

        print('Done.')

        print('Writing data to disk...')

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
            for result in results:
                ip = result.pop('ip')

                for index, data in result.items():
                    if not data:
                        continue

                    if index == 'geoip':
                        added = wh.write(index, data)
                        if added:
                            write_stats[index]['added'] += 1
                        else:
                            write_stats[index]['skipped'] += 1
                    else:
                        for item in data:
                            if index == 'ip_rdap':
                                added = wh.write(index, {'ip': ip, 'handle': item['handle']})
                            else:
                                print(item)
                                added = wh.write(index, item)

                            if added:
                                write_stats[index]['added'] += 1
                            else:
                                write_stats[index]['skipped'] += 1

        for index, stats in write_stats.items():
            print(f'\n{index}')
            added = stats['added']
            skipped = stats['skipped']
            print(f'added: {added}, skipped: {skipped}')

    @staticmethod
    def retrieve_ip_data(shared, ip):
        result = {
            'ip': ip,
            'geoip': None,
            'rdap': [],
            'ip_rdap': []
        }

        geo_ip_info = geoip.get(ip)

        if geo_ip_info:
            result['geoip'] = geo_ip_info

            '''
            index = 'geoip'
            added = wh.write(index, geo_ip_info)
            if added:
                write_stats[index]['added'] += 1
            else:
                write_stats[index]['skipped'] += 1
            '''

        rdap_info = rdap.get(ip)

        if rdap_info:
            for datum in rdap_info:
                if 'handle' not in datum:
                    continue

                result['rdap'].append(datum)

                '''
                index = 'rdap'
                added = wh.write(index, datum)
                if added:
                    write_stats[index]['added'] += 1
                else:
                    write_stats[index]['skipped'] += 1
                '''

                result['ip_rdap'].append({'ip': ip, 'handle': datum['handle']})

                '''
                index = 'ip_rdap'
                added = wh.write(index, {'ip': ip, 'handle': datum['handle']})
                if added:
                    write_stats[index]['added'] += 1
                else:
                    write_stats[index]['skipped'] += 1
                '''

        d = shared[0]

        d['i'] += 1

        new_p = round(d['i'] / d['num'] * 100)

        if new_p != d['p']:
            print(f'{new_p}%')
            d['p'] = new_p

        shared[0] = d

        return result

    def input_loop(self):
        """
        Loop and accept input for querying the data.
        """
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
                        results = search.query('search ' + query, self.warehouse, verbose=self.args.verbose)
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
