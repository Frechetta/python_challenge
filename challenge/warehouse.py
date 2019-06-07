import json
import re
from pathlib import Path
from challenge import util
import sys


class Warehouse:
    def __init__(self, path='data'):
        self.path = Path(path)
        self.keys = {}
        self.open_files = {}
        self.open = False

        self.init()

    def init(self):
        if not self.path.exists():
            return

        files = self.path.glob('*')
        for file_path in files:
            index = str(file_path.name).replace('.json', '')

            key_func = util.get_key_func(index)

            if index not in self.keys:
                self.keys[index] = set()

            with file_path.open() as file:
                for line in file:
                    event = json.loads(line)

                    key = key_func(event)
                    self.keys[index].add(key)

    def __enter__(self):
        files = self.path.glob('*')
        for file_path in files:
            index = str(file_path.name).replace('.json', '')
            self.open_files[index] = file_path.open('a')

        self.open = True

        return self

    def __exit__(self, t, val, tb):
        for file in self.open_files.values():
            file.close()

        self.open = False

        if tb is not None:
            return False

        return True

    def write(self, index, data):
        if not self.open:
            raise Exception('Not within context of a warehouse!')

        if not isinstance(data, dict):
            raise Exception('Data object is not a dict. Type: {0}'.format(type(data)))

        self.path.mkdir(exist_ok=True)

        if index not in self.open_files:
            file_path = self.path / (index + '.json')
            self.open_files[index] = file_path.open('a')

        file = self.open_files[index]

        key_func = util.get_key_func(index)

        if index not in self.keys:
            self.keys[index] = set()

        key = key_func(data)

        if key not in self.keys[index]:
            data['index'] = index
            self.keys[index].add(key)
            file.write(json.dumps(data) + '\n')
            return True

        return False

    def query(self, query_str):
        quote_positions = [m.start() for m in re.finditer('"', query_str)]
        print(quote_positions)
        if len(quote_positions) % 2 != 0:
            print('Error: mismatched quotes')
            return []

        quote_pairs = list(zip(quote_positions[::2], quote_positions[1::2]))
        print(quote_pairs)

        for pair in quote_pairs:
            first = pair[0]
            second = pair[1]

            part1 = query_str[:first]
            part2 = query_str[first:second + 1]
            part3 = query_str[second + 1:]

            query_str = part1 + part2.replace(' ', '&sp;') + part3

        print(query_str)

        elements = query_str.split()
        elements = list(map(lambda e: e.replace('&sp;', ' ').replace('"', ''), elements))

        print(elements)

        field_filters = []
        text_filters = []

        for element in elements:
            if '=' in element:
                field_filters.append(element)
            else:
                text_filters.append(element)

        print(field_filters)
        print(text_filters)

        files = self.path.glob('*')
        for file_path in files:
            with file_path.open() as file:
                for line in file:
                    event = json.loads(line)
                    if self.filter_event(event, line, field_filters, text_filters):
                        yield event

    @staticmethod
    def filter_event(event, raw_event, field_filters, text_filters):
        for field_filter in field_filters:
            parts = field_filter.split('=')
            field = parts[0]
            value = parts[1]

            if field not in event or (value != '*' and str(event[field]).lower() != value.lower()):
                return False

        for text_filter in text_filters:
            if raw_event.find(text_filter) < 0:
                return False

        return True


if __name__ == '__main__':
    query = sys.argv[1]
    print('query: ' + query)

    wh = Warehouse()
    results = wh.query(query)
    print('results:')
    print(json.dumps(list(results)))
