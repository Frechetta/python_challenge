import json
import re
from pathlib import Path
from challenge import util
import sys


class Warehouse:
    def __init__(self, path='data'):
        self.path = Path(path)
        self.data = []
        self.keys = {}

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
                    if key not in self.keys:
                        self.data.append(event)

                    self.keys[index].add(key)

    def write(self, index, data):
        if not isinstance(data, dict) and not isinstance(data, list):
            raise Exception('data object is not a dict or list')

        self.path.mkdir(exist_ok=True)

        index_path = self.path / (index + '.json')

        key_func = util.get_key_func(index)

        if index not in self.keys:
            self.keys[index] = set()

        added_events = 0
        skipped_events = 0

        with index_path.open('a') as file:
            if isinstance(data, dict):
                key = key_func(data)

                if key not in self.keys[index]:
                    data['index'] = index
                    self.data.append(data)

                    file.write(json.dumps(data) + '\n')
                    added_events += 1
                else:
                    skipped_events += 1
            else:
                for line in data:
                    key = key_func(line)
                    if key not in self.keys[index]:
                        line['index'] = index
                        self.data.append(line)
                        file.write(json.dumps(line) + '\n')
                        added_events += 1
                    else:
                        skipped_events += 1

        print('added: {0}, skipped: {1}'.format(added_events, skipped_events))

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

        for event in self.data:
            keep = True

            for field_filter in field_filters:
                parts = field_filter.split('=')
                field = parts[0]
                value = parts[1]

                if field not in event or str(event[field]).lower() != value.lower():
                    keep = False
                    break

            if not keep:
                continue

            for text_filter in text_filters:
                if json.dumps(event).find(text_filter) < 0:
                    keep = False
                    break

            if keep:
                yield event


query = sys.argv[1]
print('query: ' + query)

wh = Warehouse()
results = wh.query(query)
print('results:')
print(list(results))
