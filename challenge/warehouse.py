import json
import re
from pathlib import Path
from contextlib import contextmanager
from challenge import util


class Warehouse:
    """
    Manages reading and writing data.
    """
    def __init__(self, path='data'):
        self.path = Path(path)
        self.keys = {}
        self.open_files = {}
        self.opened = False

    @contextmanager
    def open(self):
        """
        Open the warehouse by reading keys from data files and opening them for writing
        """
        try:
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

                self.open_files[index] = file_path.open('a')

            self.opened = True

            yield self
        finally:
            for file in self.open_files.values():
                file.close()

            self.opened = False

    def write(self, index, data):
        """
        Write data to an index (file).
        :param index:
        :param data:
        :return: True if the data was written; False if it was not
        """
        if not self.open:
            raise Exception('Not within context of a warehouse!')

        if not isinstance(data, dict):
            raise Exception(f'Data object is not a dict. Type: {type(data)}')

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
        """
        Run a query against the data and yield the results.
        :param query_str:
        """
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
                    if self._filter_event(event, line, field_filters, text_filters):
                        yield event

    @staticmethod
    def _filter_event(event, raw_event, field_filters, text_filters):
        """
        Filter an event.
        :param event:
        :param raw_event: the event in string format
        :param field_filters: filters involving a key, value pair
        :param text_filters: filters involving plain text
        :return: True if the event matches the filters; False if not
        """
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
