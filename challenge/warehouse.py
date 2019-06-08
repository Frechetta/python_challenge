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
