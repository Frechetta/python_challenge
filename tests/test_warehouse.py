import unittest
import json
import shutil
import os
from challenge import warehouse


DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


if __name__ == '__main__':
    unittest.main()


class TestOpen(unittest.TestCase):
    def setUp(self):
        self.wh = warehouse.Warehouse(path=DATA_DIR)

    def test(self):
        self.assertFalse(self.wh.opened)

        with self.wh.open():
            self.assertTrue(self.wh.opened)

        self.assertFalse(self.wh.opened)


class TestWrite(unittest.TestCase):
    def setUp(self):
        self.wh = warehouse.Warehouse(path=DATA_DIR)

    def tearDown(self):
        if self.wh.path.exists():
            shutil.rmtree(str(self.wh.path))

    def test_not_open(self):
        with self.assertRaises(Exception):
            self.wh.write('index', 'data')

    def test_data_not_dict(self):
        with self.wh.open() as wh:
            with self.assertRaises(Exception):
                wh.write('index', 'data')

    def test_no_prior_data(self):
        self.assertFalse(self.wh.path.exists())
        self.assertFalse(self.wh.open_files)
        self.assertFalse(self.wh.keys)

        data = {'k1': 'v1', 'k2': 'v2', 'ip': 5}

        with self.wh.open() as wh:
            added = wh.write('geoip', data)
            self.assertEqual(['geoip'], list(self.wh.open_files.keys()))

        self.assertEqual([self.wh.path / 'geoip.json'], list(self.wh.path.glob('*')))
        self.assertFalse(self.wh.open_files)
        self.assertEqual({'geoip': {5}}, self.wh.keys)
        self.assertTrue(added)

        with (self.wh.path / 'geoip.json').open() as file:
            contents = json.loads(file.read())
            self.assertEqual(data, contents)

    def test_prior_data_different_index(self):
        self.wh.path.mkdir()

        geoip_data = {'k1': 'v1', 'k2': 'v2', 'ip': 5}
        with (self.wh.path / 'geoip.json').open('w') as file:
            file.write(json.dumps(geoip_data))

        self.assertTrue(self.wh.path.exists())
        self.assertFalse(self.wh.open_files)
        self.assertFalse(self.wh.keys)

        rdap_data = {'k1': 'v1', 'k2': 'v2', 'handle': 'derp'}

        with self.wh.open() as wh:
            added = wh.write('rdap', rdap_data)
            self.assertEqual(['geoip', 'rdap'], list(self.wh.open_files.keys()))

        self.assertEqual({self.wh.path / 'geoip.json', self.wh.path / 'rdap.json'}, set(self.wh.path.glob('*')))
        self.assertFalse(self.wh.open_files)
        self.assertEqual({'geoip': {5}, 'rdap': {'derp'}}, self.wh.keys)
        self.assertTrue(added)

        with (self.wh.path / 'geoip.json').open() as file:
            contents = json.loads(file.read())
            self.assertEqual(geoip_data, contents)

        with (self.wh.path / 'rdap.json').open() as file:
            contents = json.loads(file.read())
            self.assertEqual(rdap_data, contents)

    def test_prior_data_same_index_no_collisions(self):
        self.wh.path.mkdir()

        geoip_data = {'k1': 'v1', 'k2': 'v2', 'ip': 5}
        with (self.wh.path / 'geoip.json').open('w') as file:
            file.write(json.dumps(geoip_data))

        rdap_data1 = {'k1': 'v1', 'k2': 'v2', 'handle': 'derp'}
        with (self.wh.path / 'rdap.json').open('w') as file:
            file.write(json.dumps(rdap_data1) + '\n')

        self.assertTrue(self.wh.path.exists())
        self.assertFalse(self.wh.open_files)
        self.assertFalse(self.wh.keys)

        rdap_data2 = {'k1': 'v1', 'k2': 'v2', 'handle': 'herp'}

        with self.wh.open() as wh:
            added = wh.write('rdap', rdap_data2)
            self.assertEqual(['geoip', 'rdap'], list(self.wh.open_files.keys()))

        self.assertEqual([self.wh.path / 'geoip.json', self.wh.path / 'rdap.json'], list(self.wh.path.glob('*')))
        self.assertFalse(self.wh.open_files)
        self.assertEqual({'geoip': {5}, 'rdap': {'derp', 'herp'}}, self.wh.keys)
        self.assertTrue(added)

        with (self.wh.path / 'geoip.json').open() as file:
            contents = json.loads(file.read())
            self.assertEqual(geoip_data, contents)

        with (self.wh.path / 'rdap.json').open() as file:
            actual_events = [json.loads(line) for line in file]
            expected_events = [rdap_data1, {'index': 'rdap', **rdap_data2}]
            self.assertEqual(expected_events, actual_events)

    def test_prior_data_same_index_with_collision(self):
        self.wh.path.mkdir()

        geoip_data = {'k1': 'v1', 'k2': 'v2', 'ip': 5}
        with (self.wh.path / 'geoip.json').open('w') as file:
            file.write(json.dumps(geoip_data))

        rdap_data1 = {'k1': 'v1', 'k2': 'v2', 'handle': 'derp'}
        with (self.wh.path / 'rdap.json').open('w') as file:
            file.write(json.dumps(rdap_data1) + '\n')

        self.assertTrue(self.wh.path.exists())
        self.assertFalse(self.wh.open_files)
        self.assertFalse(self.wh.keys)

        rdap_data2 = {'k3': 'v3', 'k4': 'v4', 'handle': 'derp'}

        with self.wh.open() as wh:
            added = wh.write('rdap', rdap_data2)
            self.assertEqual(['geoip', 'rdap'], list(self.wh.open_files.keys()))

        self.assertEqual([self.wh.path / 'geoip.json', self.wh.path / 'rdap.json'], list(self.wh.path.glob('*')))
        self.assertFalse(self.wh.open_files)
        self.assertEqual({'geoip': {5}, 'rdap': {'derp'}}, self.wh.keys)
        self.assertFalse(added)

        with (self.wh.path / 'geoip.json').open() as file:
            contents = json.loads(file.read())
            self.assertEqual(geoip_data, contents)

        with (self.wh.path / 'rdap.json').open() as file:
            contents = json.loads(file.read())
            self.assertEqual(rdap_data1, contents)
