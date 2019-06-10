import unittest
import shutil
import os
from challenge import warehouse, search


DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


if __name__ == '__main__':
    unittest.main()


class TestEverything(unittest.TestCase):
    wh = warehouse.Warehouse(path=DATA_DIR)

    @classmethod
    def setUpClass(cls):
        with cls.wh.open() as wh:
            wh.write('geoip', {'event': 1, 'k': 'v', 'ip': 7})
            wh.write('geoip', {'event': 2, 'k': 'v', 'ip': 10})
            wh.write('geoip', {'event': 3, 'k': 'v', 'ip': 15})
            wh.write('rdap', {'event': 4, 'k': 'v1', 'handle': 'handle1'})
            wh.write('rdap', {'event': 5, 'k': 'v2', 'handle': 'handle2'})
            wh.write('rdap', {'event': 6, 'k': 'v3', 'handle': 'handle3'})
            wh.write('ip_rdap', {'ip': 7, 'handle': 'handle1'})
            wh.write('ip_rdap', {'ip': 10, 'handle': 'handle2'})
            wh.write('ip_rdap', {'ip': 15, 'handle': 'handle3'})
            wh.write('ip_rdap', {'ip': 19, 'handle': 'handle3'})
            wh.write('ip_rdap', {'ip': 21, 'handle': 'handle3'})

    @classmethod
    def tearDownClass(cls):
        if cls.wh.path.exists():
            shutil.rmtree(str(cls.wh.path))

    def test_search_one_expression(self):
        expected = [
            {'event': 1, 'k': 'v', 'ip': 7, 'index': 'geoip'},
            {'event': 2, 'k': 'v', 'ip': 10, 'index': 'geoip'},
            {'event': 3, 'k': 'v', 'ip': 15, 'index': 'geoip'}
        ]

        actual = list(search.query('search index=geoip', self.wh))

        self.assertEqual(expected, actual)

    def test_search_conjunction(self):
        expected = [
            {'event': 2, 'k': 'v', 'ip': 10, 'index': 'geoip'}
        ]

        actual = list(search.query('search index=geoip ip=10', self.wh))

        self.assertEqual(expected, actual)

    def test_search_ne_expression(self):
        expected = [
            {'event': 2, 'k': 'v', 'ip': 10, 'index': 'geoip'},
            {'event': 3, 'k': 'v', 'ip': 15, 'index': 'geoip'}
        ]

        actual = list(search.query('search index=geoip ip!=7', self.wh))

        self.assertEqual(expected, actual)

    def test_search_lt_expression(self):
        expected = [
            {'event': 1, 'k': 'v', 'ip': 7, 'index': 'geoip'}
        ]

        actual = list(search.query('search index=geoip ip<10', self.wh))

        self.assertEqual(expected, actual)

    def test_search_le_expression(self):
        expected = [
            {'event': 1, 'k': 'v', 'ip': 7, 'index': 'geoip'},
            {'event': 2, 'k': 'v', 'ip': 10, 'index': 'geoip'}
        ]

        actual = list(search.query('search index=geoip ip<=10', self.wh))

        self.assertEqual(expected, actual)

    def test_search_gt_expression(self):
        expected = [
            {'event': 3, 'k': 'v', 'ip': 15, 'index': 'geoip'}
        ]

        actual = list(search.query('search index=geoip ip>10', self.wh))

        self.assertEqual(expected, actual)

    def test_search_ge_expression(self):
        expected = [
            {'event': 2, 'k': 'v', 'ip': 10, 'index': 'geoip'},
            {'event': 3, 'k': 'v', 'ip': 15, 'index': 'geoip'}
        ]

        actual = list(search.query('search index=geoip ip>=10', self.wh))

        self.assertEqual(expected, actual)

    def test_search_field_dne(self):
        expected = []

        actual = list(search.query('search index=geoip derp=herp', self.wh))

        self.assertEqual(expected, actual)

    def test_search_no_matches(self):
        expected = []

        actual = list(search.query('search index=geoip ip=2', self.wh))

        self.assertEqual(expected, actual)

    def test_search_asterisk(self):
        expected = [
            {'event': 1, 'k': 'v', 'ip': 7, 'index': 'geoip'},
            {'event': 2, 'k': 'v', 'ip': 10, 'index': 'geoip'},
            {'event': 3, 'k': 'v', 'ip': 15, 'index': 'geoip'}
        ]

        actual = list(search.query('search index=geoip ip=*', self.wh))

        self.assertEqual(expected, actual)

    def test_search_disjunction(self):
        expected = [
            {'event': 2, 'k': 'v', 'ip': 10, 'index': 'geoip'},
            {'event': 3, 'k': 'v', 'ip': 15, 'index': 'geoip'}
        ]

        actual = list(search.query('search index=geoip ip=10 OR ip=15', self.wh))

        self.assertEqual(expected, actual)

    def test_search_not(self):
        expected = [
            {'event': 1, 'k': 'v', 'ip': 7, 'index': 'geoip'},
            {'event': 3, 'k': 'v', 'ip': 15, 'index': 'geoip'}
        ]

        actual = list(search.query('search index=geoip NOT ip=10', self.wh))

        self.assertEqual(expected, actual)

    def test_search_disjunction_not(self):
        expected = [
            {'event': 1, 'k': 'v', 'ip': 7, 'index': 'geoip'},
            {'event': 2, 'k': 'v', 'ip': 10, 'index': 'geoip'},
            {'event': 3, 'k': 'v', 'ip': 15, 'index': 'geoip'}
        ]

        actual = list(search.query('search index=geoip NOT ip=10 OR event=2', self.wh))

        self.assertEqual(expected, actual)

    def test_search_fields(self):
        expected = [
            {'event': 1, 'ip': 7},
            {'event': 2, 'ip': 10},
            {'event': 3, 'ip': 15}
        ]

        actual = list(search.query('search index=geoip | fields event ip', self.wh))

        self.assertEqual(expected, actual)

    def test_search_fields_join(self):
        expected = [
            {'handle': 'handle1', 'index': ['ip_rdap', 'rdap'], 'event': 4, 'ip': 7},
            {'handle': 'handle2', 'index': ['ip_rdap', 'rdap'], 'event': 5, 'ip': 10},
            {'handle': 'handle3', 'index': ['ip_rdap', 'rdap'], 'event': 6, 'ip': [15, 19, 21]}
        ]

        actual = list(search.query('search index=ip_rdap OR index=rdap OR index=geoip | fields index ip handle event | join BY handle', self.wh))

        self.assertEqual(expected, actual)

    def test_search_fields_join_prettyprint_json(self):
        expected = [
            '{\n'
            '    "index": [\n'
            '        "ip_rdap",\n'
            '        "rdap"\n'
            '    ],\n'
            '    "ip": 7,\n'
            '    "handle": "handle1",\n'
            '    "event": 4\n'
            '}',
            '{\n'
            '    "index": [\n'
            '        "ip_rdap",\n'
            '        "rdap"\n'
            '    ],\n'
            '    "ip": 10,\n'
            '    "handle": "handle2",\n'
            '    "event": 5\n'
            '}',
            '{\n'
            '    "index": [\n'
            '        "ip_rdap",\n'
            '        "rdap"\n'
            '    ],\n'
            '    "ip": [\n'
            '        15,\n'
            '        19,\n'
            '        21\n'
            '    ],\n'
            '    "handle": "handle3",\n'
            '    "event": 6\n'
            '}'
        ]

        actual = list(search.query('search index=ip_rdap OR index=rdap | fields index ip handle event | join BY handle | prettyprint format=json', self.wh))

        self.assertEqual(expected, actual)

    def test_search_fields_join_prettyprint_table(self):
        expected = [
            'index                ip            handle   event  ',
            "['ip_rdap', 'rdap']  7             handle1  4      ",
            "['ip_rdap', 'rdap']  10            handle2  5      ",
            "['ip_rdap', 'rdap']  [15, 19, 21]  handle3  6      "
        ]

        actual = list(search.query('search index=ip_rdap OR index=rdap | fields index ip handle event | join BY handle | prettyprint format=table', self.wh))

        self.assertEqual(expected, actual)


class TestPipelineToJson(unittest.TestCase):
    def test(self):
        pipeline = search.Pipeline.create_pipeline('search index=geoip NOT ip=10 OR event>=2 | fields index ip handle event | join BY handle | prettyprint format=table')

        expected = [
            {
                'type': 'search',
                'expressions': [
                    {
                        'type': 'comparison',
                        'field': 'index',
                        'val': 'geoip',
                        'op': 'eq'
                    },
                    {
                        'type': 'disjunction',
                        'parts': [
                            {
                                'type': 'not',
                                'item': {
                                    'type': 'comparison',
                                    'field': 'ip',
                                    'val': '10',
                                    'op': 'eq'
                                }
                            },
                            {
                                'type': 'comparison',
                                'field': 'event',
                                'val': '2',
                                'op': 'ge'
                            }
                        ]
                    }
                ]
            },
            {
                'type': 'fields',
                'fields': ['index', 'ip', 'handle', 'event']
            },
            {
                'type': 'join',
                'by_field': 'handle'
            },
            {
                'type': 'prettyprint',
                'format': 'table'
            }
        ]

        actual = pipeline.to_json()

        self.assertEqual(expected, actual)
