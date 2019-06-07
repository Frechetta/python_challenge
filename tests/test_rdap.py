import unittest
from unittest.mock import patch, MagicMock
import challenge.rdap as rdap


if __name__ == '__main__':
    unittest.main()


class TestGet(unittest.TestCase):
    def test_none_ip(self):
        with self.assertRaises(Exception):
            rdap.get(None)

    def test_not_an_ip(self):
        with self.assertRaises(Exception):
            rdap.get('not_an_ip')

    @patch('challenge.rdap._process_data')
    @patch('requests.get')
    def test_process_false(self, mock_get, mock_process):
        response = MagicMock()
        response.json = MagicMock(return_value='response_json')

        mock_get.return_value = response
        mock_process.return_value = 'processed_data'

        expected = 'response_json'
        actual = rdap.get('1.1.1.1', process=False)

        self.assertEqual(expected, actual)

    @patch('challenge.rdap._process_data')
    @patch('requests.get')
    def test_process_true_explicit(self, mock_get, mock_process):
        response = MagicMock()
        response.json = MagicMock(return_value='response_json')

        mock_get.return_value = response
        mock_process.return_value = 'processed_data'

        expected = 'processed_data'
        actual = rdap.get('1.1.1.1', process=True)

        self.assertEqual(expected, actual)

    @patch('challenge.rdap._process_data')
    @patch('requests.get')
    def test_process_true(self, mock_get, mock_process):
        response = MagicMock()
        response.json = MagicMock(return_value='response_json')

        mock_get.return_value = response
        mock_process.return_value = 'processed_data'

        expected = 'processed_data'
        actual = rdap.get('1.1.1.1')

        self.assertEqual(expected, actual)


class TestGetAll(unittest.TestCase):
    def test_none_ips(self):
        with self.assertRaises(TypeError):
            rdap.get_all(None)

    def test_no_ips(self):
        expected = []
        actual = rdap.get_all([])

        self.assertEqual(expected, actual)

    @patch('challenge.rdap.get')
    def test(self, mock_get):
        mock_get.side_effect = [['arr1', 1], ['arr2', 2], ['arr3', 3]]

        expected = ['arr1', 1, 'arr2', 2, 'arr3', 3]
        actual = rdap.get_all(['derp', 'herp', 'lerp'])

        self.assertEqual(expected, actual)


class TestProcessData(unittest.TestCase):
    def test_not_dict(self):
        with self.assertRaises(Exception):
            rdap._process_data(['i', 'am', 'a', 'list'])

    def test_no_desired_fields(self):
        data = {
            'key1': 'val1',
            'key2': 'val2'
        }

        expected = [{'class': 'root'}]

        actual = rdap._process_data(data)

        self.assertEqual(expected, actual)

    def test_interesting_fields(self):
        data = {
            'key1': 'val1',
            'key2': 'val2',
            'handle': 'cool_handle',
            'name': 'cool_name'
        }

        expected = [{'class': 'root', 'handle': 'cool_handle', 'name': 'cool_name'}]

        actual = rdap._process_data(data)

        self.assertEqual(expected, actual)

    @patch('challenge.rdap._parse_events')
    def test_events(self, mock_parse_events):
        mock_parse_events.return_value = {'event1': 'derp', 'event2': 'herp'}

        data = {
            'key1': 'val1',
            'key2': 'val2',
            'events': 'events'
        }

        expected = [{'class': 'root', 'event1': 'derp', 'event2': 'herp'}]

        actual = rdap._process_data(data)

        self.assertEqual(expected, actual)

    def test_status(self):
        data = {
            'key1': 'val1',
            'key2': 'val2',
            'status': ['status1', 'status2']
        }

        expected = [{'class': 'root', 'status': 'status1,status2'}]

        actual = rdap._process_data(data)

        self.assertEqual(expected, actual)

    def test_entities_none_desired(self):
        data = {
            'key1': 'val1',
            'key2': 'val2',
            'entities': [
                {
                    'name': 'entity1',
                    'objectClassName': 'derp'
                },
                {
                    'name': 'entity2',
                    'objectClassName': 'herp'
                }
            ]
        }

        expected = [{'class': 'root'}]

        actual = rdap._process_data(data)

        self.assertEqual(expected, actual)

    @patch('challenge.rdap._parse_entity')
    def test_entities(self, mock_parse_entity):
        mock_parse_entity.return_value = [{'name': 'entity2'}]

        data = {
            'key1': 'val1',
            'key2': 'val2',
            'entities': [
                {
                    'name': 'entity1',
                    'objectClassName': 'derp'
                },
                {
                    'name': 'entity2',
                    'objectClassName': 'entity'
                },
                {
                    'name': 'entity3',
                    'objectClassName': 'herp'
                }
            ],
            'handle': 'handle'
        }

        expected = [{'class': 'root', 'handle': 'handle'}, {'name': 'entity2'}]

        actual = rdap._process_data(data)

        self.assertEqual(expected, actual)

    @patch('challenge.rdap._parse_entity')
    @patch('challenge.rdap._parse_events')
    def test(self, mock_parse_events, mock_parse_entity):
        mock_parse_events.return_value = {'event1': 'derp', 'event2': 'herp'}
        mock_parse_entity.return_value = [{'name': 'entity2'}]

        data = {
            'key1': 'val1',
            'key2': 'val2',
            'handle': 'cool_handle',
            'name': 'cool_name',
            'events': 'events',
            'status': ['status1', 'status2'],
            'entities': [
                {
                    'name': 'entity1',
                    'objectClassName': 'derp'
                },
                {
                    'name': 'entity2',
                    'objectClassName': 'entity'
                },
                {
                    'name': 'entity3',
                    'objectClassName': 'herp'
                }
            ]
        }

        expected = [
            {
                'class': 'root',
                'handle': 'cool_handle',
                'name': 'cool_name',
                'event1': 'derp',
                'event2': 'herp',
                'status': 'status1,status2'
            },
            {
                'name': 'entity2'
            }
        ]

        actual = rdap._process_data(data)

        self.assertEqual(expected, actual)


class TestParseEvents(unittest.TestCase):
    def test_no_events(self):
        expected = {}
        actual = rdap._parse_events([])

        self.assertEqual(expected, actual)

    def test(self):
        events = [
            {
                'key1': 'val1'
            },
            {
                'key1': 'val1',
                'eventAction': 'cool action'
            },
            {
                'key1': 'val1',
                'eventAction': 'cool action 1',
                'eventDate': 'date1'
            },
            {
                'key1': 'val1',
                'eventDate': 'date'
            },
            {
                'key1': 'val1',
                'eventAction': 'cool action 2',
                'eventDate': 'date2'
            }
        ]

        expected = {
            'event_cool_action_1': 'date1',
            'event_cool_action_2': 'date2'
        }

        actual = rdap._parse_events(events)

        self.assertEqual(expected, actual)


class TestParseEntity(unittest.TestCase):
    pass


class TestParseVcard(unittest.TestCase):
    pass
