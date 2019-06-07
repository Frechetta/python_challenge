import unittest
from unittest.mock import patch, MagicMock
import challenge.geoip as geoip


if __name__ == '__main__':
    unittest.main()


class TestGet(unittest.TestCase):
    def test_none_ip(self):
        with self.assertRaises(Exception):
            geoip.get(None)

    def test_not_an_ip(self):
        with self.assertRaises(Exception):
            geoip.get('not_an_ip')

    @patch('challenge.geoip._process_data')
    @patch('requests.get')
    def test_process_false(self, mock_get, mock_process):
        response = MagicMock()
        response.json = MagicMock(return_value='response_json')

        mock_get.return_value = response
        mock_process.return_value = 'processed_data'

        expected = 'response_json'
        actual = geoip.get('1.1.1.1', process=False)

        self.assertEqual(expected, actual)

    @patch('challenge.geoip._process_data')
    @patch('requests.get')
    def test_process_true_explicit(self, mock_get, mock_process):
        response = MagicMock()
        response.json = MagicMock(return_value='response_json')

        mock_get.return_value = response
        mock_process.return_value = 'processed_data'

        expected = 'processed_data'
        actual = geoip.get('1.1.1.1', process=True)

        self.assertEqual(expected, actual)

    @patch('challenge.geoip._process_data')
    @patch('requests.get')
    def test_process_true(self, mock_get, mock_process):
        response = MagicMock()
        response.json = MagicMock(return_value='response_json')

        mock_get.return_value = response
        mock_process.return_value = 'processed_data'

        expected = 'processed_data'
        actual = geoip.get('1.1.1.1')

        self.assertEqual(expected, actual)


class TestGetAll(unittest.TestCase):
    def test_none_ips(self):
        with self.assertRaises(TypeError):
            geoip.get_all(None)

    def test_no_ips(self):
        expected = []
        actual = geoip.get_all([])

        self.assertEqual(expected, actual)

    @patch('challenge.geoip.get')
    def test(self, mock_get):
        mock_get.side_effect = [1, 2, 3]

        expected = [1, 2, 3]
        actual = geoip.get_all(['derp', 'herp', 'lerp'])

        self.assertEqual(expected, actual)


class TestProcessData(unittest.TestCase):
    def test_not_dict(self):
        with self.assertRaises(Exception):
            geoip._process_data(['i', 'am', 'a', 'list'])

    def test_no_location(self):
        data = {
            'key1': 'val1',
            'key2': 'val2'
        }

        expected = {
            'key1': 'val1',
            'key2': 'val2'
        }

        actual = geoip._process_data(data)

        self.assertEqual(expected, actual)

    def test(self):
        data = {
            'key1': 'val1',
            'location': 'location',
            'key2': 'val2'
        }

        expected = {
            'key1': 'val1',
            'key2': 'val2'
        }

        actual = geoip._process_data(data)

        self.assertEqual(expected, actual)
