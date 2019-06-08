import unittest
import os
import challenge.reader as reader


if __name__ == '__main__':
    unittest.main()


class TestReadIps(unittest.TestCase):
    def test_none_path(self):
        with self.assertRaises(TypeError):
            reader.read_ips(None)

    def test_wrong_path(self):
        with self.assertRaises(FileNotFoundError):
            reader.read_ips('path/does/not/exist.txt')

    def test_empty_file(self):
        path = 'test.txt'

        open(path, 'w').close()

        expected = {}
        actual = reader.read_ips(path)

        self.assertEqual(expected, actual)

        os.remove(path)

    def test(self):
        expected = {
            '172.217.12.14': 1,
            '157.240.18.35': 2,
            '72.30.35.10': 1,
            '151.101.64.81': 1,
            '151.101.1.164': 1,
            '208.80.153.224': 2,
            '192.30.253.113': 1,
            '108.174.10.10': 2,
            '18.205.93.0': 1,
            '151.101.193.69': 1
        }

        actual = reader.read_ips('tests/resources/ips.txt')

        self.assertEqual(expected, actual)
