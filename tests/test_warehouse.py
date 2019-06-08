import unittest
import os
from challenge import warehouse


if __name__ == '__main__':
    unittest.main()


class TestOpen(unittest.TestCase):
    def setUp(self):
        self.wh = warehouse.Warehouse()

    def test(self):
        self.assertFalse(self.wh.opened)

        with self.wh.open():
            self.assertTrue(self.wh.opened)

        self.assertFalse(self.wh.opened)
