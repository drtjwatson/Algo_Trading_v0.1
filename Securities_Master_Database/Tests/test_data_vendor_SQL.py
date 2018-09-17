from unittest import TestCase
from Securities_Master_Database.Classes.SQL import Data_vendor_SQL


class TestData_vendor_SQL(TestCase):
    def setUp(self):
        self.dvSQL = Data_vendor_SQL()

    def test_checkVendor(self):
        name = 'AlphaVantage'
        self.assertEqual(1,self.dvSQL.checkVendor(name))

        name = 'NoName'
        self.assertEqual(0,self.dvSQL.checkVendor(name))

    def test_setup(self):
        self.assertEqual('data_vendor', self.dvSQL.table_name)

    def test_vendorID(self):
        name = 'AlphaVantage'
        data = self.dvSQL.vendorID(name)
        self.assertEqual(1,data['id'])

    def test_sqlEntry(self):
        name = 'AlphaVantage'
        data = self.dvSQL.sqlEntry(name)
        self.assertEqual(name,data['name'])
