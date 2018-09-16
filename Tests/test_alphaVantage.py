from unittest import TestCase
from online_data_sources import AlphaVantage
from SQL import Symbol_SQL

class TestAlphaVantage(TestCase):
    def setUp(self):
        self.avSQL = AlphaVantage()
        self.symSQL = Symbol_SQL()

    def test_getDailyAdjusted(self):
        ticker = 'MMM'
        symbol = self.symSQL.sqlEntry(ticker)
        test_data = self.avSQL.getDailyAdjusted(symbol)[0]
        vendor = self.avSQL.vendorID(self.avSQL.name)

        self.assertEqual(test_data['symbol_id'],symbol['id'])
        self.assertEqual(test_data['data_vendor_id'],vendor['id'])

    def test_insertHistoricalData(self):
        self.avSQL.insertHistoricalData()
