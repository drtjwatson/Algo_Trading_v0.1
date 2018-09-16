from unittest import TestCase
from Classes.SQL import Daily_price_SQL, Symbol_SQL
from Classes.online_data_sources import AlphaVantage

class TestDaily_price_SQL(TestCase):
    def setUp(self):
        self.dpSQL = Daily_price_SQL()
        self.symSQL = Symbol_SQL()
        self.avSQL = AlphaVantage()

    def test_insertData(self):
        ticker = 'MMM'
        symbol = self.symSQL.sqlEntry(ticker)
        test_data = self.avSQL.getDailyAdjusted(symbol)
        self.dpSQL.insertData(test_data)

    def test_checkExising(self):
        ticker = 'MMM'
        vendor_id = self.avSQL.vendorID(self.avSQL.name)['id']
        symbol_id = self.symSQL.symbolID(ticker)['id']
        self.assertEqual(1,self.dpSQL.checkExisting(vendor_id,symbol_id))
        self.assertEqual(0, self.dpSQL.checkExisting(vendor_id, 'No_ticker'))

        ticker = 'ABT'
        vendor_id = self.avSQL.vendorID(self.avSQL.name)['id']
        symbol_id = self.symSQL.symbolID(ticker)['id']
        self.assertEqual(1, self.dpSQL.checkExisting(vendor_id, symbol_id))