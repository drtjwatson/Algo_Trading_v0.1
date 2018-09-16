from unittest import TestCase
from SQL import Symbol_SQL


class TestSymbol_SQL(TestCase):
    def setUp(self):
        self.symSQL = Symbol_SQL()

    def test_getTickers(self):
        tickers = self.symSQL.getTickers()
        self.assertEqual('MMM',tickers[0]['ticker'])

    def test_setup(self):
        self.assertEqual('symbol', self.symSQL.table_name)

    def test_symbolID(self):
        ticker = 'MMM'
        data = self.symSQL.symbolID(ticker)
        self.assertEqual(1,data['id'])
