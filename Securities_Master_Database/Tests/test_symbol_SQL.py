from unittest import TestCase
from Securities_Master_Database.Classes.SQL import Symbol_SQL


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

    def test_columnNames(self):
        data = self.symSQL.tableColumnNames()
        self.assertEqual('id',data[0])

    def test_getSymbol(self):
        data = self.symSQL.symbolID('MMM')
        self.assertEqual(data['id'],1)
        self.assertEqual(data['ticker'],'MMM')
