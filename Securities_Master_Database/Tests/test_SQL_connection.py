from unittest import TestCase
from Securities_Master_Database.Classes.SQL import SQL_connection


class TestSQL_connection(TestCase):
    def test_openDatabaseConnection(self):
        self.fail()

    def test_insertIntoTable(self):
        self.fail()

    def test_insertIgnoreIntoTable(self):
        self.fail()

    def test_selectFromTable(self):
        self.fail()

    def test_getData(self):
        sql = SQL_connection()
        table_name = 'symbol'
        column_str = 'daily_price.price_date, daily_price.adj_close_price'
        conditional_statement = "symbol.ticker = 'MMM'"
        joined_table_names = [['daily_price','daily_price.symbol_id = symbol.id'],['data_vendor','daily_price.data_vendor_id = data_vendor.id']]
        data = sql.getData(table_name,column_str,conditional_statement,joined_table_names)

