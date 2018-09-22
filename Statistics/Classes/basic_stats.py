import pandas as pd
from Securities_Master_Database.Classes.SQL import Daily_price_SQL
import datetime

class basic_finance_stats:
    def __init__(self):
        pass

    def testData(self):
        ticker = 'AAPL'
        price_from = datetime.datetime.strptime("01/01/2016", "%d/%m/%Y")
        data = Daily_price_SQL().getAdjClosePrice(1,ticker,price_from)
        return data

    def movingAverage(self,windowSize):
        data = self.testData()
        av = data.rolling(windowSize).mean()
        return av