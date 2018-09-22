import numpy as np
import pandas as pd
from Securities_Master_Database.Classes.SQL import Daily_price_SQL
import datetime

class basic_stats:
    def __init__(self):
        pass

    def testData(self):
        ticker = 'AAPL'
        price_from = datetime.datetime.strptime("01/01/2010", "%d/%m/%Y")
        data = Daily_price_SQL().getAdjClosePrice(1,ticker)
        return data

    def movingAverage(self,windowSize):
        movingAverage = pd.rolling_average(self.testData(),windowSize)
        return movingAverage