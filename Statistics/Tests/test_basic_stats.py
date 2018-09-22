from unittest import TestCase
from Statistics.Classes.basic_stats import basic_finance_stats
import matplotlib.pyplot as plt

class TestBasic_stats(TestCase):
    def setUp(self):
        self.fs = basic_finance_stats()

    def test_testData(self):
        data = self.fs.testData()
        print(data.head((10)))

    def test_movingAverage(self):
        windowSize = 50
        data = self.fs.testData()
        av = self.fs.movingAverage(data,windowSize)
        data['%s, window = %s' % (data.columns[0],windowSize)] = av
        data = data.astype(float)
        data.plot()
        plt.title('Moving Average')
        plt.show()
