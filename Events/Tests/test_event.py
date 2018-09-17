from unittest import TestCase
from Events.Classes.event import MarketEvent, FillEvent, OrderEvent, SignalEvent
import datetime

class Test_MarketEvent(TestCase):
    def test_init(self):
        ME = MarketEvent()
        self.assertEqual(ME.type,'MARKET')

class Test_SignalEvent(TestCase):
    def test_init(self):
        ticker = 'MMM'
        now = datetime.datetime.utcnow()
        type = 'LONG'
        SE = SignalEvent(ticker,now,type)
        self.assertEqual(SE.type,'SIGNAL')
        self.assertEqual(SE.datetime,now)
        self.assertEqual(SE.ticker,ticker)
