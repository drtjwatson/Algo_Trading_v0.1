import datetime
import time
from alpha_vantage.timeseries import TimeSeries
from Securities_Master_Database.Classes.SQL import Data_vendor_SQL
from Securities_Master_Database.Classes.SQL import Symbol_SQL, Daily_price_SQL

class AlphaVantage(Data_vendor_SQL):
    def __init__(self):
        super().__init__()
        apiKey = '41THIACD2XXFFFOG'
        self.name = 'AlphaVantage'
        url = 'https://www.alphavantage.co/'
        support_email = 'support@alphavantage.co'
        self.newVendor(self.name, url, support_email, apiKey)

    def getDailyAdjusted(self, symbol, type='compact'):
        now = datetime.datetime.utcnow()
        ts = TimeSeries(key=self.sqlEntry(self.name)['api_key'], output_format='pandas')
        data, metadata = ts.get_daily_adjusted(symbol=symbol['ticker'], outputsize=type)
        last_updated_date = metadata['3. Last Refreshed']
        vendor_id = self.sqlEntry(self.name)['id']
        daily_prices = []
        for d in data.iterrows():
            daily_prices.append(({'data_vendor_id':vendor_id,
                                  'symbol_id':symbol['id'],
                                  'last_updated_date':last_updated_date,
                                  'created_date':now,
                                  'price_date':d[0],
                                  'open_price':d[1]['1. open'],
                                  'high_price': d[1]['2. high'],
                                  'low_price': d[1]['3. low'],
                                  'close_price': d[1]['4. close'],
                                  'volume': d[1]['6. volume'],
                                  'adj_close_price': d[1]['5. adjusted close'],
                                  'dividend_amount': d[1]['7. dividend amount'],
                                  'split_coeff': d[1]['8. split coefficient'],
                                  }))
        return daily_prices

    def insertHistoricalData(self):
        sym = Symbol_SQL()
        dp = Daily_price_SQL()
        symbols = sym.getTickers()
        vendor_id = self.vendorID(self.name)['id']
        API_call_interval = 10
        t_start = None
        for idx, s in enumerate(symbols):
            symbol_id = s['id']
            test_check = dp.checkExisting(vendor_id,symbol_id)
            if dp.checkExisting(vendor_id,symbol_id)==0:
                if t_start is None:
                    prices = self.getDailyAdjusted(s,'full')
                    t_start = time.clock()
                    print('idx = ' + repr(idx))
                else:
                    while time.clock() - t_start < API_call_interval:
                        continue
                    print('idx = ' + repr(idx) + ', delta = ' + repr(time.clock()-t_start) + 's')
                    prices = self.getDailyAdjusted(s, 'full')
                    t_start = time.clock()
                dp.insertData(prices)
