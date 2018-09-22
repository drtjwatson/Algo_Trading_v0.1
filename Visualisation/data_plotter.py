from Securities_Master_Database.Classes.SQL import Daily_price_SQL
import pandas as pd
import matplotlib.pyplot as plt
import datetime

def plotAdjClosePrice(tickers,vendor_id,price_from):
    df = Daily_price_SQL().getAdjClosePrice(vendor_id,tickers,price_from)
    df =df.astype(float)
    df.plot()
    plt.title('Adj. Close Price')
    plt.show()

# main
ticker = ['MMM','AAPL','GOOGL']
vendor_id = 1
price_from = datetime.datetime.strptime("01/01/2010","%d/%m/%Y")
plotAdjClosePrice(ticker,vendor_id,price_from)