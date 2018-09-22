from Securities_Master_Database.Classes.SQL import Daily_price_SQL, Symbol_SQL
import matplotlib.pyplot as plt
import datetime

def plotAdjClosePrice(tickers,vendor_id,price_from):
    df = Daily_price_SQL().getAdjClosePrice(vendor_id,tickers,price_from)
    df =df.astype(float)
    df.plot()
    plt.title('Adj. Close Price')
    plt.show()

# main
tickers = Symbol_SQL().getTickers()
tickers = tickers['ticker'].iloc[0]

vendor_id = 1
price_from = datetime.datetime.strptime("01/01/2010","%d/%m/%Y")
plotAdjClosePrice(tickers,vendor_id,price_from)