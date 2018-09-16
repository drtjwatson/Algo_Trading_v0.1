import lxml.html
import datetime
import requests
from SQL import Symbol_SQL
from SQL import Exchange_SQL

class wiki_exchange(Exchange_SQL):
    def __init__(self):
        super().__init__()

    # formats into time string
    def formatIntoTimeType(self,txt):
        if (any(char.isdigit() for char in txt)):
            if '.' in txt:
                hours = txt.split('.')[0]
                digits = [char for char in hours if char.isdigit()]
                if len(digits) == 1:
                    hours = '0' + digits[0]
                else:
                    hours = digits[0] + digits[1]
                if '−' in txt:
                    hours = '−' + hours
                minutes = str(int(txt.split('.')[1]) * 6)
                timezone_offset_str = hours + ':' + minutes + ':00'
            else:
                digits = [char for char in txt if char.isdigit()]
                if len(digits) == 1:
                    hours = '0' + digits[0]
                else:
                    hours = digits[0] + digits[1]
                if '−' in txt:
                    hours = '-' + hours
                timezone_offset_str = hours + ':00:00'
        else:
            timezone_offset_str = None
        return timezone_offset_str

    def obtain_parse_wiki_stock_exchanges(self):
        now = datetime.datetime.utcnow()

        # Use libxml to download the list of S&P500 companies and obtain the symbol table
        page = requests.get('https://en.wikipedia.org/wiki/List_of_stock_exchanges')
        tree = lxml.html.fromstring(page.content)
        table = tree.xpath(' // *[ @ id = "mw-content-text"] / div / table[2]')[0]
        exchange_list = table.xpath('.//tr')[1:]

        # gets stock exchange abbreviations, and splits into array
        abbrevs = self.obtain_exchange_abbrev()

        # Obtain the stock exchange information for each row in the table
        exchanges = []
        for idx, exchange in enumerate(exchange_list):
            tds = exchange.getchildren()
            # converts to TIME format for MySQL
            if (len(tds[7].getchildren()) == 0):
                txt = tds[7].text.split('\n')[0]
            elif (len(tds[7].getchildren()) == 1):
                txt = tds[7].getchildren()[0].text.split('\n')[0]
            else:
                print('Error in finding table value')
            timezone_offset_str = self.formatIntoTimeType(txt)

            sd = {'name': tds[1].getchildren()[0].text,
                  'country': tds[2].getchildren()[0].tail,
                  'city': tds[3].getchildren()[0].text,
                  'timezone_offset': timezone_offset_str,
                  'abbrev': abbrevs[idx],
                  'created_date' : now,
                  'last_updated_date' : now}
            # Create a tuple (for the DB format) and append to the grand list
            exchanges.append(sd)
        return exchanges

    # obtains stock abbreviation sorted by capital (same as wiki)
    def obtain_exchange_abbrev(self):
        page = requests.get('https://www.stockmarketclock.com/exchanges')
        tree = lxml.html.fromstring(page.content)
        table = tree.xpath('//*[@id="exchangetable"]')[0]
        exchange_list = table.xpath('.//tr')[1:]

        abbrevs = []
        for exchange in exchange_list:
            tds = exchange.getchildren()
            sd = {'abbrev': tds[1].getchildren()[0].text}
            abbrevs.append((sd['abbrev']))
        return abbrevs

    # inserts aquired data from web page into database
    def insertWebData(self):
        exchanges = self.obtain_parse_wiki_stock_exchanges()
        self.insertData(exchanges)

class sp500_wiki(Symbol_SQL):
    def __init__(self):
        super().__init__()

    def obtain_parse_wiki_snp500(self):
        """Download and parse the Wikipedia list of S&P500
        constituents using requests and libxml.

        Returns a list of tuples for to add to MySQL."""

        # Stores the current time, for the created_at record
        now = datetime.datetime.utcnow()

        # Use libxml to download the list of S&P500 companies and obtain the symbol table
        page = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        tree = lxml.html.fromstring(page.content)
        table = tree.xpath(' // *[ @ id = "mw-content-text"] / div / table[1]')[0]
        symbolslist = table.xpath('.//tr')[1:]

        # Obtain the symbol information for each row in the S&P500 constituent table
        symbols = []
        print(symbolslist)
        for symbol in symbolslist:
            tds = symbol.getchildren()
            sd = {'ticker': tds[0].getchildren()[0].text,
                  'name': tds[1].getchildren()[0].text,
                  'sector': tds[3].text,
                  'industry': tds[4].text,
                  'CIK': tds[7].text,
                  'currency' : 'USD',
                  'created_date':now,
                  'last_updated_date':now,
                  'instrument':'stock'}
            # Create a tuple (for the DB format) and append to the grand list
            symbols.append(sd)
        return symbols

    # inserts aquired data from web page into database
    def insertWebData(self):
        symbols = self.obtain_parse_wiki_snp500()
        self.insertData(symbols)