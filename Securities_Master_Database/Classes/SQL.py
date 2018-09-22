import mysql.connector
from mysql.connector import errorcode
from math import ceil
import datetime
import pandas

# super class of SQL_connection
class SQL_connection():
    def __init__(self, host_name='localhost', user_name='tom', database_name='securities_master',
                 password='SMdatabase1'):
        self.host = host_name
        self.user = user_name
        self.database = database_name
        self.password = password

        self.cnx = self.openDatabaseConnection()

    def openDatabaseConnection(self):
        try:
            cnx = mysql.connector.connect(host=self.host,
                                          user=self.user,
                                          database=self.database,
                                          password=self.password)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            return cnx

    def insertIntoTable(self, table_name, column_str, new_values):
        insert_str = ("%s, " * len(column_str.split(',')))[:-2]
        final_str = "INSERT INTO %s (%s) VALUES (%s)" % (table_name, column_str, insert_str)

        # Using the MySQL connection, carry out an INSERT INTO for every symbol
        cur = self.cnx.cursor()
        # This line avoids the MySQL MAX_PACKET_SIZE
        # Although of course it could be set larger!
        if isinstance(new_values, tuple):
            cur.execute(final_str, new_values)
        else:
            for i in range(0, int(ceil(len(new_values) / 100.0))):
                cur.executemany(final_str, new_values[i * 100:(i + 1) * 100 - 1])

        self.cnx.commit()
        cur.close()

    def insertIgnoreIntoTable(self, table_name, column_str, new_values):
        insert_str = ("%s, " * len(column_str.split(',')))[:-2]
        final_str = "INSERT IGNORE INTO %s (%s) VALUES (%s)" % (table_name, column_str, insert_str)

        # Using the MySQL connection, carry out an INSERT INTO for every symbol
        cur = self.cnx.cursor()
        # This line avoids the MySQL MAX_PACKET_SIZE
        # Although of course it could be set larger!
        if isinstance(new_values, tuple):
            cur.execute(final_str, new_values)
        else:
            for i in range(0, int(ceil(len(new_values) / 100.0))):
                cur.executemany(final_str, new_values[i * 100:(i + 1) * 100 - 1])

        self.cnx.commit()
        cur.close()

    def selectFromTable(self, table_name, column_str='*',conditional_statement=None):
        cur = self.cnx.cursor()
        if conditional_statement!=None:
            final_str = "SELECT %s FROM %s WHERE %s" % (column_str, table_name, conditional_statement)
        else:
            final_str = "SELECT %s FROM %s" % (column_str, table_name)
        cur.execute(final_str)
        data = cur.fetchall()
        names = cur.column_names
        cur.close()
        output = []
        for d in data:
            output.append({n : d[idx] for idx,n in enumerate(names)})
        return output

    def getData(self,table_name,column_str='*',conditional_statement=None,joined_table_names=None):
        cur = self.cnx.cursor()
        if conditional_statement==None:
            if joined_table_names==None:
                str = "SELECT %s FROM %s" % (column_str,table_name)
            else:
                str = "SELECT %s FROM %s" % (column_str,table_name) + " " + ' '.join(["INNER JOIN %s ON %s" % (name_pair[0],name_pair[1]) for name_pair in joined_table_names])
        else:
            if joined_table_names == None:
                str = "SELECT %s FROM %s WHERE %s" % (column_str,table_name,conditional_statement)
            else:
                str = "SELECT %s FROM %s " % (column_str,table_name) + " " + ' '.join(
                    ["INNER JOIN %s ON %s" % (name_pair[0], name_pair[1]) for name_pair in joined_table_names])
                str = str + " WHERE %s" % conditional_statement
        cur.execute(str)
        data = cur.fetchall()
        names = cur.column_names
        cur.close()
        output = []
        for d in data:
            output.append({n: d[idx] for idx, n in enumerate(names)})

        output = pandas.DataFrame(output)
        return output

    # @data from list of dicts [{name:value,..}] where name is a column name in table
    def insertData(self,data,table_name):
        for d in data:
            keys = list(d.keys())
            values = tuple(d.values())
            column_str = ", ".join(keys)
            self.insertIgnoreIntoTable(table_name,column_str,values)

    def tableColumnNames(self,table_name):
        final_str = "DESCRIBE %s" % table_name
        cur = self.cnx.cursor()
        cur.execute(final_str)
        data = cur.fetchall()
        names = [d[0] for d in data]
        return names

# sub class symbol_SQL for access of symbols table in database
class Symbol_SQL(SQL_connection):
    # initialise super class object
    def __init__(self,host_name='localhost', user_name='tom', database_name='securities_master',
                 password='SMdatabase1'):
        super().__init__(host_name, user_name, database_name,password)
        self.table_name = 'symbol'

    def getTickers(self):
        column_str = "id, ticker"
        symbols = self.selectFromTable(self.table_name, column_str)
        return symbols

    def symbolID(self,ticker):
        data = self.selectFromTable(self.table_name, 'id, ticker', "ticker='%s'" % ticker)
        return data[0]

    def sqlEntry(self,ticker):
        data = self.selectFromTable(self.table_name,'*',"ticker='%s'" % ticker)
        return data[0]

    def symbolTicker(self,id):
        data = self.selectFromTable(self.table_name, 'id, ticker', "id='%s'" % id)
        return data[0]

# sub class exchange_SQL for access of exchange table in database
class Exchange_SQL(SQL_connection):
    # initialise super class object
    def __init__(self, host_name='localhost', user_name='tom', database_name='securities_master',
                 password='SMdatabase1'):
        super().__init__(host_name, user_name, database_name, password)
        self.table_name = 'exchange'

    def exchangeID(self,exchange_abbrev):
        data = self.selectFromTable(self.table_name,'id',"abbrev='%s'" % exchange_abbrev)
        return data[0]

    def exchangeAbbrev(self,id):
        data = self.selectFromTable(self.table_name, 'id, abbrev', "id='%s'" % id)
        return data[0]

# sub class data_vendor_SQL for access of data_vendors table in database
class Data_vendor_SQL(SQL_connection):
    # initialise super class object
    def __init__(self,host_name='localhost', user_name='tom', database_name='securities_master',
                 password='SMdatabase1'):
        super().__init__(host_name, user_name, database_name, password)
        self.table_name = 'data_vendor'

    def checkVendor(self,vendor_name):
        final_str = "SELECT EXISTS (SELECT 1 FROM %s WHERE name ='%s')" % (self.table_name,vendor_name)
        cur = self.cnx.cursor()
        cur.execute(final_str)
        data = cur.fetchall()[0]
        cur.close()
        return data[0]

    def newVendor(self,vendor_name,url=None,support_email=None,api_key=None):
        now = datetime.datetime.utcnow()
        column_str = "name, website_url, support_email, api_key, created_date, last_updated_date "
        values = (vendor_name, url, support_email, api_key, now, now)
        self.insertIgnoreIntoTable(self.table_name, column_str, values)

    def vendorID(self,vendor_name):
        data = self.selectFromTable(self.table_name,'id',"name='%s'" % vendor_name)
        return data[0]

    def sqlEntry(self,vendor_name):
        data = self.selectFromTable(self.table_name,'*',"name='%s'" % vendor_name)
        return data[0]

# sub class daily_price_SQL for access to daily_price table in database
class Daily_price_SQL(SQL_connection):
    def __init__(self,host_name='localhost', user_name='tom', database_name='securities_master',
                 password='SMdatabase1'):
        super().__init__(host_name, user_name, database_name, password)
        self.table_name = 'daily_price'

    def checkExisting(self,vendor_id,symbol_id):
        final_str = "SELECT EXISTS (SELECT 1 FROM %s WHERE symbol_id ='%s' AND data_vendor_id='%s')" % (self.table_name,symbol_id,vendor_id)
        cur = self.cnx.cursor()
        cur.execute(final_str)
        data = cur.fetchall()[0]
        cur.close()
        return data[0]

    def getAdjClosePrice(self,vendor_id,symbol_tickers,price_from = None):
        df = pandas.DataFrame()
        for idx, symbol_ticker in enumerate(symbol_tickers):
            column_str = 'daily_price.price_date, daily_price.adj_close_price'
            if price_from==None:
                conditional_statement = "symbol.ticker = '%s' AND data_vendor.id = %s" % (symbol_ticker,vendor_id)
            else:
                conditional_statement = "symbol.ticker = '%s' AND data_vendor.id = %s AND daily_price.price_date > '%s'" % (symbol_ticker, vendor_id,price_from)
            joined_table_names = [['symbol', 'daily_price.symbol_id = symbol.id'],
                                  ['data_vendor', 'daily_price.data_vendor_id = data_vendor.id']]
            data = self.getData(self.table_name,column_str,conditional_statement,joined_table_names)
            data = data.set_index('price_date')
            data = data.rename(index=str,columns={list(data)[0]:symbol_ticker})
            if idx==0:
                df = data
            else:
                df[symbol_ticker] = data
        return df






