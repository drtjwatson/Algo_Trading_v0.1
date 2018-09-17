# event.py event class


class Event(object):
    """
    Event is base class providing an interface for all subsequent
    (inherited) events, that will trigger further events in the
    trading infrastructure.
    """
    pass


class MarketEvent(Event):
    """
    Handles the event of receiving a new market update with
    corresponding bars.

    This is triggered when the outer while loop begins a new "heartbeat".
     It occurs when the DataHandler object receives a new update of market
     data for any symbols which are currently being tracked. It is used to
     trigger the Strategy object generating new trading signals. The event
     object simply contains an identification that it is a market event, with no other structure.
    """
    def __init__(self):
        self.type = 'MARKET'


class SignalEvent(Event):
    """
    Handles the event of sending a Signal from a Strategy object.
    This is received by a Portfolio object and acted upon.

    The Strategy object utilises market data to create new SignalEvents.
    The SignalEvent contains a ticker symbol, a timestamp for when it was
    generated and a direction (long or short). The SignalEvents are utilised
     by the Portfolio object as advice for how to trade.
    """
    def __init__(self,symbol,datetime,signal_type):
        """
        Initialises the SignalEvent.

        Parameters:
        symbol id - the id of the signal object in from of dict {'id':val,'ticker'}
        datetime - The timestamp at which the signal was generated.
        signal_type - 'LONG' or 'SHORT'.
        """
        self.type = 'SIGNAL'
        self.symbol = symbol
        self.datetime = datetime
        self.signal_type = signal_type


class OrderEvent(Event):
    """
    Handles the event of sending an Order to an execution system.
    The order contains a symbol (e.g. GOOG), a type (market or limit),
    quantity and a direction.

    When a Portfolio object receives SignalEvents it assesses them in
    the wider context of the portfolio, in terms of risk and position
    sizing. This ultimately leads to OrderEvents that will be sent to
    an ExecutionHandler.
    """
    def __init__(self,symbol,order_type,quantity,direction):
        """
        Initialises the order type, setting whether it is
        a Market order ('MKT') or Limit order ('LMT'), has
        a quantity (integral) and its direction ('BUY' or
        'SELL').

        Parameters:
        symbol - The instrument to trade from the symbols SQL database in from of dict {'id':val,'ticker'}
        order_type - 'MKT' or 'LMT' for Market or Limit.
        quantity - Non-negative integer for quantity.
        direction - 'BUY' or 'SELL' for long or short.
        """
        self.type = 'ORDER'
        self.symbol = symbol
        self.order_type = order_type
        self.quantity = quantity
        self.direction = direction

    def print_order(self):
        """
        Outputs the values within the Order.
        """
        print("Order: Ticker=%s, Type=%s, Quantity=%s, Direction=%s" %
            (self.symbol['ticker'], self.order_type, self.quantity, self.direction))


class FillEvent(Event):
    """
    Encapsulates the notion of a Filled Order, as returned
    from a brokerage. Stores the quantity of an instrument
    actually filled and at what price. In addition, stores
    the commission of the trade from the brokerage.

    When an ExecutionHandler receives an OrderEvent it must
     transact the order. Once an order has been transacted it
     generates a FillEvent, which describes the cost of
     purchase or sale as well as the transaction costs, such
     as fees or slippage.
    """
    def __init__(self, timeindex, symbol, exchange, quantity,
                 direction, fill_cost, commission=None):
        """
        Initialises the FillEvent object. Sets the symbol, exchange,
        quantity, direction, cost of fill and an optional
        commission.

        If commission is not provided, the Fill object will
        calculate it based on the trade size and Interactive
        Brokers fees.

        Parameters:
        timeindex - The bar-resolution when the order was filled.
        symbol - The instrument which was filled in form of dict {'id':val,'ticker':val}
        exchange - The exchange where the order was filled if form of dict {'id':val,'abbrev',val}
        quantity - The filled quantity.
        direction - The direction of fill ('BUY' or 'SELL')
        fill_cost - The holdings value in dollars.
        commission - An optional commission
        """

        self.type = 'FILL'
        self.timeindex = timeindex
        self.symbol = symbol
        self.exchange = exchange
        self.quantity = quantity
        self.direction = direction
        self.fill_cost = fill_cost

        # Calculate commission
        if commission is None:
            self.commission = self.calculate_ib_commission()
        else:
            self.commission = commission

    def calculate_ib_commission(self):
        """
        Calculates the fees of trading based on an Interactive
        Brokers fee structure for API, in USD.

        This does not include exchange or ECN fees.

        Based on "US API Directed Orders":
        https://www.interactivebrokers.com/en/index.php?f=commission&p=stocks2
        """
        full_cost = 1.3
        if self.quantity <= 500:
            full_cost = max(1.3, 0.013 * self.quantity)
        else:  # Greater than 500
            full_cost = max(1.3, 0.008 * self.quantity)
        full_cost = min(full_cost, 0.5 / 100.0 * self.quantity * self.fill_cost)
        return full_cost
