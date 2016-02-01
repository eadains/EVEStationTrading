import pandas as pd
from sqlalchemy import create_engine


def init_function(SQL_USER, SQL_PASS, SQL_DATABASE):
    """
    Sets global engine variable for use in the calc scores function. Should be used as intializer function in
    multiprocessing.
    """
    global engine
    engine = create_engine('mysql+mysqlconnector://%s:%s@localhost/%s' % (SQL_USER, SQL_PASS, SQL_DATABASE))


def calc_scores(wallet, broker_fee, sales_tax, item_id):
    """
    Calculates profitability scores for specified item_id. Returns Pandas dataframe containing item_id and
    its score. The profit factor is calculated with
    (Volume * low) * Margin * Number of days where items were traded * cost proportional to wallet
    SQL_USER = Database username
    SQL_PASS = Database account pass
    SQL_DATABASE = name of database to use
    wallet = isk in wallet
    broker_fee = broker fee
    sales_tax = sales tax
    """
    # TODO: Fetch wallet automatically
    # TODO: Fetch expenses automatically
    # TODO: Fetch Margin level automatically
    frame = pd.read_sql('SELECT * FROM prices WHERE item_id=%d' % item_id, engine,
                        index_col='date', parse_dates=['date'])
    frame['high'] = frame['high'] - (frame['high'] * broker_fee * sales_tax)
    frame['low'] = frame['low'] + (frame['low'] * broker_fee)
    history_weight = len(frame.index)
    frame['profit_factor'] = (frame['volume'] * frame['low']) *\
                             ((frame['high'] - frame['low']) / frame['low']) * history_weight * (wallet / frame['low'])
    try:
        return pd.DataFrame({'item_id': [frame['item_id'][0]], 'profit_factor': [frame['profit_factor'].mean()]})
    except IndexError:
        print 'Item %d has no stored market data. Skipping.' % item_id


def calc_portfolio(weight_frame, margin_percent):
    """
    Calculates how many of each item to buy, using the dataframe created by calc_scores. Returns dataframe with item id,
    name of item, escrow amount, and number to buy.
    """
