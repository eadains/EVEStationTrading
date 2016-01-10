import pandas as pd
from sqlalchemy import create_engine


def calc_scores(SQL_USER, SQL_PASS, SQL_DATABASE, item_id):
    """
    Calculates profitability scores for specified item_id. Returns Pandas dataframe containing item_id and
    its score. The profit factor is calculated with (Volume * day low) * Day Profit margin
    SQL_USER = Database username
    SQL_PASS = Database account pass
    SQL_DATABASE = name of database to use
    """
    engine = create_engine('mysql+mysqlconnector://%s:%s@localhost/%s' % (SQL_USER, SQL_PASS, SQL_DATABASE))
    frame = pd.read_sql('SELECT FROM prices WHERE item_id=%d' % item_id, engine,
                        index_col='date', parse_dates=['date'])
    frame['profit_factor'] = frame['volume'] * frame['low'] * ((frame['high'] - frame['low']) / frame['low'])
    return pd.DataFrame({'item_id': [frame['item_id'][0]], 'profit_factor': [frame['profit_factor'].mean()]})
