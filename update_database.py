from sqlalchemy import create_engine
from sql_stuff.data_update import update_item_list, update_price_data
import pandas as pd
from functools import partial
from multiprocessing.pool import Pool
from config.config import SQL_USER, SQL_PASS


def main():
    SQL_DATABASE = 'EveMarketData'
    update_item_list(SQL_USER, SQL_PASS, SQL_DATABASE)
    engine = create_engine('mysql+mysqlconnector://%s:%s@localhost/%s' % (SQL_USER, SQL_PASS, SQL_DATABASE))
    region_id = 10000002
    item_id_list = [int(index) for (index, row) in pd.read_sql_table('items', engine, index_col='item_id').iterrows()]
    data_write = partial(update_price_data, SQL_USER, SQL_PASS, SQL_DATABASE, region_id)
    p = Pool(4)
    p.map(data_write, item_id_list, chunksize=5)

if __name__ == '__main__':
    main()
