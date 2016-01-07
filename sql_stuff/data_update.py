import pandas as pd
import requests
from sqlalchemy import create_engine


def update_item_list(SQL_USER, SQL_PASS, SQL_DATABASE):
    """
    Fetches all marketable items from API, drops any info currently in table, then inserts API data.
    SQL_USER = Username for SQL database
    SQL_PASS = Associated password for SQL account
    SQL_DATABASE = Database to use
    """
    engine = create_engine('mysql+mysqlconnector://%s:%s@localhost/%s' % (SQL_USER, SQL_PASS, SQL_DATABASE))
    url = 'https://public-crest.eveonline.com/market/types/'
    page_count = requests.get(url).json()[u'pageCount']
    fetched_data_frame = pd.DataFrame()
    for x in range(1, page_count+1):
        json_request = requests.get(url, params={'page': x}).json()
        json_frame = pd.io.json.json_normalize(json_request[u'items']).drop(['marketGroup.href', 'marketGroup.id',
                                                                             'marketGroup.id_str', 'type.href',
                                                                             'type.icon.href', 'type.id_str'], axis=1)
        json_frame.columns = ['item_id', 'name']
        fetched_data_frame = fetched_data_frame.append(json_frame)
        print "Page %s added." % x
    engine.execute('DELETE FROM items')
    fetched_data_frame.to_sql('items', engine, if_exists='append', index=False, index_label='item_id')
    print "Finished."


def update_price_data(SQL_USER, SQL_PASS, SQL_DATABASE, region_id, item_id):
    """
    Fetches any new price data for given item ID in given region.
    item_id = item id of item you want to fetch price data for
    region_id = region id of region you want price data from
    SQL_USER = Username to use for database
    SQL_PASS = associated password to use for database
    SQL_DATABASE = name of database to use
    """
    engine = create_engine('mysql+mysqlconnector://%s:%s@localhost/%s' % (SQL_USER, SQL_PASS, SQL_DATABASE))
    most_recent_date = engine.execute('SELECT MAX(date) FROM prices WHERE item_id=%d' % item_id).fetchone()[0]
    for attempt in range(10):
        try:
            json_request = requests.get('https://public-crest.eveonline.com/market/%d/types/%d/history/' %
                                        (region_id, item_id)).json()
        except requests.exceptions.RequestException:
            continue
        break
    try:
        json_frame = pd.io.json.json_normalize(json_request[u'items']).drop(['avgPrice', 'orderCount_str',
                                                                             'volume_str'], axis=1)
    except IndexError:
        return
    json_frame['item_id'] = item_id
    json_frame.columns = ['date', 'high', 'low', 'orders', 'volume', 'item_id']
    try:
        json_frame = json_frame[json_frame['date'] > most_recent_date.strftime('%Y-%m-%d')]
        json_frame.to_sql('prices', engine, if_exists='append', index=False, index_label='item_id')
    except AttributeError:
        json_frame.to_sql('prices', engine, if_exists='append', index=False, index_label='item_id')
