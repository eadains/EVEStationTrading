import pandas as pd
import requests


def update_item_list(sql_engine):
    """
    Fetches all marketable items from API, drops any info currently in table, then inserts API data.
    sql_engine = SQLAlchemy create_engine object
    """
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
    sql_engine.execute('DELETE FROM items')
    fetched_data_frame.to_sql('items', sql_engine, if_exists='append', index=False, index_label='item_id')
    print "Finished."


def update_price_data(sql_engine):
    """
    Fetches price data for all items in items table, and appends only new price data.
    sql_engine = SQLAlchemy create_engine object
    """
    item_frame = pd.read_sql_table('items', sql_engine, index_col='item_id')
    for index, row in item_frame.iterrows():
        most_recent_date = sql_engine.execute('SELECT MAX(date) FROM prices WHERE item_id=%d' % index).fetchone()[0]
        for attempt in range(10):
            try:
                json_request = requests.get('https://public-crest.eveonline.com/market/10000002/types/%d/history/' % index).json()
            except requests.exceptions.RequestException:
                continue
            break
        json_frame = pd.io.json.json_normalize(json_request[u'items']).drop(['avgPrice', 'orderCount_str',
                                                                             'volume_str'], axis=1)
        json_frame['item_id'] = index
        json_frame.columns = ['date', 'high', 'low', 'orders', 'volume', 'item_id']
        try:
            json_frame = json_frame[json_frame['date'] < most_recent_date.strftime('%y-%m-%d')]
            json_frame.to_sql('prices', sql_engine, if_exists='append', index=False, index_label='item_id')
        except AttributeError:
            json_frame.to_sql('prices', sql_engine, if_exists='append', index=False, index_label='item_id')
        print "Item %s done." % row
    print "Finished."
