from sqlalchemy import create_engine
import pandas as pd
from config import config
import requests

sqlEngine = create_engine('mysql+mysqlconnector://%s:%s@localhost/EveMarketData' % (config.SQL_USER, config.SQL_PASS))
itemNamesFrame = pd.read_sql_table('items', sqlEngine, index_col='item_id')
for index, row in itemNamesFrame.iterrows():
    jsonRequest = requests.get('https://public-crest.eveonline.com/market/10000002/types/%d/history/' % index).json()
    jsonFrame = pd.io.json.json_normalize(jsonRequest[u'items']).drop(['avgPrice', 'orderCount_str',
                                                                       'volume_str'], axis=1)
    jsonFrame['item_id'] = index
    jsonFrame.columns = ['date', 'high', 'low', 'orders', 'volume', 'item_id']
    jsonFrame.to_sql('prices', sqlEngine, if_exists='append', index=False)