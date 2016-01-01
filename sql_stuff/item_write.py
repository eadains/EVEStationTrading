import pandas as pd
import requests
from sqlalchemy import create_engine

from config import config

url = 'https://public-crest.eveonline.com/market/types/'
pageCount = requests.get(url).json()[u'pageCount']
sqlEngine = create_engine('mysql+mysqlconnector://%s:%s@localhost/EveMarketData' % (config.SQL_USER, config.SQL_PASS))
for x in range(1, pageCount+1):
    jsonRequest = requests.get(url, params={'page': x}).json()
    jsonFrame = pd.io.json.json_normalize(jsonRequest[u'items']).drop(['marketGroup.href', 'marketGroup.id',
                                                                       'marketGroup.id_str', 'type.href',
                                                                       'type.icon.href', 'type.id_str'], axis=1)
    jsonFrame.columns = ['item_id', 'name']
    jsonFrame.to_sql('items', sqlEngine, if_exists='append', index=False)
    print "%s done" % x
