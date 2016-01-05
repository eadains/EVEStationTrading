from sqlalchemy import create_engine
from sql_stuff.data_update import update_item_list, update_price_data

engine = create_engine('mysql+mysqlconnector://root:Erikqazwsx12@@localhost/EveMarketData')
# update_item_list(engine)
update_price_data(engine)