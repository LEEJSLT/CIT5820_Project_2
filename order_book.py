from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

def process_order(order):

    # Insert the order into the database load data from order datebase.
    buy_currency = order['buy_currency']
    sell_currency = order['sell_currency']
    buy_amount = order['buy_amount']
    sell_amount = order['sell_amount']
    sender_pk = order['sender_pk']
    receiver_pk = order['receiver_pk']

    # Generate the new order.
    new_order = Order(sender_pk=sender_pk, receiver_pk=receiver_pk, buy_currency=buy_currency, sell_currency=sell_currency, buy_amount=buy_amount, sell_amount=sell_amount)
    session.add(new_order) # add new order into datebase
    session.commit # commit the datebase

    # Check if there are any existing orders that match the new order.
    existing_order = session.query(Order).filter(Order.filled == None, Order.buy_currency == new_order.sell_currency, Order.sell_currency == new_order.buy_currency, ((Order.sell_amount / Order.buy_amount ) >= (new_order.buy_amount / new_order.sell_amount))).first()

    # Match orders
    # Set the filled field to be the current timestamp on both orders
    new_order.filled = datetime.now()
    existing_order.filled = datetime.now()








    pass