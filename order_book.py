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
    sender_pk = order['sender_pk'] # sender's public key
    receiver_pk = order['receiver_pk'] # order’s originator controlled key

    # Generate new order.
    new_order = Order(sender_pk=sender_pk, receiver_pk=receiver_pk, buy_currency=buy_currency, sell_currency=sell_currency, buy_amount=buy_amount, sell_amount=sell_amount)
    session.add(new_order) # add new order into datebase
    session.commit # commit the datebase

    # Check if there are any existing orders that match the new order. FIFO is applied here.
    existing_order = session.query(Order).filter(Order.filled == None, Order.buy_currency == new_order.sell_currency, Order.sell_currency == new_order.buy_currency, ((Order.sell_amount / Order.buy_amount ) >= (new_order.buy_amount / new_order.sell_amount))).first()

    # Match orders, if the existing match is found.
    if existing_order != None:

        # Set the filled field to be the current timestamp on both orders
        new_order.filled = datetime.now()
        existing_order.filled = datetime.now()

        # Set counterparty_id to be the id of the other order
        new_order.counterparty_id = existing_order.id
        existing_order.counterparty_id = new_order.id

        # If one of the orders is not completely filled (i.e. the counterparty’s sell_amount is less than buy_amount):

        if existing_order.sell_amount < new_order.buy_amount or new_order.buy_amount < existing_order.sell_amount:
            
            # new_order is the parent for the create_order
            if existing_order.sell_amount < new_order.buy_amount:
                
                # id from parent order
                creator_id = new_order.id 
                
                # pk & platform from parent order
                sender_pk = new_order.sender_pk
                receiver_pk = new_order.receiver_pk
                buy_currency = new_order.buy_currency
                sell_currency = new_order.sell_currency

                # buy amount is the difference between the large order and small order
                buy_amount = new_order.buy_amount - existing_order.sell_amount

                # sell amount implies exchange rate of the new order is at least that of the old order
                exchange_rate = new_order.buy_amount / new_order.sell_amount
                sell_amount = buy_amount / exchange_rate
                
            # existing_order is the parent for the create_order
            else:
                # id from parent order
                creator_id = existing_order.id

                # pk & platform from parent order
                sender_pk = existing_order.sender_pk
                receiver_pk = existing_order.receiver_pk
                buy_currency = existing_order.buy_currency
                sell_currency = existing_order.sell_currency

                # sell amount is the difference between the large order and small order
                sell_amount = existing_order.sell_amount - new_order.buy_amount

                # buy amount implies exchange rate of the new order is at least that of the old order
                exchange_rate = existing_order.sell_amount / existing_order.buy_amount
                buy_amount = sell_amount / exchange_rate
                

            # Create a new order for remaining balance
            create_order = Order(sender_pk=sender_pk, receiver_pk=receiver_pk, buy_currency=buy_currency, sell_currency=sell_currency, buy_amount=buy_amount, sell_amount=sell_amount, creator_id = creator_id)

            session.add(create_order)
            session.commit()
            process_order(create_order)
                
        else:
            session.commit()

    pass