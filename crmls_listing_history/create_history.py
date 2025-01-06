import pandas as pd
import datetime as dt

event_codes = { 
                'C': 50,
                'A': 100,
                'B': 200,
                'U': 240,
                'P': 240,
                'X': 300,
                'S': 500,
                'H': 640,
                'K': 620,
                'W': 600,
                'Z': 600
              }

event_codes = {
    'Coming Soon': 50,
    'Active': 100,
    'Contingent': 200,
    'Active Under Contract': 240,
    'Pending': 240,
    'Expired': 300,
    "Incomplete": 999,
    'Hold': 640,
    'Closed': 500,
    "Withdrawn": 600,
    "Canceled": 620,
    "Delete": 600
}

def coalesce(*values):
    """Return the first non-None value or None if all values are None"""
    return next((v for v in values if pd.notna(v)), None)

def create_pairs(close_date, expiration_date, listing_contract_date,
                 purchase_contract_date, list_price, contingent_date, 
                 original_list_price, close_price):
        """Based on which variables are present/not null, create events from date-price pairs

        Returns:
            list: list of events
        """
        pairs = []
        '''
        when input data is in csv form, all the datatypes are strings unless they are blank
        if type(listing_contract_date) == str:
            pairs.append((100, listing_contract_date, coalesce(original_list_price, list_price)))
        if type(contingent_date) == str:
            pairs.append((200, contingent_date, coalesce(list_price, original_list_price)))
        if type(purchase_contract_date) == str:
            pairs.append((240, purchase_contract_date, coalesce(list_price, original_list_price)))
        if type(expiration_date) == str:
            pairs.append((300, expiration_date, list_price))
        if type(close_date) == str:
            pairs.append((500, close_date, coalesce(close_price, list_price)))
        '''
        if not pd.isna(listing_contract_date):
            pairs.append((100, listing_contract_date, coalesce(original_list_price, list_price)))
        if not pd.isna(contingent_date):
            pairs.append((200, contingent_date, coalesce(list_price, original_list_price)))
        if not pd.isna(purchase_contract_date):
            pairs.append((240, purchase_contract_date, coalesce(list_price, original_list_price)))
        if not pd.isna(expiration_date):
            pairs.append((300, expiration_date, list_price))
        if not pd.isna(close_date):
            pairs.append((500, close_date, coalesce(close_price, list_price)))
        pairs.sort(key=lambda x: x[1])
        return pairs

def clean_events(events: list) -> list:
    """Remove out of order events (i.e. all status codes should be in
       ascending order)

    Args:
        events (list): list of events in order of date

    Returns:
        list: list of cleaned events
    """
    index = 1
    while index <= len(events):
        try:
            if events[index] < events[index - 1]:
                del events[index]
            else:
                index += 1
        except IndexError:
            break
    return events

def clean_expiry(events: list, status: str) -> list:
    """Remove expiry event if status is not expired

    Args:
        events (list): list of events
        status (str): status of listing

    Returns:
        list: list of events with expiry event removed if necessary
    """
    if status != 'Expired':
        t = None
        for i in range(len(events)):
            if 300 in events[i]:
                t = i
        if t:
            events.pop(t)
    return events

def add_withdraw_cancel(events, status, withdraw_date, expiration_date, list_price, status_change_timestamp) -> list:
    """Add withdraw, cancel, and hold events

    Returns:
        list: list of events with cancel, withdraw, and hold events added
    """
    '''
    if type(withdraw_date) == str and status != 640:
        events.append((status, withdraw_date, list_price))
    elif type(status_change_timestamp) == str:
        events.append((status, status_change_timestamp, list_price))
    else:
        events.append((status, expiration_date, list_price))
    '''
    if withdraw_date and status != 640:
        events.append((status, withdraw_date, list_price))
    elif status_change_timestamp:
        events.append((status, status_change_timestamp, list_price))
    else:
        events.append((status, expiration_date, list_price))
    return events

def create_listing_history(close_date, expiration_date, listing_contract_date, purchase_contract_date, withdrawn_date,
                           list_price,contingent_date, original_list_price, close_price, status, status_change_timestamp) -> list:
    """Given the dates and prices available, generate all events

    Returns:
        list: list of ordered and cleaned events that a listing went through
    """
    ec = event_codes[status]
    events = []
    if ec == 50:
        events.append(f'{ec}: {listing_contract_date}, {coalesce(original_list_price, list_price)}')
    else:
        events = create_pairs(close_date, expiration_date, listing_contract_date,
                              purchase_contract_date, list_price,contingent_date, 
                              original_list_price, close_price)
        if ec in (600, 620, 640):
            events = add_withdraw_cancel(events, ec, withdrawn_date, expiration_date, list_price, status_change_timestamp)
        events = clean_events(events)
        events = clean_expiry(events, status)
    return events

def transform_events(events):
    status = []
    d = []
    price = []
    for event in events:
        status.append(str(event[0]))
        if type(event[1]) == dt.date:
            d.append(str(event[1]) + ' 00:00:00')
        else:
            d.append(str(event[1]))
        price.append(str(event[2]))
    return status, d, price

# TODO: ADD COMING_SOON_TIMESTAMP
def create_listings(listings_sample):
    events = [create_listing_history(close_date, expiration_date, listing_contract_date, purchase_contract_date, withdrawn_date, 
                        list_price,contingent_date, original_list_price, close_price, status, status_change_timestamp) for close_date, expiration_date, listing_contract_date, purchase_contract_date, withdrawn_date, list_price,contingent_date, original_list_price, close_price, status, status_change_timestamp in zip(listings_sample.close_date, listings_sample.expiration_date, listings_sample.listing_contract_date, listings_sample.purchase_contract_date, listings_sample.withdrawn_date,listings_sample.list_price,listings_sample.contingent_date, listings_sample.original_list_price, listings_sample.close_price, listings_sample.standard_status, listings_sample.status_change_timestamp)]
    
    '''
    events = []
    for close_date, expiration_date, listing_contract_date, purchase_contract_date, withdrawn_date, list_price,contingent_date, original_list_price, close_price, status, status_change_timestamp in zip(listings_sample.close_date, listings_sample.expiration_date, listings_sample.listing_contract_date, listings_sample.purchase_contract_date, listings_sample.withdrawn_date,listings_sample.list_price,listings_sample.contingent_date, listings_sample.original_list_price, listings_sample.close_price, listings_sample.standard_status, listings_sample.status_change_timestamp):
        events.append(create_listing_history(close_date, expiration_date, listing_contract_date, purchase_contract_date, withdrawn_date, 
                        list_price,contingent_date, original_list_price, close_price, status, status_change_timestamp))
    '''
    listings_sample['events'] = events
    return listings_sample

def transform_histories(pw_df):
    lh = {'id': [], 'status_code': [], 'price': [], 'time': []}
    tmp = create_listings(pw_df)[['id', 'standard_status','events']]
    for i, e in zip(tmp['id'], tmp['events']):
        s, d, p = transform_histories(e)
        lh['status_code'] += s
        lh['price'] += p
        lh['time'] += d
        lh['id'] += [i] * len(s)
    
    return pd.DataFrame(lh)#.to_csv(f'data/crmls_lh_backfill/crmls_backfill_{j}.csv', index=False)

if __name__ == '__main__':
    '''
    listings_sample = pd.read_csv('data/crmls_listings_sample1.csv')
    for i in range(2,13):
        listings_sample = pd.concat([listings_sample, pd.read_csv(f'data/crmls_listings_sample{i}.csv')])
    ret_df = create_listings(listings_sample)
    ret_df.to_csv('data/listings_with_histories.csv')
    '''
    listings_sample = pd.read_csv('data/crmls-demo-sample.csv')
    transform_histories(listings_sample).to_csv('data/listings_with_histories_demo.csv')