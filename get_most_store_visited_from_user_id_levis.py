import pandas as pd
from django.db.models import Count
from collections import defaultdict
from django.db.models import Count, IntegerField, Case, When


df = pd.read_csv('/home/bintang/ID_EOSS_H223_eDM.csv')
column_name = 'User_ID'

uids = df[column_name].unique()

user_ids = User.objects.filter(id__in=uids)\
                       .values('id','email','phone','has_incorrect_email','has_incorrect_phone',\
                               'profile__gender')

transactions = Transaction.objects.filter(user_id__in=uids)\
                                  .exclude(status=Transaction.STATUS.canceled)\

transactions_with_store = transactions.values('user_id', 'store__display_name')\
                                      .annotate(trx_count=Count('id'))\
                                      .order_by('user_id', '-trx_count', 'store__display_name')

def store_priority(name):
    if name.startswith('OLS'):
        return 1
    elif name.startswith('SOGO') or name.startswith('SIS'):
        return 2
    elif name.startswith('MDS') or name.startswith('MSI'):
        return 3
    elif name.startswith('Levi.co.id'):
        return 4
    else:
        return 5

# Get transactions with required fields and annotate with store count and priority
transactions_with_store = transactions.values('user_id', 'store__display_name')\
                                      .annotate(
                                          trx_count=Count('id'),
                                          priority=Case(
                                              When(store__display_name__startswith='OLS', then=1),
                                              When(store__display_name__startswith='SOGO', then=2),
                                              When(store__display_name__startswith='SIS', then=2),
                                              When(store__display_name__startswith='MDS', then=3),
                                              When(store__display_name__startswith='MSI', then=3),
                                              When(store__display_name__startswith='Levi.co.id', then=4),
                                              default=5,
                                              output_field=IntegerField()))\
                                      .order_by('user_id', '-trx_count', 'priority', 'store__display_name')

# Initialize a dictionary to store the user's most frequented store and the transaction count
user_most_frequented_store = {}

# Initialize previous user_id as None
prev_user_id = None

# Iterate over the annotated transactions
for transaction in transactions_with_store:
    # If user_id is not the same as the previous one, then this is the highest count store for this user_id
    # because the results are ordered by user_id, then by count descending, then by priority, and then by store name
    if transaction['user_id'] != prev_user_id:
        user_most_frequented_store[transaction['user_id']] = (transaction['store__display_name'], transaction['trx_count'])
        prev_user_id = transaction['user_id']

# Convert dictionary to dataframe
most_frequented_store_df = pd.DataFrame.from_dict(user_most_frequented_store, orient='index', columns=['Store', 'Count'])
most_frequented_store_df.reset_index(level=0, inplace=True)
most_frequented_store_df = most_frequented_store_df.rename(columns = {'index':'User_ID'})

# Convert QuerySets to dataframes
transactions_df = pd.DataFrame.from_records(transactions.values())
user_ids_df = pd.DataFrame.from_records(user_ids)

# Merge the dataframes
df = df.merge(user_ids_df, how='left', left_on='User_ID', right_on='id')
df = df.merge(most_frequented_store_df, how='left', on='User_ID')


df.to_csv('/home/bintang/ID_EOSS_H223_eDM_with_most_store.csv', index=False)
