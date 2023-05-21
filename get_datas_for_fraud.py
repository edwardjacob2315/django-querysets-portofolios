from datetime import date
from stamps.core.utils import prepare_datetime_range
import csv
import pandas as pd

# filter_date = date(2023, 2, 27)
filter_date_end = date(2023, 5, 16)

# filter_date, filter_date_end = prepare_datetime_range(filter_date, filter_date_end)

redemption_cancel = Redemption.objects.filter(created__date__lte=filter_date_end)\
                                .filter(status=Redemption.STATUS.canceled)\
                                .values('user_id').annotate(Count('id'))\
                                .order_by()

df_redemption_cancel = pd.DataFrame(redemption_cancel)
df_redemption_cancel.to_csv('/home/bintang/canceled-redeem-data-2023-5-15.csv')



from datetime import date
from stamps.core.utils import prepare_datetime_range
import csv
import pandas as pd

# filter_date = date(2023, 2, 27)
filter_date_end = date(2023, 5, 16)

# filter_date, filter_date_end = prepare_datetime_range(filter_date, filter_date_end)

trx_cancel = Transaction.objects.filter(created__date__lte=filter_date_end)\
                               .filter(status=Transaction.STATUS.canceled)\
                               .values('user_id').annotate(Count('id'))\
                               .order_by()

df_trx_cancel = pd.DataFrame(trx_cancel)
df_trx_cancel.to_csv('/home/bintang/canceled-trx-data-2023-5-15.csv')



from datetime import date
from stamps.core.utils import prepare_datetime_range
import csv
import pandas as pd

filter_date = date(2023, 2, 21)
filter_date_end = date(2023, 5, 15)

filter_date , filter_date_end = prepare_datetime_range(filter_date, filter_date_end)

redemption = Redemption.objects.filter(created__range=prepare_datetime_range(filter_date, filter_date_end))\
                               .exclude(status=Redemption.STATUS.canceled)\
                               .values('id', 'user_id', 'merchant_id', 'store_id', 'reward_id', 'voucher_id', 'voucher_template_id','promotion_id', 'qty',\
                                       'stamps', 'bonus_stamps','created','modified','merchant_invoice_number','status','extra_data','input_method','monetary_value',\
                                        'notes', 'channel','transaction_id','order_type')

df_redeem = pd.DataFrame(redemption)
df_redeem.to_csv('/home/bintang/redeem-data-2023-5-15.csv')



from datetime import date
from stamps.core.utils import prepare_datetime_range
import csv
import pandas as pd

filter_date = date(2023, 2, 21)
filter_date_end = date(2023, 5, 15)

filter_date , filter_date_end = prepare_datetime_range(filter_date, filter_date_end)

merchant = Merchant.objects.get(id=2)
users = User.objects.filter(date_joined__range=prepare_datetime_range(filter_date, filter_date_end)).select_related('profile')\
                    .values('id', 'has_incorrect_email', 'has_incorrect_phone', 'name', 'email', 'is_active','profile__gender','profile__birthday__year','memberships__redemption_allowed')

df = pd.DataFrame(users)

df.to_csv('/home/bintang/user-data-2023-5-15.csv')



import csv
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from stamps.core.utils import prepare_datetime_range
from django.core.exceptions import ObjectDoesNotExist as RelatedObjectDoesNotExist


end_date = date(2023, 5, 2)

merchant = Merchant.objects.get(slug='levis-indonesia')

retailers = ['AMP', 'LAU', 'KCS', 'SCU', 'MMR', 'MDS', 'PL', 'Ecommerce', 'Digital', 'LES','others']

transaction_stamps = merchant.transactions.exclude(user_id=None)\
                             .exclude(status=Transaction.STATUS.canceled)\
                             .filter(created__date__lte=end_date)\
                             .select_related('meta')\
                             .prefetch_related('items','items__product')\
                             .only('id', 'user_id', 'store','created', 'value','meta')


trx_annotate = transaction_stamps.values('id').annotate(
    item_quantity=Sum('items__quantity'),\
    product_count=Count('items__product_id', distinct=True))

storetagvalue = set(StoreTagValue.objects.filter(group__merchant=merchant)\
                                     .filter(group__name='Retailer')\
                                     .filter(name__in=retailers)\
                                     .values_list('stores', flat=True))



store_tag_value_dict = {}
for store in Store.objects.filter(id__in=storetagvalue):
    tag_value_names = store.tag_values.values_list('name', flat=True)
    if store.area:
        area_name = store.area.name
    else:
        area_name = None
    store_tag_value_dict[store.id] = {'retailer': list(tag_value_names), 'area_name': area_name}

for key, value in store_tag_value_dict.items():
    shortest_retailer = min(value['retailer'], key=len)
    store_tag_value_dict[key]['retailer'] = shortest_retailer



data_list = []

batch_size = 100000
total_count = transaction_stamps.count()

# print('you are here')
for i in range(0, total_count, batch_size):
    # print('udah masuk')
    transactions_filtered = transaction_stamps[i:i+batch_size]
    print(i+batch_size)

    for transaction in transactions_filtered:
        # print(transaction.id)
        data_dict = {}
        data_dict['id'] = transaction.id
        data_dict['user_id'] = transaction.user_id
        data_dict['store_id'] = transaction.store_id
        data_dict['created'] = transaction.created
        data_dict['created_year'] = transaction.created.year
        data_dict['created_month'] = transaction.created.month
        data_dict['created_day'] = transaction.created.day
        data_dict['value'] = transaction.value

        # print('meta')
        try:
            meta = transaction.meta
            # print('meta ada')
        except RelatedObjectDoesNotExist:
            meta = None
        #     print('RelatedObjectDoesNotExist')
        # print('lewat')

        if meta is not None and transaction.meta.employee_id is not None:
            data_dict['employee_count'] = 1
        else:
            data_dict['employee_count'] = 0

        data_dict['product_count'] = trx_annotate.get(id=transaction.id)['product_count']
        data_dict['item_quantity'] = trx_annotate.get(id=transaction.id)['item_quantity']


        # print(transaction.store_id)
        # data_dict['retailer'] = store_tag_value_dict.get(transaction.store_id)['retailer']
        data_dict['retailer'] = store_tag_value_dict.get(transaction.store_id, {}).get('retailer', None)
        data_dict['area_name'] = store_tag_value_dict.get(transaction.store_id, {}).get('area_name', None)

        # Append the dictionary to the list
        data_list.append(data_dict)

# Convert the list of dictionaries to a DataFrame
df = pd.DataFrame(data_list)

import csv
from datetime import date
from dateutil.relativedelta import relativedelta
from stamps.core.utils import prepare_datetime_range
from django.core.exceptions import ObjectDoesNotExist as RelatedObjectDoesNotExist


end_date = date(2023, 5, 2)

merchant = Merchant.objects.get(slug='levis-indonesia')

retailers = ['AMP', 'LAU', 'KCS', 'SCU', 'MMR', 'MDS', 'PL', 'Ecommerce', 'Digital', 'LES','others']

transaction_stamps = merchant.transactions.exclude(user_id=None)\
                             .exclude(status=Transaction.STATUS.canceled)\
                             .filter(created__date__lte=end_date)\
                             .filter(meta__isnull=True)\
                             .select_related('meta')\
                             .prefetch_related('items','items__product')\
                             .only('id', 'user_id', 'store','created', 'value','meta')

trx_annotate = transaction_stamps.values('id').annotate(
    item_quantity=Sum('items__quantity'),\
    product_count=Count('items__product_id', distinct=True))

print('setelah_annotate')
#store tag and area mapping
storetagvalue = set(StoreTagValue.objects.filter(group__merchant=merchant)\
                                     .filter(group__name='Retailer')\
                                     .filter(name__in=retailers)\
                                     .values_list('stores', flat=True))



store_tag_value_dict = {}
for store in Store.objects.filter(id__in=storetagvalue):
    tag_value_names = store.tag_values.values_list('name', flat=True)
    if store.area:
        area_name = store.area.name
    else:
        area_name = None
    store_tag_value_dict[store.id] = {'retailer': list(tag_value_names), 'area_name': area_name}

for key, value in store_tag_value_dict.items():
    shortest_retailer = min(value['retailer'], key=len)
    store_tag_value_dict[key]['retailer'] = shortest_retailer


data_list = []

batch_size = 10000
total_count = transaction_stamps.count()

print('you are here')
for i in range(0, total_count, batch_size):
    print('udah masuk')
    transactions_filtered = transaction_stamps[i:i+batch_size]

    for transaction in transactions_filtered:
        print(transaction.id)
        data_dict = {}
        data_dict['id'] = transaction.id
        data_dict['user_id'] = transaction.user_id
        data_dict['store_id'] = transaction.store_id
        data_dict['created'] = transaction.created
        data_dict['created_year'] = transaction.created.year
        data_dict['created_month'] = transaction.created.month
        data_dict['created_day'] = transaction.created.day
        data_dict['value'] = transaction.value

        print('meta')
        try:
            meta = transaction.meta
        except RelatedObjectDoesNotExist:
            meta = None
        print('lewat')

        if meta is not None and transaction.meta.employee_id is not None:
            data_dict['employee_count'] = 1
        else:
            data_dict['employee_count'] = 0

        data_dict['product_count'] = trx_annotate.get(id=transaction.id)['product_count']
        data_dict['item_quantity'] = trx_annotate.get(id=transaction.id)['item_quantity']


        # print(transaction.store_id)
        # data_dict['retailer'] = store_tag_value_dict.get(transaction.store_id)['retailer']
        data_dict['retailer'] = store_tag_value_dict.get(transaction.store_id, {}).get('retailer', None)
        data_dict['area_name'] = store_tag_value_dict.get(transaction.store_id, {}).get('area_name', None)

        # Append the dictionary to the list
        data_list.append(data_dict)

# Convert the list of dictionaries to a DataFrame
df = pd.DataFrame(data_list)
