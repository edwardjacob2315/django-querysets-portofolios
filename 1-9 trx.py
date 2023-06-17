import csv
from datetime import date
from typing import Any, Dict
import pandas as pd

from django.db.models import Q
import pandas as pd
# from stamps.core.utils import prepare_datetime_range

start_date=date(2022, 5, 30)
end_date=date(2023, 5, 28)

# 1-Nov-21	s/d	30-Oct-22
# 29-Oct-18	s/d	27-Oct-19

# start_date=date(2018, 10, 29)
# end_date=date(2019, 10, 27)


merchant = StampsMerchant.objects.get(slug='levis-indonesia')

vip_store_ids = set(merchant.store_tags.get(name='VIP Club').stores.values_list('id', flat=True))

online = set(StampsStoreTagValue.objects
                        .get(name='Ecommerce').stores
                        .values_list('id', flat=True))

offline = vip_store_ids.difference(online)

online_offline = vip_store_ids.union(online)

transaction_qs_all = CHTransaction.objects.filter(merchant_id=merchant.id)\
                                        .exclude(status=CHTransaction.Status.CANCELED)\
                                        .filter(created__date__range=(start_date, end_date))\
                                        .filter(store_id__in=online_offline)\
                                        .exclude(user_id=0)\



transactions_all = transaction_qs_all.order_by('user_id').values('user_id') \
                                    .annotate(count=Count('id', distinct=True), total=Sum('value'))

transactions_all_items = transaction_qs_all.order_by('user_id').values('user_id')\
                                    .annotate(sum_item=Sum('items__quantity'))

trx_all_df = pd.DataFrame(transactions_all)
trx_all_item_df = pd.DataFrame(transactions_all_items)
trx_all_lj = pd.merge(trx_all_df, trx_all_item_df, on='user_id')


mask1 = (trx_all_lj['count'] == 1)
mask2 = (trx_all_lj['count'] >= 2) & (trx_all_lj['count'] <= 4)
mask3 = (trx_all_lj['count'] >= 5) & (trx_all_lj['count'] <= 7)
mask4 = (trx_all_lj['count'] >= 8) & (trx_all_lj['count'] <= 10)
mask5 = (trx_all_lj['count'] > 10)

# Apply the masks to the DataFrame
trx_all_lj_1 = trx_all_lj[mask1]
trx_all_lj_2_to_4 = trx_all_lj[mask2]
trx_all_lj_5_to_7 = trx_all_lj[mask3]
trx_all_lj_8_to_10 = trx_all_lj[mask4]
trx_all_lj_above_10 = trx_all_lj[mask5]

uids_2_to_4 = trx_all_lj_2_to_4['user_id'].unique()

trx24_id = set(transaction_qs_all.filter(user_id__in=uids_2_to_4).values_list('id', flat=True))

trxs24 = StampsTransaction.objects.filter(id__in=trx24_id)


def get_data_function(dataframe):

    user_count = dataframe.shape[0]
    sum_value = dataframe['total'].sum()
    sum_trx = dataframe['count'].sum()
    sum_item = dataframe['sum_item'].sum()

    list = ['user_count', user_count, 'sum_value', sum_value, 'sum_trx', sum_trx, 'sum_item', sum_item]

    return list


lj1_list = get_data_function(trx_all_lj_1)
lj_2_to_4_list = get_data_function(trx_all_lj_2_to_4)
lj_5_to_7_list = get_data_function(trx_all_lj_5_to_7)
lj_8_to_10_list = get_data_function(trx_all_lj_8_to_10)
lj_above_10_list = get_data_function(trx_all_lj_above_10)



# items = StampsItem.objects.filter(
#     (Q(extra_data__discount_description1__isnull=False) & ~Q(extra_data__discount_description1="")) |
#     (Q(extra_data__discount_description2__isnull=False) & ~Q(extra_data__discount_description2="")) |
#     (Q(extra_data__discount_description3__isnull=False) & ~Q(extra_data__discount_description3="")))\
#         .filter(transaction_id__in=trx24_id)


def create_discount_df(trx24_id):
    # Querying the items
    items = StampsItem.objects.filter(transaction_id__in=trx24_id)

    # Converting the items to a DataFrame
    items_df = pd.DataFrame.from_records(items.values('transaction_id', 'extra_data__discount_description1', 'extra_data__discount_description2', 'extra_data__discount_description3'))

    # Melting the DataFrame (i.e., converting from wide to long format)
    melted_df = pd.melt(items_df, id_vars='transaction_id', value_vars=['extra_data__discount_description1', 'extra_data__discount_description2', 'extra_data__discount_description3'])

    # Grouping by discount description and counting distinct transaction ids
    count_df = melted_df.groupby('value')['transaction_id'].nunique().reset_index()
    count_df.columns = ['Discount Description', 'Distinct Transaction Count']

    return count_df, melted_df

count_df24, melted_df24 = create_discount_df(trx24_id)
count_df24 = count_df24.sort_values('Distinct Transaction Count', ascending=False)
# transaction = merchant.transactions.filter(id__in = items)\
#                         .filter(created__range=prepare_datetime_range(start_date, end_date))\
#                         .aggregate(trx_count=(Count('id', distinct=True)), trx_value=(Sum('value')))


# transaction_all = merchant.transactions.exclude(status=Transaction.STATUS.canceled)\
#                         .filter(created__range=prepare_datetime_range(start_date, end_date))\
#                         .aggregate(trx_count=(Count('id', distinct=True)), trx_value=(Sum('value')))


x = StampsTransaction.objects.filter(
    Q(id__in=trx24_id) &
    (
        Q(items__extra_data__discount_description1='ID-LPA-CRM-Surprise V500K-SOGO') |
        Q(items__extra_data__discount_description2='ID-LPA-CRM-Surprise V500K-SOGO') |
        Q(items__extra_data__discount_description3='ID-LPA-CRM-Surprise V500K-SOGO')
    )
)
