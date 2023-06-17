from datetime import date
import math
import pandas as pd

from lsco_reporting_server.core.utils import prepare_datetime_range
from lsco_reporting_server.core.calendar_fiscal import get_quarter

# Tiers
TIER_SPENDING_1 = 1000000
TIER_SPENDING_2 = 2000000
TIER_FREQUENCY_1 = 1
TIER_FREQUENCY_2 = 4


# # Quarter dates This Year
START_QUARTER_DATE = date(2022, 5, 30)
END_QUARTER_DATE = date(2023, 5, 28)
REAL_START_QUARTER_DATE = date(2023, 2, 27)

# Quarter dates Last Year
# START_QUARTER_DATE = date(2021, 5, 31)
# END_QUARTER_DATE = date(2022, 5, 29)
# REAL_START_QUARTER_DATE = date(2022, 2, 28)


retailers = ['AMP', 'LAU', 'KCS', 'SCU', 'MMR', 'MDS', 'PL', 'Ecommerce']
merchant = StampsMerchant.objects.get(id=2)
storetagvalue = set(StampsStoreTagValue.objects.filter(group__merchant=merchant)\
                                     .filter(group__name='Retailer')\
                                     .filter(name__in=retailers)\
                                     .values_list('stores', flat=True))

store_tag_value_dict = {}
for store in StampsStore.objects.filter(id__in=storetagvalue):
    tag_value_names = store.tag_values.values_list('name', flat=True)
    store_tag_value_dict[store.id] = {'retailer': list(tag_value_names)}

for key, value in store_tag_value_dict.items():
    shortest_retailer = min(value['retailer'], key=len)
    store_tag_value_dict[key]['retailer'] = shortest_retailer

store_list = [{'store_id': k, **v} for k, v in store_tag_value_dict.items()]
store_retailer_df = pd.DataFrame(store_list)




def get_spending_classification(atv: int) -> tuple:

    if atv < TIER_SPENDING_1:
        spending_group = 'LS'
    elif atv >= TIER_SPENDING_1 and atv <= TIER_SPENDING_2:
        spending_group = 'MS'
    else:
        spending_group = 'HS'

    return spending_group


def get_frequency_classification(count_values: int) -> str:

    if count_values <= TIER_FREQUENCY_1:
        frequency_group = "LF"
    elif count_values > TIER_FREQUENCY_1 and count_values <= TIER_FREQUENCY_2:
        frequency_group = "MF"
    else:
        frequency_group = "HF"

    return frequency_group


def get_user_mapping(transaction_dict):
    user_ids = set(transaction_dict.values_list('user_id', flat = True))
    users =StampsUser.objects.filter(id__in=user_ids).select_related('profile')\
                        .only('date_joined','profile__gender','profile__birthday')

    user_mapping = {}
    for user in users:
        user_mapping[user.id] = {
            'user_date_joined': user.date_joined,
            'user_gender': user.profile.get_gender_display() if user.profile.gender is not None else None,
            'user_age': math.floor((END_QUARTER_DATE - user.profile.birthday).days/365.25) if user.profile.birthday is not None else None
        }
    return user_mapping


def get_transaction(START_QUARTER_DATE, END_QUARTER_DATE):

    transactions_qs = StampsTransaction.objects.exclude(status=StampsTransaction.STATUS.canceled)\
                                              .filter(created__range=prepare_datetime_range(START_QUARTER_DATE, END_QUARTER_DATE))\

    transactions_qs_member = transactions_qs.exclude(user_id=None)\

    transactions_dict = transactions_qs_member.values('user_id')\
                                       .annotate(count_trx=Count('id'), sum_value=Sum('value'), atv=Avg('value'))\
                                       .order_by('user_id')

    transactions_dict_retailer = pd.DataFrame(transactions_qs.values('id','user_id','store_id','value', 'from_returning_user'))


    return transactions_qs, transactions_dict, transactions_dict_retailer


def get_rfm_data(user_id, transactions_dict, user_mapping, START_QUARTER_DATE=REAL_START_QUARTER_DATE, END_QUARTER_DATE=END_QUARTER_DATE):

    transaction = transactions_dict.get(user_id=user_id)
    atv_value = transaction['atv']
    sum_values = transaction['sum_value']
    count_values = transaction['count_trx']
    user_data = user_mapping.get(user_id)
    date_joined = user_data['user_date_joined'].date()
    status_join = 'New' if date_joined >= START_QUARTER_DATE and date_joined <= END_QUARTER_DATE else 'Returning'

    current_age = user_data['user_age']
    if current_age is not None and current_age >= 18 and current_age <= 30:
        age_cat = '18 - 30'
    elif current_age is not None and current_age > 30:
        age_cat = '> 30'
    elif current_age is not None and current_age < 18:
        age_cat = '< 18'
    else:
        age_cat = None

    return {
        'user_id': user_id,
        'transaction_count': count_values,
        'revenue': sum_values,
        'atv': atv_value,
        'frequency_group': get_frequency_classification(count_values),
        'spending_group': get_spending_classification(atv_value),
        'gender': user_data['user_gender'],
        'current_age': current_age,
        'age_cat': age_cat,
        'date_joined': date_joined,
        'status_join': status_join
        }


transactions_qs, transactions_dict, transactions_dict_retailer = get_transaction(START_QUARTER_DATE, END_QUARTER_DATE)
user_mapping = get_user_mapping(transactions_dict)


rfm_dict = {}

for uid in user_mapping.keys():
    data = get_rfm_data(uid, transactions_dict, user_mapping)
    rfm_dict[uid] = data

df_rfm = pd.DataFrame.from_dict(rfm_dict, orient='index')
transactions_dict_retailer = pd.merge(transactions_dict_retailer, store_retailer_df, how = 'left', on='store_id')
trx_dict_df = pd.merge(transactions_dict_retailer, df_rfm[['user_id', 'frequency_group', 'spending_group']], how = 'left', on='user_id')

# df_rfm.to_csv(f'/home/bintang/df_rfm_{START_QUARTER_DATE}-{END_QUARTER_DATE}.csv', index=False)
trx_dict_df.to_csv(f'/home/bintang/trx_dict_df{START_QUARTER_DATE}-{END_QUARTER_DATE}.csv', index=False)
