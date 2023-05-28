import csv
from datetime import date
import math


from django.utils import timezone
# from app.models import User  # replace with your actual import
from stamps.core.utils import prepare_datetime_range

# INPUT_PATH = '/tmp/test_data_exc.csv'
# OUTPUT_PATH = f'/tmp/rfm_transaction_quarter_2023-levisQ1{timezone.localdate()}.csv'

# Tiers
TIER_SPENDING_1 = 1000000
TIER_SPENDING_2 = 2000000
TIER_FREQUENCY_1 = 1
TIER_FREQUENCY_2 = 4

# Column names

# Quarter dates
START_QUARTER_DATE = date(2022, 8, 29)
END_QUARTER_DATE = date(2022, 8, 30)

def get_spending_classification(sum_values: int, count_values: int) -> tuple:
    ATV = sum_values/count_values

    if ATV < TIER_SPENDING_1:
        spending_group = 'LS'
    elif ATV >= TIER_SPENDING_1 and ATV <= TIER_SPENDING_2:
        spending_group = 'MS'
    else:
        spending_group = 'HS'

    return spending_group

# def get_spending_classification(sum_values: int, count_values: int) -> tuple:
#     ATV = sum_values/count_values
#     if ATV < TIER_SPENDING_1:
#         return 'LS'
#     elif ATV <= TIER_SPENDING_2:
#         return 'MS'
#     return 'HS'


def get_frequency_classification(count_values: int) -> str:

    if count_values <= TIER_FREQUENCY_1:
        frequency_group = "LF"
    elif count_values > TIER_FREQUENCY_1 and count_values <= TIER_FREQUENCY_2:
        frequency_group = "MF"
    else:
        frequency_group = "HF"

    return frequency_group

# def get_frequency_classification(count_values: int) -> str:
#     if count_values <= TIER_FREQUENCY_1:
#         return "LF"
#     elif count_values <= TIER_FREQUENCY_2:
#         return "MF"
#     return "HF"


def get_user_mapping(transaction_dict):
    user_ids = set(transaction_dict.values_list('user_id', flat = True))
    users = User.objects.filter(id__in=user_ids).select_related('profile')\
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

    transactions_dict = Transaction.objects.exclude(status=Transaction.STATUS.canceled)\
                                           .exclude(user_id=None)\
                                           .filter(created__range=prepare_datetime_range(START_QUARTER_DATE, END_QUARTER_DATE))\
                                           .values('user_id')\
                                           .annotate(count_trx=Count('id'), sum_value=Sum('value'))\
                                           .order_by('user_id')

    return transactions_dict


def get_rfm_data(user_id, transactions_dict, user_mapping, START_QUARTER_DATE=START_QUARTER_DATE, END_QUARTER_DATE=END_QUARTER_DATE):
    transaction = transactions_dict.get(user_id=user_id)
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
        'atv': round(sum_values / count_values, 2),
        'spending_group': get_spending_classification(sum_values, count_values),
        'frequency_group': get_frequency_classification(count_values),
        'gender': user_data['user_gender'],
        'current_age': current_age,
        'age_cat': age_cat,
        'date_joined': date_joined,
        'status_join': status_join
        }


transaction_dict = get_transaction(START_QUARTER_DATE, END_QUARTER_DATE)
user_mapping = get_user_mapping(transaction_dict)


rfm_dict = {}

for uid in user_mapping.keys():
    data = get_rfm_data(uid, transaction_dict, user_mapping)
    rfm_dict[uid] = data
