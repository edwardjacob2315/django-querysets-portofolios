import csv
from datetime import date
import math


date_today = timezone.localdate()
INPUT_PATH = '/tmp/test_data_exc.csv'
OUTPUT_PATH = f'/tmp/rfm_transaction_quarter_2023-levisQ1{date_today}.csv'


tier_spending_1 = 1000000
tier_spending_2 = 2000000

tier_frequency_1 = 1
tier_frequency_2 = 4

# insert column name here
transaction_id = 'Transaction ID'
id_user = 'User ID'
tanggal = 'Date'
value = 'Value'

start_quarter_date = date(2023, 1, 1)
end_quarter_date = date(2023, 1, 31)


def get_spending_classification(sum_values: int, count_values: int) -> tuple:
    ATV = sum_values/count_values

    if ATV < tier_spending_1:
        spending_group = 'LS'
    elif ATV >= tier_spending_1 and ATV <= tier_spending_2:
        spending_group = 'MS'
    else:
        spending_group = 'HS'

    return (ATV, spending_group)


def get_frequency_classification(count_values: int) -> str:

    if count_values <= tier_frequency_1:
        frequency_group = "LF"
    elif count_values > tier_frequency_1 and count_values <= tier_frequency_2:
        frequency_group = "MF"
    else:
        frequency_group = "HF"

    return frequency_group


with open(INPUT_PATH, newline='', encoding='utf-8-sig') as csvfile:
    reader = csv.DictReader(csvfile)

    data = {}
    for rows in reader:
        price = rows[value]
        if price == '':
            price = 0
        rows[value] = float(price)
        user_id = rows.get(id_user)
        if user_id:
            user_id = int(user_id)
        else:
            continue
        if user_id not in data.keys():
            data[user_id] = []
        data[user_id].append({
            'transaction_id': rows[transaction_id],
            'date': rows[tanggal],
            'value': rows[value]
        })



users=User.objects.filter(id__in=data.keys()).select_related('profile')\
                  .only('date_joined','profile__gender','profile__birthday')

user_mapping = dict()

for user in users:
    user_id = user.id

    user_gender = user.profile.get_gender_display() if user.profile.gender is not None else None
    user_age = (math.floor((end_quarter_date - user.profile.birthday).days/365.25) if user.profile.birthday is not None else None)

    user_mapping[user_id] = {
                          'user_date_joined': user.date_joined,
                          'user_gender': user_gender,
                          'user_age': user_age
                           }


with open(OUTPUT_PATH, 'w', newline='') as csvfile:
    write = csv.writer(csvfile, delimiter=';')
    write.writerow(['user_id',
                    'transaction_count',
                    'revenue',
                    'atv',
                    'spending_group',
                    'frequency_group',
                    'concat_group',
                    'gender',
                    'current_age',
                    'age_cat',
                    'date_joined',
                    'status_join'])

    for key, value in data.items():
        transaction_values = []
        for items in value:
            user_id = key
            transaction_values.append(items['value'])
            sum_values = sum(transaction_values)
            count_values = len(transaction_values)
            user_data = user_mapping.get(user_id)
            gender = user_data['user_gender']
            date_joined = user_data['user_date_joined'].date()

            status_join = 'New' if date_joined >= start_quarter_date and date_joined <= end_quarter_date else 'Returning'

            current_age = user_data['user_age']

            if current_age is not None and current_age >= 18 and current_age <= 30:
                age_cat = '18 - 30'
            elif current_age is not None and current_age > 30:
                age_cat = '> 30'
            elif current_age is not None and current_age < 18:
                age_cat = '< 18'
            else:
                age_cat = None

        ATV, spending_group = get_spending_classification(sum_values, count_values)
        frequency_group = get_frequency_classification(count_values)

        concat_group = frequency_group+spending_group

        write.writerow([user_id,
                        count_values,
                        sum_values,
                        round(ATV, 2),
                        spending_group,
                        frequency_group,
                        concat_group,
                        gender,
                        current_age,
                        age_cat,
                        date_joined,
                        status_join
                        ])
