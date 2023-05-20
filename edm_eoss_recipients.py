# Member yang aktif selama 5 tahun
# +
# Member yang register di 5 tahun terakhir tapi belum pernah transaksi
# +
# Member yang tidak ada trx lebih dari 5 tahun, tapi ATV di atas 2juta
# Nanti tolong ditotalin jumlahnya berapa dan info ke bu megah yaa. thankyouuu

from datetime import date
from stamps.core.utils import prepare_datetime_range
import pandas as pd


start_date = date(2018, 5, 16)
end_date = date(2023, 5, 16)

#ACTIVE 5 YEARS

merchant = Merchant.objects.get(id=2)
transactions = merchant.transactions.exclude(status=Transaction.STATUS.canceled)\
                                .exclude(user_id=None)\
                                .filter(created__range=prepare_datetime_range(start_date, end_date))

trx_above = transactions.values('user_id').annotate(counts=Count('id')).order_by('counts')
active_users = set(trx_above.values_list('user_id', flat=True))

active_users_qs = User.objects.filter(id__in=active_users)\
                              .values('id','has_incorrect_email','has_incorrect_phone','profile__gender','profile__birthday')

pd.DataFrame(active_users_qs).to_csv('/home/bintang/active_selama_5tahun_karet_merah.csv.', index=False)

#JOIN 5 YEARS NO TRX
start_date2 = date(2021, 5, 16)
end_date2 = date(2023, 5, 16)
memberships = merchant.group.memberships.filter(created__range=prepare_datetime_range(start_date2, end_date2))\
                                        .filter(user__transactions__isnull=True)

no_trx_user = set(memberships.values_list('user_id', flat=True))
no_trx_user_qs = User.objects.filter(id__in=no_trx_user)\
                             .values('id','has_incorrect_email','has_incorrect_phone','profile__gender','profile__birthday')

pd.DataFrame(no_trx_user_qs).to_csv('/home/bintang/no_trx_join2tahun_karet_kuning.csv.', index=False)




#NO TRX last 5 years, atv above 2jt

# user_trx_abv5 = User.objects.exclude(transactions__status=Transaction.STATUS.canceled)\
#                             .exclude(id=None)\
#                             .exclude(transactions__created__range=prepare_datetime_range(start_date, end_date))\
#                             .exclude(transactions__created__date__gte=end_date)\

# user_trx_abv5 = User.objects.exclude(transactions__status=Transaction.STATUS.canceled)\
#                             .exclude(id=None)\
#                             .exclude(transactions__created__range=prepare_datetime_range(start_date, end_date))\
#                             .exclude(transactions__created__date__gte=end_date)\


transactions_abv5 = merchant.transactions.exclude(status=Transaction.STATUS.canceled)\
                                .exclude(user_id=None)\
                                .filter(created__lt=start_date)\
                                .order_by('created')

                                # .exclude(created__range=prepare_datetime_range(start_date, end_date))\
                                # .exclude(created__date__gte=end_date)\


transactions_no_abv5 = merchant.transactions.exclude(status=Transaction.STATUS.canceled)\
                                .exclude(user_id=None)\
                                .filter(created__range=prepare_datetime_range(start_date, end_date))\
                                .order_by('created')


transactions_abv5_user = set(transactions_abv5.values_list('user_id',flat=True))
transactions_no_abv5_user = set(transactions_no_abv5.values_list('user_id', flat=True))
user_only_befor5 = transactions_abv5_user.difference(transactions_no_abv5_user)

transactions_abv5_filtered = transactions_abv5.filter(user_id__in=user_only_befor5)

aggregate_trxabv5 = transactions_abv5_filtered.values('user_id')\
                                     .annotate(Average=Avg('value'), count_trx=Count('id'))\
                                     .order_by('Average')

aggregate_trx_user = set(aggregate_trxabv5.filter(Average__gt=2000000).values_list('user_id', flat=True))

aggregate_trx_user_qs = User.objects.filter(id__in=aggregate_trx_user)\
                                    .values('id','has_incorrect_email','has_incorrect_phone','profile__gender','profile__birthday')

pd.DataFrame(aggregate_trx_user_qs).to_csv('/home/bintang/no_trxlast5_atvabove2jt_karet_hijau.csv.', index=False)








print('Active Member', len(active_users))
print('No trx , join last 5 years Member', len(no_trx_user))
print('atv above 2jt member, no trx last 5 years', len(aggregate_trx_user))

# Active Member 985173
# No trx , join last 5 years Member 201106
# atv above 2jt member, no trx last 5 years 15698

import pandas as pd

df = pd.read_csv('/tmp/edm 501 early access userid.csv')

column_name = df.columns[0]
unique_values = df[column_name].unique()

# Now, include the column name in the list of unique values.
unique_values_with_col_name = [column_name] + list(unique_values)

# Print the result
print(unique_values_with_col_name)

Users = User.objects.filter(id__in=unique_values_with_col_name)\
                    .values('id','profile__gender')

for user in Users:
    if user['profile__gender'] == 1:
        user['profile__gender'] = 'male'
    elif user['profile__gender'] == 2:
        user['profile__gender'] = 'female'

pd.DataFrame(Users).to_csv('/tmp/edm_501_early_acces_with_gender.csv')
