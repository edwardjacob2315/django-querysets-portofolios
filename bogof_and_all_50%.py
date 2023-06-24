# BOGOF
# - 150K member
# - pernah belanja barang diskon
# - punya ATV 1.2-1.7jt
# -  OLS, SOGO group


# All 50%
# - MDS, Levi.co.id
# - ATV 799K - 1099K
# - pernah belanja barang diskon
# - 50K data

# @Bintang Muhammad
#  tlg ditarik ya , kalau bisa dicarikan dlu jumlahnya bogof brpa, all 50 berapa sesuai request

# exclude konsumen yg udah blanja 24 mei - 20 jun

from datetime import date
from django.db.models import Avg
from stamps.core.utils import prepare_datetime_range
from django.db.models import OuterRef, Subquery
import pandas as pd


merchant = Merchant.objects.get(id=2)

exclude_date_start = date(2023, 5, 24)
exclude_date_end = date(2023, 6, 20)

start_date_real = date(2018, 6, 20)
end_date_real = date(2023, 6, 20)



# tarik orang2 yang belanja produk eoss selama 24 mei-20 juni
# dibagi 2, user ID ganjil, user ID genap
# nanti yang genap dibagi lagi -> OLS & SOGO (untuk dikirim BOGOF), MDS & Levi.co.id (untuk dikirim ALL 50%). ini liatnya dari pembelian terakhir dia aja di mana
# ini nanti filter DD nya semua yg ada kata EOSS dan voucher (edited)

#Include Specific Date
trx_exclude_people = set(merchant.transactions.exclude(status=Transaction.STATUS.canceled)\
                                              .exclude(user_id=None)\
                                              .filter(created__range=prepare_datetime_range(exclude_date_start, exclude_date_end)).values_list('user_id', flat=True))\

uids_include_people_even = {num for num in trx_exclude_people if num % 2 == 0}
uids_include_people_odd = {num for num in trx_exclude_people if num % 2 != 0}

ganjil = pd.DataFrame(list(uids_include_people_odd), columns=['user_id'])

# Save DataFrame to CSV
ganjil.to_csv('uids_include_people_odd.csv', index=False)

items_eoss = Item.objects.filter(
    Q(extra_data__discount_description1__icontains='eoss') |
    Q(extra_data__discount_description2__icontains='eoss') |
    Q(extra_data__discount_description3__icontains='eoss')
)


items_voucher = Item.objects.filter(
    Q(extra_data__discount_description1__icontains='voucher') |
    Q(extra_data__discount_description2__icontains='voucher') |
    Q(extra_data__discount_description3__icontains='voucher')
)


ids_disc_eoss = set(items_eoss.values_list('transaction_id', flat=True))
ids_disc_voucher = set(items_voucher.values_list('transaction_id', flat=True))
dds = ids_disc_eoss.union(ids_disc_voucher)



# RETAILER OLS SOGO
retailer_ols_sogo = ['AMP', 'LAU', 'KCS', 'SCU', 'MMR', 'PL']

# RETAILER MDS LEVIS, INSERT LEVI.CO.ID LATER
retailer_mds_levi = ['MDS']

storetagvalue = StoreTagValue.objects.filter(group__merchant=merchant)\
                                     .filter(group__name='Retailer')

store_ols_sogo = set(storetagvalue.filter(name__in=retailer_ols_sogo)\
                                  .values_list('stores', flat=True))

# Also add 475 here
store_mds_levi = set(storetagvalue.filter(name__in=retailer_mds_levi)\
                                  .values_list('stores', flat=True))
store_mds_levi.add(475)

trx_ids_people_even = pd.DataFrame(merchant.transactions.exclude(status=Transaction.STATUS.canceled)\
                                       .filter(user_id__in=uids_include_people_even)\
                                       .filter(id__in=dds)\
                                       .values('id', 'user_id', 'created__date', 'store_id'))

trx_ids_people_even = trx_ids_people_even.sort_values(by=['created__date', 'id'], ascending=[False, False])
latest_transactions = trx_ids_people_even.groupby('user_id').first().reset_index()


latest_transactions_ols_sogo = latest_transactions[latest_transactions['store_id'].isin(store_ols_sogo)]
latest_transactions_mds_levi = latest_transactions[latest_transactions['store_id'].isin(store_mds_levi)]

# Get sets of user_ids
user_ids_ols_sogo = set(latest_transactions_ols_sogo['user_id'])
user_ids_mds_levi = set(latest_transactions_mds_levi['user_id'])









trx_excluded = merchant.transactions.exclude(status=Transaction.STATUS.canceled)\
                                  .exclude(user_id=None)\
                                  .filter(created__range=prepare_datetime_range(start_date_real, end_date_real))\
                                  .order_by('created')


# RETAILER OLS SOGO
retailer_ols_sogo = ['AMP', 'LAU', 'KCS', 'SCU', 'MMR', 'PL']

# RETAILER MDS LEVIS, INSERT LEVI.CO.ID LATER
retailer_mds_levi = ['MDS']

storetagvalue = StoreTagValue.objects.filter(group__merchant=merchant)\
                                     .filter(group__name='Retailer')

store_ols_sogo = set(storetagvalue.filter(name__in=retailer_ols_sogo)\
                                  .values_list('stores', flat=True))

# Also add 475 here
store_mds_levi = set(storetagvalue.filter(name__in=retailer_mds_levi)\
                                  .values_list('stores', flat=True))
store_mds_levi.add(475)

#FILTER TRX BASED ON STORE AFTER EXCLUDED
trx_ols_sogo = trx_excluded.filter(store_id__in=store_ols_sogo)
trx_mds_levi = trx_excluded.filter(store_id__in=store_mds_levi)


def annotate_trx(transaction_query_set):
    trx_annotate = transaction_query_set.values('user_id')\
                                        .annotate(avg_value=Avg('value'))\
                                        .order_by()
    return trx_annotate

# trx_annotate_ols_sogo_resik = annotate_trx(trx_ols_sogo)
# trx_annotate_mds_levi_resik = annotate_trx(trx_mds_levi)

# trx_annotate_ols_sogo = annotate_trx(trx_ols_sogo).filter(Q(avg_value__gte=1200000) & Q(avg_value__lte=1700000))
# trx_annotate_mds_levi = annotate_trx(trx_mds_levi).filter(Q(avg_value__gte=799000) & Q(avg_value__lte=1099000))

# trx_annotate_ols_sogo = annotate_trx(trx_ols_sogo).filter(avg_value__gte=1200000)
# trx_annotate_mds_levi = annotate_trx(trx_mds_levi).filter(avg_value__gte=799000)

trx_annotate_ols_sogo = annotate_trx(trx_ols_sogo).filter(avg_value__gte=399000)
trx_annotate_mds_levi = annotate_trx(trx_mds_levi).filter(avg_value__gte=120000)



#OLS SOGO ATV = Rp. 1.200.000 - Rp. 1.700.000
#MDS LEVI.CO.ID ATV = Rp. 799.000 - Rp. 1.099.000

#CHECK PEOPLE WHO USE EOSS AND BOGOF
items_eoss = Item.objects.filter(
    Q(extra_data__discount_description1__icontains='eoss') |
    Q(extra_data__discount_description2__icontains='eoss') |
    Q(extra_data__discount_description3__icontains='eoss')
)


items_bogof = Item.objects.filter(
    Q(extra_data__discount_description1__icontains='bogof') |
    Q(extra_data__discount_description2__icontains='bogof') |
    Q(extra_data__discount_description3__icontains='bogof')
)


items_voucher = Item.objects.filter(
    Q(extra_data__discount_description1__icontains='voucher') |
    Q(extra_data__discount_description2__icontains='voucher') |
    Q(extra_data__discount_description3__icontains='voucher')
)


user_ids_disc_eoss = set(items_eoss.values_list('transaction__user_id', flat=True))
user_ids_disc_bogof = set(items_bogof.values_list('transaction__user_id', flat=True))
user_ids_disc_voucher = set(items_voucher.values_list('transaction__user_id', flat=True))
user_ids_eoss_bogof_voucher = user_ids_disc_eoss.union(user_ids_disc_bogof).union(user_ids_disc_voucher)



#FIND FROM ANNOTATE WHO BUYS FROM DISC(EOSS AND BOGOF)
uids_bogof_int = set(trx_annotate_ols_sogo.values_list('user_id', flat=True))
# uids_bogof_int_clean = {user['user_id'] for user in trx_annotate_ols_sogo}

uids_bogof_int_clean = uids_bogof_int.intersection(user_ids_eoss_bogof_voucher).difference(trx_exclude_people).union(user_ids_ols_sogo)

uids_all50_int = set(trx_annotate_mds_levi.values_list('user_id', flat=True))

uids_all50_int_clean = uids_all50_int.intersection(user_ids_eoss_bogof_voucher).difference(trx_exclude_people)
uids_all50_exclude_bogof_uids = uids_all50_int_clean.difference(uids_bogof_int_clean).union(user_ids_mds_levi)

print(len(uids_bogof_int_clean))
print(len(uids_all50_exclude_bogof_uids))
# Tambah org yg belanja 20 jun - 10 july 2022 (diskon dan non diskon) tanpa liat atv -

bogof = pd.DataFrame(User.objects.filter(id__in=uids_bogof_int_clean)\
                                 .values('id', 'profile__gender', 'email',\
                                        'phone', 'has_incorrect_email', 'has_incorrect_phone'))

bogof.to_csv('/tmp/bogof_2023.csv', index=False)

all50 = pd.DataFrame(User.objects.filter(id__in=uids_all50_exclude_bogof_uids)\
                                 .values('id', 'profile__gender', 'email',\
                                        'phone', 'has_incorrect_email', 'has_incorrect_phone'))

all50.to_csv('/tmp/all50%_2023.csv', index=False)
