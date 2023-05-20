# @Clara Pranata Stamps @Bintang M @Febri Aji P Stamps  tim Levis akan ke Bandung minggu dpn dan mau ada ngobrol2 sama member youth di bandung.

# Tolong dicarikan member yg belanja levis 2x atau lebih dlm 1 tahun terakhir.

# - Member yg belanja di OLS TSM Bandung dan BIP 198, 207,

# - Member yang belanja di Matahari Bandung [207, 137, 46, 562, 530, 421]


from datetime import date
from stamps.core.utils import prepare_datetime_range
from dateutil.relativedelta import relativedelta
import csv

start_date = date(2022, 3, 2)
end_date = date(2023, 3, 2)

store_ids_mds = [137, 46, 562, 530, 421]
store_ids_ols = [198, 207]

age_20 = end_date - relativedelta(years = 20)
age_25 = end_date - relativedelta(years = 25)
age_26 = end_date - relativedelta(years = 26)
age_30 = end_date - relativedelta(years = 30)

def annotate_trx(transaction_data):

    trxs_annotate = transaction_data.values('user_id','user__phone','user__profile__gender','user__has_incorrect_phone')\
                    .annotate(count_trx_id=Count('id'))\
                    .order_by('-count_trx_id')


    return trxs_annotate.filter(count_trx_id__gte=2), trxs_annotate.count()

def get_classification(start_date, end_date, store_id):

    transaction_data = Transaction.objects.filter(created__range=prepare_datetime_range(start_date, end_date))\
                         .exclude(status=Transaction.STATUS.canceled)\
                         .exclude(user_id=None)\
                         .filter(store_id__in=store_id)\
                         .exclude(user__profile__birthday=None)\
                         .exclude(user__profile__gender=None)\

    trxs_female = transaction_data.filter(user__profile__gender=Profile.GENDER.female)
    trxs_male = transaction_data.filter(user__profile__gender=Profile.GENDER.male)

    female_20_25 = trxs_female.filter(user__profile__birthday__range=(age_25, age_20))
    male_20_25 = trxs_male.filter(user__profile__birthday__range=(age_25, age_20))

    female_26_30 = trxs_female.filter(user__profile__birthday__range=(age_30, age_26))
    male_26_30 = trxs_male.filter(user__profile__birthday__range=(age_30, age_26))

    return female_20_25, male_20_25, female_26_30, male_26_30


def get_set_and_users(user_dict_trx):
    users_id = set(user_dict_trx.values_list('user_id',flat=True))
    user_qs = User.objects.filter(id__in=users_id)

    return user_qs

def write_csv(user_queryset, user_annotations,  criteria_name):

    with open(f'/tmp/user_id_criteria_{criteria_name}.csv','w') as data:
        header = ['id','name','age', 'trx_frequency', 'phone', 'invalid_phone']
        writer = csv.DictWriter(data, header)
        writer.writeheader()

        for user in user_queryset:

            id = user.id
            name = user.name
            phone = user.phone
            invalid_phone = user.has_incorrect_phone
            age = user.profile.get_age(on_date=end_date)
            trx_frequency = user_annotations.get(user_id=user.id)['count_trx_id']

            writer.writerow({
                             'id': id,
                             'name': name,
                             'age': age,
                             'trx_frequency': trx_frequency,
                             'phone': phone,
                             'invalid_phone': invalid_phone
                             })


female_20_25_ols, male_20_25_ols, female_26_30_ols, male_26_30_ols = get_classification(start_date, end_date, store_ids_ols)
female_20_25_mds, male_20_25_mds, female_26_30_mds, male_26_30_mds = get_classification(start_date, end_date, store_ids_mds)

female_20_25_annotate_ols, female_20_25_annotate_ols_count = annotate_trx(female_20_25_ols)
male_20_25_annotate_ols, male_20_25_annotate_ols_count = annotate_trx(male_20_25_ols)
female_26_30_annotate_ols, female_26_30_annotate_ols_count = annotate_trx(female_26_30_ols)
male_26_30_annotate_ols, male_26_30_annotate_ols_count = annotate_trx(male_26_30_ols)

female_20_25_annotate_mds, female_20_25_annotate_mds_count = annotate_trx(female_20_25_mds)
male_20_25_annotate_mds, male_20_25_annotate_mds_count = annotate_trx(male_20_25_mds)
female_26_30_annotate_mds, female_26_30_annotate_mds_count = annotate_trx(female_26_30_mds)
male_26_30_annotate_mds, male_26_30_annotate_mds_count = annotate_trx(male_26_30_mds)

female_20_25_ols_qs = get_set_and_users(female_20_25_annotate_ols)
male_20_25_ols_qs = get_set_and_users(male_20_25_annotate_ols)
female_26_30_ols_qs = get_set_and_users(female_26_30_annotate_ols)
male_26_30_ols_qs = get_set_and_users(male_26_30_annotate_ols)

female_20_25_mds_qs = get_set_and_users(female_20_25_annotate_mds)
male_20_25_mds_qs = get_set_and_users(male_20_25_annotate_mds)
female_26_30_mds_qs = get_set_and_users(female_26_30_annotate_mds)
male_26_30_mds_qs = get_set_and_users(male_26_30_annotate_mds)

write_csv(female_20_25_ols_qs, female_20_25_annotate_ols, 'female_20_25_ols_qs')
write_csv(male_20_25_ols_qs, male_20_25_annotate_ols, 'male_20_25_ols_qs')
write_csv(female_26_30_ols_qs, female_26_30_annotate_ols, 'female_26_30_ols_qs')
write_csv(male_26_30_ols_qs,  male_26_30_annotate_ols, 'male_26_30_ols_qs')

write_csv(female_20_25_mds_qs, female_20_25_annotate_mds, 'female_20_25_mds_qs')
write_csv(male_20_25_mds_qs, male_20_25_annotate_mds, 'male_20_25_mds_qs')
write_csv(female_26_30_mds_qs, female_26_30_annotate_mds, 'female_26_30_mds_qs')
write_csv(male_26_30_mds_qs,  male_26_30_annotate_mds, 'male_26_30_mds_qs')


# header = ['name','age', 'trx_frequency', 'phone', 'invalid_phone']
# with open(f'/tmp/user_id_criteria_test.csv','w',newline='') as data:

#     writer = csv.DictWriter(data, header)
#     writer.writeheader()

#     for user in female_20_25_ols_qs:


#         name = user.name
#         phone = user.phone
#         invalid_phone = user.has_incorrect_phone
#         age = user.profile.get_age(on_date=end_date)
#         trx_frequency = female_20_25_annotate_ols.get(user_id=user.id)['count_trx_id']

#         writer.writerow({
#                             'name': name,
#                             'age': age,
#                             'trx_frequency': trx_frequency,
#                             'phone': phone,
#                             'invalid_phone': invalid_phone
#                             })


#                             #   .values('user_id','user__profile__birthday','created')


# member_age_dict = {member.get('user_id'): math.floor((member.get('created').date() - member.get('user__profile__birthday')).days/365.25) for member in members}




# Tolong di pisahkan masing2 sheet.
# 1. Member 20-25 tahun  perempuan (OLS)
# 2. Member 26 - 30 tahun laki2 (OLS)
# 3. Member 20-25 tahun  perempuan (MDS)
# 4. Member 26 - 30 tahun laki2 (MDS)
