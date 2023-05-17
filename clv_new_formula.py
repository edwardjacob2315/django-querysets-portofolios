import pandas as pd
from typing import Optional
from datetime import date
import math
from dateutil.relativedelta import relativedelta
from stamps.core.utils import prepare_datetime_range

merchant_slug = "levis-indonesia"

end_date_quarter = date(2023, 4, 30)

end_date_quarter_list = [
                        #end_date_quarter #prefix
                        [date(2023, 4, 30), 'april 2023']
                        # [date(2022, 5, 1), 'april 2022'],
                        # [date(2021, 5, 2), 'april 2021'],
                        # [date(2020, 4, 26), 'april 2020'],
                        # [date(2019, 4, 28), 'april 2019'],
                        # [date(2018, 4, 29), 'april 2018']
                        ]

age_30 = end_date_quarter - relativedelta(years=30)
age_18 = end_date_quarter - relativedelta(years=18)


def get_delta_year(years, count_trx):
    if years == 0 and count_trx > 1:
        return years + 1
    return years


def get_age_classification(transaction_qs):
    above30_trx = transaction_qs.filter(user__profile__birthday__lt=age_30)
    bw_18_30_trx = transaction_qs.filter(user__profile__birthday__range=prepare_datetime_range(age_30, age_18))
    under18_trx = transaction_qs.filter(user__profile__birthday__gt=age_18)

    return above30_trx, bw_18_30_trx, under18_trx


def get_transaction(end_date_quarter):
    merchant = Merchant.objects.get(id=2)

    transaction_qs = merchant.transactions.exclude(status=Transaction.STATUS.canceled)\
                        .filter(created__date__lte=end_date_quarter)\
                        .exclude(user_id=None)\
                        .select_related('user__profile')\
                        .only('user_id','created','id', 'user__profile')\

    transaction_qs_male = transaction_qs.filter(user__profile__gender=Profile.GENDER.male)
    transaction_qs_female = transaction_qs.filter(user__profile__gender=Profile.GENDER.female)



    return transaction_qs, transaction_qs_male, transaction_qs_female





def get_avg_lifespan_freq_atv(transaction_qs, prefix):

    transaction_avg_lifespan = transaction_qs.values("user_id").annotate(first_trx=Min("created"), last_trx=Max("created"),\
                                              trx_count=Count('id', distinct=True))\
                                             .order_by('first_trx')\
                                             .filter(trx_count__gt=1)


    transaction_freq_atv = transaction_qs.aggregate(sum_value=Sum('value'), count_trx=Count('id'), count_user_id=Count('user_id', distinct = True))

    df = pd.DataFrame(transaction_avg_lifespan)
    df['delta_days'] = (df['last_trx'].dt.date - df['first_trx'].dt.date).dt.days
    df['delta_years'] = round(df['delta_days']/365)
    df['delta_years'] = df.apply(lambda x:get_delta_year(x.delta_years, x.trx_count), axis=1)

    atv = transaction_freq_atv['sum_value']/ transaction_freq_atv['count_trx']
    frequency = transaction_freq_atv['count_trx']/ transaction_freq_atv['count_user_id']
    avg_lifespan = df['delta_years'].mean()

    print(f'atv {prefix} : ', atv)
    print(f'frequency {prefix} : ', frequency)
    print(f'Avg Lifespan {prefix} : ', avg_lifespan)
    # print(df)

    return atv, frequency, avg_lifespan

    # return atv, frequency, avg_lifespan


# def get_statistics_by_ages(transaction_qs_above30):
#     atv_above30, frequency_above30, avg_lifespan_above30 = get_avg_lifespan_freq_atv(transaction_qs_above30, 'above30')

#     return atv_above30, frequency_above30


transaction_qs, transaction_qs_male, transaction_qs_female = get_transaction(date(2023, 4, 30))

transaction_qs_male_above30, transaction_qs_male_bw_18_30, transaction_qs_male_under18 = get_age_classification(transaction_qs_male)
transaction_qs_female_above30, transaction_qs_female_bw_18_30, transaction_qs_female_under18 = get_age_classification(transaction_qs_female)


# for end_date_quarter, prefix in end_date_quarter_list:
#     print(end_date_quarter, prefix)

atv_male_above30, frequency_male_above30, avg_lifespan_male_above30 = get_avg_lifespan_freq_atv(transaction_qs_male_above30,'male_above30')
atv_male_bw18_30, frequency_male_bw18_30, avg_lifespan_male_bw18_30 = get_avg_lifespan_freq_atv(transaction_qs_male_bw_18_30,'male_bw18_30')
atv_male_under18, frequency_male_under18, avg_lifespan_male_under18 = get_avg_lifespan_freq_atv(transaction_qs_male_under18, 'male_under18')

atv_female_above30, frequency_female_above30, avg_lifespan_female_above30 = get_avg_lifespan_freq_atv(transaction_qs_female_above30, 'female_above30')
atv_female_bw18_30, frequency_female_bw18_30, avg_lifespan_female_bw18_30 = get_avg_lifespan_freq_atv(transaction_qs_female_bw_18_30, 'female_bw18_3')
atv_female_under18, frequency_female_under188, avg_lifespan_female_under18 = get_avg_lifespan_freq_atv(transaction_qs_female_under18, 'female_under18')
