# JALANKAN DI LSCO REPORTING SERVER

import csv
from datetime import date
from dateutil.relativedelta import relativedelta
from lsco_reporting_server.core.utils import prepare_datetime_range
######################################################

date_lists =[
            #start_date, #end_date
            # [date(2021, 12, 1), date(2022, 1, 2)],
            # [date(2022, 1, 3), date(2022, 1, 30)],
            # [date(2022, 1, 31), date(2022, 2, 27)],
            # [date(2022, 2, 28), date(2022, 4, 3)],
            # [date(2022, 4, 4), date(2022, 5, 1)],
            # [date(2022, 5, 2), date(2022, 5, 29)],
            # [date(2022, 5, 30), date(2022, 7, 3)],
            # [date(2022, 7, 4), date(2022, 7, 31)],
            # [date(2022, 8, 1), date(2022, 8, 28)],
            # [date(2022, 8, 29), date(2022, 10, 2)],
            # [date(2022, 10, 3), date(2022, 10, 30)],
            # [date(2022, 10, 31), date(2022, 11, 30)],
            # [date(2022, 12, 1), date(2023, 1, 1)]
            [date(2023, 1, 30), date(2023, 2, 26)]
            ]


######################################################
merchant = StampsMerchant.objects.get(slug='levis-indonesia')

def get_transaction_statistic(transactions, key_prefix: str = "") -> dict:
    aggregates = transactions.aggregate(
        Sum('value'), Count('id'), Count('user_id', distinct=True))

    revenue = aggregates.get('value__sum') or 0
    transaction_count = aggregates.get('id__count') or 0


    return {
        f"{key_prefix}revenue": revenue,
        f"{key_prefix}transaction_count": transaction_count,

    }

def generate_transaction(start_date, end_date):

    age_30 = 30

    vip_store_ids = set(merchant.store_tags.get(name='VIP Club').stores.values_list('id', flat=True))

    online = set(StampsStoreTagValue.objects
                            .get(name='Ecommerce').stores
                            .values_list('id', flat=True))

    offline = vip_store_ids.difference(online)

    online_offline = vip_store_ids.union(online)

    transaction_qs_all = CHTransaction.objects.filter(merchant_id=merchant.id)\
        .exclude(status=CHTransaction.Status.CANCELED)\
        .filter(created__date__range=prepare_datetime_range(start_date, end_date))\
        .filter(store_id__in=online_offline)


    transaction_qs_online_all = transaction_qs_all.filter(store_id__in=online)

    transaction_qs_offline_all = transaction_qs_all.filter(store_id__in=offline)

    transaction_qs_member = CHTransaction.objects.filter(merchant_id=merchant.id)\
        .exclude(status=CHTransaction.Status.CANCELED)\
        .exclude(user_id=0)\
        .filter(store_id__in=online_offline)\
        .filter(created__date__range=[start_date, end_date])

    transaction_qs_online_member = transaction_qs_member.filter(store_id__in=online)

    transaction_qs_offline_member = transaction_qs_member.filter(store_id__in=offline)

    transaction_qs_non_member = CHTransaction.objects.filter(merchant_id=merchant.id)\
        .exclude(status=CHTransaction.Status.CANCELED)\
        .filter(user_id=0)\
        .filter(store_id__in=online_offline)\
        .filter(created__date__range=[start_date, end_date])

    transaction_qs_online_non_member = transaction_qs_non_member.filter(store_id__in=online)

    transaction_qs_offline_non_member = transaction_qs_non_member.filter(store_id__in=offline)




    #ONLINE #Member
    transaction_qs_online_women_youth = transaction_qs_online_member.filter(user_gender=StampsProfile.GENDER.female)\
                                                                    .filter(user_age__lte = age_30)\
                                                                    .exclude(user_age=0)\

    transaction_qs_online_women_non_youth = transaction_qs_online_member.filter(user_gender=StampsProfile.GENDER.female)\
                                                                        .filter(user_age__gt = age_30)\
                                                                        .exclude(user_age=0)\

    transaction_qs_online_men_youth = transaction_qs_online_member.filter(user_gender=StampsProfile.GENDER.male)\
                                                                  .filter(user_age__lte = age_30)\
                                                                  .exclude(user_age=0)

    transaction_qs_online_men_non_youth = transaction_qs_online_member.filter(user_gender=StampsProfile.GENDER.male)\
                                                                      .filter(user_age__gt = age_30)\
                                                                      .exclude(user_age=0)\

    transaction_qs_online_non_gender_youth = transaction_qs_online_member.filter(user_gender=0)\
                                                                         .filter(user_age__lte = age_30)\
                                                                         .exclude(user_age=0)\

    transaction_qs_online_non_gender_non_youth = transaction_qs_online_member.filter(user_gender=0)\
                                                                             .filter(user_age__gt = age_30)\
                                                                             .exclude(user_age=0)\

    transaction_qs_online_non_gender_no_age = transaction_qs_online_member.filter(user_gender=0)\
                                                                          .filter(user_age=0)\

    transaction_qs_online_male_no_age = transaction_qs_online_member.filter(user_gender=StampsProfile.GENDER.male)\
                                                                    .filter(user_age=0)

    transaction_qs_online_female_no_age = transaction_qs_online_member.filter(user_gender=StampsProfile.GENDER.female)\
                                                                      .filter(user_age=0)




    #OFFLINE   #Member
    transaction_qs_offline_women_youth = transaction_qs_offline_member.filter(user_gender=StampsProfile.GENDER.female)\
                                                                      .filter(user_age__lte = age_30)\
                                                                      .exclude(user_age=0)\

    transaction_qs_offline_women_non_youth = transaction_qs_offline_member.filter(user_gender=StampsProfile.GENDER.female)\
                                                                          .filter(user_age__gt = age_30)\
                                                                          .exclude(user_age=0)\

    transaction_qs_offline_men_youth = transaction_qs_offline_member.filter(user_gender=StampsProfile.GENDER.male)\
                                                                    .filter(user_age__lte = age_30)\
                                                                    .exclude(user_age=0)

    transaction_qs_offline_men_non_youth = transaction_qs_offline_member.filter(user_gender=StampsProfile.GENDER.male)\
                                                                        .filter(user_age__gt = age_30)\
                                                                        .exclude(user_age=0)\

    transaction_qs_offline_non_gender_youth = transaction_qs_offline_member.filter(user_gender=0)\
                                                                           .filter(user_age__lte = age_30)\
                                                                           .exclude(user_age=0)\

    transaction_qs_offline_non_gender_non_youth = transaction_qs_offline_member.filter(user_gender=0)\
                                                                               .filter(user_age__gt = age_30)\
                                                                               .exclude(user_age=0)\

    transaction_qs_offline_non_gender_no_age = transaction_qs_offline_member.filter(user_gender=0)\
                                                                            .filter(user_age=0)\

    transaction_qs_offline_male_no_age = transaction_qs_offline_member.filter(user_gender=StampsProfile.GENDER.male)\
                                                                      .filter(user_age=0)

    transaction_qs_offline_female_no_age = transaction_qs_offline_member.filter(user_gender=StampsProfile.GENDER.female)\
                                                                        .filter(user_age=0)




    data = dict()

    data.update({"Start Date":start_date, "End Date":end_date})
    data.update(get_transaction_statistic(transaction_qs_all, "total_all_"))
    data.update(get_transaction_statistic(transaction_qs_online_all, "total_online_all_"))
    data.update(get_transaction_statistic(transaction_qs_offline_all, "total_offline_all_"))
    data.update(get_transaction_statistic(transaction_qs_member, "total_member_"))
    data.update(get_transaction_statistic(transaction_qs_online_member, "total_online_member_"))
    data.update(get_transaction_statistic(transaction_qs_offline_member, "total_offline_member_"))

    data.update(get_transaction_statistic(transaction_qs_online_non_member, "total_online_nm_member_"))
    data.update(get_transaction_statistic(transaction_qs_offline_non_member, "total_offline_nm_member_"))


    data.update(get_transaction_statistic(transaction_qs_online_women_youth, "online_women_youth_"))
    data.update(get_transaction_statistic(transaction_qs_online_women_non_youth, "online_women_non_youth_"))
    data.update(get_transaction_statistic(transaction_qs_online_men_youth, "online_men_youth_"))
    data.update(get_transaction_statistic(transaction_qs_online_men_non_youth, "online_men_non_youth_"))
    data.update(get_transaction_statistic(transaction_qs_online_non_gender_youth, "online_non_gender_youth_"))
    data.update(get_transaction_statistic(transaction_qs_online_non_gender_non_youth, "online_non_gender_non_youth_"))
    data.update(get_transaction_statistic(transaction_qs_offline_women_youth, "offline_women_youth_"))
    data.update(get_transaction_statistic(transaction_qs_offline_women_non_youth, "offline_women_non_youth_"))
    data.update(get_transaction_statistic(transaction_qs_offline_men_youth, "offline_men_youth_"))
    data.update(get_transaction_statistic(transaction_qs_offline_men_non_youth, "offline_men_non_youth_"))
    data.update(get_transaction_statistic(transaction_qs_offline_non_gender_youth, "offline_non_gender_youth_"))
    data.update(get_transaction_statistic(transaction_qs_offline_non_gender_non_youth, "offline_non_gender_non_youth_"))

    data.update(get_transaction_statistic(transaction_qs_online_non_gender_no_age, "online_non_gender_no_age_"))
    data.update(get_transaction_statistic(transaction_qs_offline_non_gender_no_age, "offline_non_gender_no_age_"))

    data.update(get_transaction_statistic(transaction_qs_online_male_no_age, "online_male_no_age_"))
    data.update(get_transaction_statistic(transaction_qs_online_female_no_age, "online_female_no_age_"))

    data.update(get_transaction_statistic(transaction_qs_offline_male_no_age, "offline_male_no_age_"))
    data.update(get_transaction_statistic(transaction_qs_offline_female_no_age, "offline_female_no_age_"))


    data.update({'% Online revenue all':(data['total_online_all_revenue']/data['total_all_revenue']*100)})
    data.update({'% Offline revenue all':(data['total_offline_all_revenue']/data['total_all_revenue']*100)})
    data.update({'% Online revenue member':(data['total_online_member_revenue']/data['total_online_all_revenue']*100)})
    data.update({'% Offline revenue member':(data['total_offline_member_revenue']/data['total_offline_all_revenue']*100)})
    data.update({'% Online revenue non member':(data['total_online_nm_member_revenue']/data['total_online_all_revenue']*100)})
    data.update({'% Offline revenue non member':(data['total_offline_nm_member_revenue']/data['total_offline_all_revenue']*100)})



    data.update({'% Online Women Youth revenue':(data['online_women_youth_revenue'] / data['total_online_member_revenue']*100)})
    data.update({'% Online Women Non Youth revenue':((data['online_women_non_youth_revenue'] + data['online_female_no_age_revenue']) / data['total_online_member_revenue']*100)})
    data.update({'% Online Men Youth revenue':((data['online_men_youth_revenue'] + data['online_non_gender_youth_revenue']) / data['total_online_member_revenue']*100)})
    data.update({'% Online Men Non Youth revenue':((data['online_men_non_youth_revenue'] + data['online_non_gender_non_youth_revenue'] + data['online_non_gender_no_age_revenue'] + data['online_male_no_age_revenue'])\
                                                    / data['total_online_member_revenue'] * 100)})


    data.update({'% Offline Women Youth revenue':(data['offline_women_youth_revenue']/data['total_offline_member_revenue']*100)})
    data.update({'% Offline Women Non Youth revenue':((data['offline_women_non_youth_revenue'] + data['offline_female_no_age_revenue']) / data['total_offline_member_revenue']*100)})
    data.update({'% Offline Men Youth revenue':((data['offline_men_youth_revenue'] + data['offline_non_gender_youth_revenue']) / data['total_offline_member_revenue']*100)})
    data.update({'% Offline Men Non Youth revenue':((data['offline_men_non_youth_revenue'] + data['offline_non_gender_non_youth_revenue'] + data['offline_non_gender_no_age_revenue'] + data['offline_male_no_age_revenue'])\
                                                    / data['total_offline_member_revenue'] * 100)})




    data.update({'% Online trx all':(data['total_online_all_transaction_count'] / data['total_all_transaction_count']*100)})
    data.update({'% Offline trx all':(data['total_offline_all_transaction_count'] / data['total_all_transaction_count']*100)})
    data.update({'% Online trx member':(data['total_online_member_transaction_count'] / data['total_online_all_transaction_count']*100)})
    data.update({'% Offline trx member':(data['total_offline_member_transaction_count'] / data['total_offline_all_transaction_count']*100)})
    data.update({'% Online trx non member':(data['total_online_nm_member_transaction_count'] / data['total_online_all_transaction_count']*100)})
    data.update({'% Offline trx non member':(data['total_offline_nm_member_transaction_count'] / data['total_offline_all_transaction_count']*100)})


    data.update({'% Online Women Youth trx':(data['online_women_youth_transaction_count'] / data['total_online_member_transaction_count']*100)})
    data.update({'% Online Women Non Youth trx':((data['online_women_non_youth_transaction_count'] + data['online_female_no_age_transaction_count']) / data['total_online_member_transaction_count']*100)})
    data.update({'% Online Men Youth trx':((data['online_men_youth_transaction_count'] + data['online_non_gender_youth_transaction_count']) / data['total_online_member_transaction_count']*100)})
    data.update({'% Online Men Non Youth trx':((data['online_men_non_youth_transaction_count'] + data['online_non_gender_non_youth_transaction_count'] + data['online_non_gender_no_age_transaction_count'] + data['online_male_no_age_transaction_count']) / data['total_online_member_transaction_count']*100)})


    data.update({'% Offline Women Youth trx':(data['offline_women_youth_transaction_count'] / data['total_offline_member_transaction_count']*100)})
    data.update({'% Offline Women Non Youth trx':((data['offline_women_non_youth_transaction_count'] + data['offline_female_no_age_transaction_count'])/ data['total_offline_member_transaction_count']*100)})
    data.update({'% Offline Men Youth trx':((data['offline_men_youth_transaction_count'] + data['offline_non_gender_youth_transaction_count']) / data['total_offline_member_transaction_count']*100)})
    data.update({'% Offline Men Non Youth trx':((data['offline_men_non_youth_transaction_count'] + data['offline_non_gender_non_youth_transaction_count'] + data['offline_non_gender_no_age_transaction_count'] + + data['offline_male_no_age_transaction_count']) / data['total_offline_member_transaction_count']*100)})


    return data

for start_date, end_date in date_lists:

    result = generate_transaction(start_date, end_date)

    with open(f'/home/bintang/crm-kpi-new-format-{start_date}-{end_date}.csv', 'w') as output:
        writer = csv.DictWriter(output, result.keys())
        writer.writeheader()
        writer.writerow(result)
