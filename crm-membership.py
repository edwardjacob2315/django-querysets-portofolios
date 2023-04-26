import csv
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from stamps.core.utils import prepare_datetime_range
######################################################

merchant_slug = "levis-indonesia"


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
            # [date(2021, 12, 1), date(2022, 11, 30)]
            [date(2023, 1, 2), date(2023, 1, 29)]
            ]


######################################################


def get_stats(membership_qs, prefix):
    count_member_id = membership_qs.aggregate(Count('id', distinct=True))['id__count'] or 0

    return {
        f"{prefix}_member": count_member_id
    }


def generate_transaction(start_date, end_date):

    merchant = Merchant.objects.get(slug=merchant_slug)

    start_date, end_date = prepare_datetime_range(start_date, end_date)


    age_30 = end_date - relativedelta(years=30)

    #Member
    membership_qs = Membership.objects.filter(merchant_group=merchant.group)\
                                        .filter(created__date__lte=end_date)\

    total_member_women_qs_youth = membership_qs.filter(user__profile__gender=Profile.GENDER.female)\
                                            .filter(user__profile__birthday__gte = age_30)

    total_member_men_qs_youth = membership_qs.filter(user__profile__gender=Profile.GENDER.male)\
                                        .filter(user__profile__birthday__gte = age_30)

    total_member_no_gender_qs_youth = membership_qs.filter(user__profile__gender__isnull=True)\
                                        .filter(user__profile__birthday__gte = age_30)

    total_member_women_qs_non_youth = membership_qs.filter(user__profile__gender=Profile.GENDER.female)\
                                            .filter(user__profile__birthday__lt=age_30)

    total_member_men_qs_non_youth = membership_qs.filter(user__profile__gender=Profile.GENDER.male)\
                                        .filter(user__profile__birthday__lt=age_30)

    total_member_no_gender_qs_non_youth = membership_qs.filter(user__profile__gender__isnull=True)\
                                        .filter(user__profile__birthday__lt=age_30)

    total_member_no_gender_qs_no_age = membership_qs.filter(user__profile__gender__isnull=True)\
                                    .filter(user__profile__birthday__isnull=True)

    total_member_women_qs_no_age = membership_qs.filter(user__profile__gender=Profile.GENDER.female)\
                                            .filter(user__profile__birthday__isnull=True)

    total_member_men_qs_no_age = membership_qs.filter(user__profile__gender=Profile.GENDER.male)\
                                        .filter(user__profile__birthday__isnull=True)






    #New Member
    new_membership_qs = membership_qs.filter(created__date__gte=start_date)

    new_membership_women_youth = new_membership_qs.filter(user__profile__gender=Profile.GENDER.female)\
                                            .filter(user__profile__birthday__gte = age_30)

    new_membership_men_youth = new_membership_qs.filter(user__profile__gender=Profile.GENDER.male)\
                                            .filter(user__profile__birthday__gte = age_30)

    new_membership_no_gender_youth = new_membership_qs.filter(user__profile__gender__isnull=True)\
                                            .filter(user__profile__birthday__gte = age_30)

    new_membership_women_non_youth = new_membership_qs.filter(user__profile__gender=Profile.GENDER.female)\
                                            .filter(user__profile__birthday__lt=age_30)

    new_membership_men_non_youth = new_membership_qs.filter(user__profile__gender=Profile.GENDER.male)\
                                            .filter(user__profile__birthday__lt=age_30)

    new_membership_no_gender_non_youth = new_membership_qs.filter(user__profile__gender__isnull=True)\
                                            .filter(user__profile__birthday__lt=age_30)


    new_member_no_gender_qs_no_age = new_membership_qs.filter(user__profile__gender__isnull=True)\
                                    .filter(user__profile__birthday__isnull=True)

    new_member_women_qs_no_age = new_membership_qs.filter(user__profile__gender=Profile.GENDER.female)\
                                            .filter(user__profile__birthday__isnull=True)

    new_member_men_qs_no_age = new_membership_qs.filter(user__profile__gender=Profile.GENDER.male)\
                                        .filter(user__profile__birthday__isnull=True)


    data = dict()

    data.update({"start_date": start_date, "end_date": end_date})

    data.update(get_stats(membership_qs, "total"))
    data.update(get_stats(total_member_women_qs_youth, "total_women_youth"))
    data.update(get_stats(total_member_men_qs_youth, "total_men_youth"))
    data.update(get_stats(total_member_no_gender_qs_youth, "total_no_gender_youth"))
    data.update(get_stats(total_member_women_qs_non_youth, "total_women_non_youth"))
    data.update(get_stats(total_member_men_qs_non_youth, "total_men_non_youth"))
    data.update(get_stats(total_member_no_gender_qs_non_youth, "total_no_gender_non_youth"))
    data.update(get_stats(total_member_no_gender_qs_no_age, "total_no_gender_no_age"))
    data.update(get_stats(total_member_women_qs_no_age, "total_women_no_age"))
    data.update(get_stats(total_member_men_qs_no_age, "total_men_no_age"))


    data.update(get_stats(new_membership_qs, "total_new"))
    data.update(get_stats(new_membership_women_youth, "total_women_new_youth"))
    data.update(get_stats(new_membership_men_youth, "total_men_new_youth"))
    data.update(get_stats(new_membership_no_gender_youth, "total_no_gender_new_youth"))
    data.update(get_stats(new_membership_women_non_youth, "total_women_new_non_youth"))
    data.update(get_stats(new_membership_men_non_youth, "total_men_new_non_youth"))
    data.update(get_stats(new_membership_no_gender_non_youth, "total_no_gender_new_non_youth"))
    data.update(get_stats(new_member_no_gender_qs_no_age, "total_no_gender_new_no_age"))
    data.update(get_stats(new_member_women_qs_no_age, "total_women_new_no_age"))
    data.update(get_stats(new_member_men_qs_no_age, "total_no_men_new_no_age"))


    data.update({'% all_member_women_youth': data['total_women_youth_member']/data['total_member']*100})
    data.update({'% all_member_women_non_youth': (data['total_women_non_youth_member'] +  data['total_women_no_age_member'])/data['total_member']*100})
    data.update({'% all_member_men_youth': (data['total_men_youth_member'] + data['total_no_gender_youth_member']) /data['total_member']*100})
    data.update({'% all_member_men_non_youth': (data['total_men_non_youth_member'] + data['total_no_gender_non_youth_member'] + data['total_no_gender_no_age_member'] + data['total_men_no_age_member'])/data['total_member']*100})


    data.update({'% new_member_women_youth': data['total_women_new_youth_member']/data['total_new_member']*100})
    data.update({'% new_member_women_non_youth': (data['total_women_new_non_youth_member'] + data['total_women_new_no_age_member'])/data['total_new_member']*100})
    data.update({'% new_member_men_youth': (data['total_men_new_youth_member'] + data['total_no_gender_new_youth_member'])/data['total_new_member']*100})
    data.update({'% new_member_men_non_youth': (data['total_men_new_non_youth_member'] + data['total_no_gender_new_non_youth_member'] + data['total_no_gender_new_no_age_member'] + data['total_no_men_new_no_age_member'])/data['total_new_member']*100})

    return data


for start_date, end_date in date_lists:

    result = generate_transaction(start_date, end_date)

    with open(f'/home/bintang/crm-kpi-new-membership_count-{start_date}-{end_date}-ya.csv', 'w') as output:
        writer = csv.DictWriter(output, result.keys())
        writer.writeheader()
        writer.writerow(result)
