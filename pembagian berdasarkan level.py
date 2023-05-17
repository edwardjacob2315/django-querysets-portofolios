from datetime import datetime
from stamps.core.utils import prepare_datetime_range
from datetime import date
from dateutil.relativedelta import relativedelta

start_date = date(2022, 2, 28)
before_date = date(2023, 2, 26)

merchant = Merchant.objects.get(id=2)
members = merchant.group.memberships.filter(created__date__lte=before_date)
member_counts = members.count()

member_gold = members.filter(status=Membership.STATUS.gold)
member_silver = members.filter(status=Membership.STATUS.silver)
member_blue = members.filter(status=Membership.STATUS.blue)

gold_percent = member_gold.count()/member_counts*100
silver_percent = member_silver.count()/member_counts*100
blue_percent = member_blue.count()/member_counts*100

age_30 = before_date - relativedelta(years=30)

transactions = Transaction.objects.exclude(status=Transaction.STATUS.canceled)\
                                  .exclude(user_id=None)\
                                  .filter(created__range=prepare_datetime_range(start_date, before_date))


def get_member_pembagian(member_queryset, transactions):
# male_member = set(member_queryset.filter(user__profile__gender=Profile.GENDER.male).values_list('user_id', flat=True))
# female_member = set(member_queryset.filter(user__profile__gender=Profile.GENDER.male).values_list('user_id', flat=True))
# youth_member = set(member_queryset.filter(user__profile__birthday__gte=age_30).values_list('user_id', flat=True))

    all_member = set(member_queryset.values_list('user_id', flat=True))
    male_member = set(member_queryset.filter(user__profile__gender=Profile.GENDER.male).values_list('user_id', flat=True))
    female_member = set(member_queryset.filter(user__profile__gender=Profile.GENDER.female).values_list('user_id', flat=True))
    youth_member = set(member_queryset.filter(user__profile__birthday__gte=age_30).values_list('user_id', flat=True))

    all_member_count = len(all_member)
    male_member_count = len(male_member)
    female_member_count = len(female_member)
    youth_member_count = len(youth_member)


    male_member_percent = male_member_count/all_member_count*100
    female_member_percent = female_member_count/all_member_count*100
    youth_member_percent = youth_member_count/all_member_count*100

    print('male', male_member_percent)
    print('female', female_member_percent)
    print('youth', youth_member_percent)

    transactions_all = transactions.filter(user_id__in=all_member)
    transactions_male = transactions.filter(user_id__in=male_member)
    transactions_female = transactions.filter(user_id__in=female_member)
    transactions_youth = transactions.filter(user_id__in=youth_member)

    return all_member, male_member, female_member, youth_member, transactions_all,  transactions_male, transactions_female, transactions_youth



def get_transaction_statistic(transactions, key_prefix: str = "") -> dict:
    aggregates = transactions.aggregate(Sum('value'), Count('id'))
    aggregates2 = transactions.aggregate(Sum('items__quantity'))

    revenue = aggregates.get('value__sum') or 0
    transaction_count = aggregates.get('id__count') or 0
    item_count = aggregates2.get('items__quantity__sum') or 0
    atv = revenue/transaction_count
    upt = item_count/transaction_count


    return {
        f"{key_prefix}revenue": revenue,
        f"{key_prefix}transaction_count": transaction_count,
        f"{key_prefix}item_count": item_count,
        f"{key_prefix}atv": atv,
        f"{key_prefix}upt": upt
    }

print('gold')
gold_all_member, gold_male_member, gold_female_member, gold_youth_member, gold_transactions_all, gold_transactions_male, gold_transactions_female, gold_transactions_youth = get_member_pembagian(member_gold, transactions)
print('silver')
silver_all_member, silver_male_member, silver_female_member, silver_youth_member, silver_transactions_all, silver_transactions_male, silver_transactions_female, silver_transactions_youth = get_member_pembagian(member_silver, transactions)
print('blue')
blue_all_member, blue_male_member, blue_female_member, blue_youth_member, blue_transactions_male, blue_transactions_all, blue_transactions_female, blue_transactions_youth = get_member_pembagian(member_blue, transactions)


gold_trx_all = get_transaction_statistic(gold_transactions_all)
gold_trx_male = get_transaction_statistic(gold_transactions_male)
gold_trx_female = get_transaction_statistic(gold_transactions_female)
gold_trx_youth = get_transaction_statistic(gold_transactions_youth)


silver_trx_all = get_transaction_statistic(silver_transactions_all)
silver_trx_male = get_transaction_statistic(silver_transactions_male)
silver_trx_female = get_transaction_statistic(silver_transactions_female)
silver_trx_youth = get_transaction_statistic(silver_transactions_youth)


blue_trx_all = get_transaction_statistic(blue_transactions_all)
blue_trx_male = get_transaction_statistic(blue_transactions_male)
blue_trx_female = get_transaction_statistic(blue_transactions_female)
blue_trx_youth = get_transaction_statistic(blue_transactions_youth)


from datetime import date
from stamps.core.utils import prepare_datetime_range


def get_repeat_rate(user_ids):

    start_date = date(2022, 2, 28)
    end_date = date(2023, 2, 26)

    transactions = Transaction.objects.exclude(status=Transaction.STATUS.canceled)\
                                    .filter(user_id__in=user_ids)\
                                    .filter(created__range=prepare_datetime_range(start_date, end_date))

    trx_above = transactions.values('user_id').annotate(counts=Count('id')).order_by('counts')

    trx_above2 = trx_above.filter(counts__gte=2)
    # print('Active Member', len(trx_above))
    # print('Repeat member', len(trx_above2))

    try:
        percentage = (len(trx_above2)/len(trx_above) or 0 )*100
    except ZeroDivisionError:
        percentage = 0

    print('percentage',percentage)

    return percentage

print('gold rr')
gold_rr_all = get_repeat_rate(gold_all_member)
gold_rr_male = get_repeat_rate(gold_male_member)
gold_rr_female = get_repeat_rate(gold_female_member)
gold_rr_youth = get_repeat_rate(gold_youth_member)

print('silver rr')
silver_rr_all = get_repeat_rate(silver_all_member)
silver_rr_male = get_repeat_rate(silver_male_member)
silver_rr_female = get_repeat_rate(silver_female_member)
silver_rr_youth = get_repeat_rate(silver_youth_member)

print('blue rr')
blue_rr_all = get_repeat_rate(blue_all_member)
blue_rr_male = get_repeat_rate(blue_male_member)
blue_rr_female = get_repeat_rate(blue_female_member)
blue_rr_youth = get_repeat_rate(blue_youth_member)
