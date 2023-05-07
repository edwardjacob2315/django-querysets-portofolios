from datetime import date
from stamps.core.utils import prepare_datetime_range

start_date = date(2022, 2, 28)
end_date = date(2023, 2, 26)

retailers_ols = ['AMP', 'LAU', 'KCS', 'SCU', 'MMR']
retailers_mds = ['MDS']
retailers_pl = ['PL']
retailers_ecomm = ['Ecommerce']


merchant = Merchant.objects.get(id=2)
members = merchant.group.memberships.all()
member_counts = members.count()

member_gold = set(members.filter(status=Membership.STATUS.gold).values_list('user_id', flat=True))

transaction_stamps = merchant.transactions.exclude(user_id=None)\
                             .exclude(status=Transaction.STATUS.canceled)\
                             .filter(created__range=prepare_datetime_range(start_date, end_date))\
                             .filter(user_id__in=member_gold)\

sum_trx_spend = transaction_stamps.aggregate(sum_spending=Sum('value'))['sum_spending']
avg_per_gold = sum_trx_spend/len(member_gold)


supergold_limit = avg_per_gold * 119/100

print('average_trx_spend', avg_per_gold)
print('super_gold_limit', supergold_limit)




def get_retailer_store(retailer_list):
    store_ids = set(StoreTagValue.objects.filter(group__merchant=merchant)
                             .filter(group__name='Retailer')
                             .filter(name__in=retailer_list)
                             .values_list('stores', flat=True))

    return store_ids


def annotate_transaction(transaction_qs):
    trx_annotate = transaction_qs.values('user_id').annotate(average_spending=Avg('value'), sum_spending=Sum('value'))\
                                 .order_by('-sum_spending')

    return trx_annotate


def get_statistics(trx_annotated):
    count_user = len(trx_annotated)
    sum_spending = sum(data['sum_spending'] for data in trx_annotated)

    return count_user, sum_spending



store_ols = get_retailer_store(retailers_ols)
store_mds = get_retailer_store(retailers_mds)
store_pl = get_retailer_store(retailers_pl)
store_ecomm = get_retailer_store(retailers_ecomm)


transaction_all_retailer = transaction_stamps
transaction_ols = transaction_stamps.filter(store_id__in=store_ols)
transaction_mds = transaction_stamps.filter(store_id__in=store_mds)
transaction_pl = transaction_stamps.filter(store_id__in=store_pl)
transaction_ecomm = transaction_stamps.filter(store_id__in=store_ecomm)


trx_annt_all = annotate_transaction(transaction_all_retailer).filter(average_spending__gt=supergold_limit)
trx_annt_ols = annotate_transaction(transaction_ols).filter(average_spending__gt=supergold_limit)
trx_annt_mds = annotate_transaction(transaction_mds).filter(average_spending__gt=supergold_limit)
trx_annt_pl = annotate_transaction(transaction_pl).filter(average_spending__gt=supergold_limit)
trx_annt_ecomm = annotate_transaction(transaction_ecomm).filter(average_spending__gt=supergold_limit)


user_all, spending_all = get_statistics(trx_annt_all)
user_ols, spending_ols = get_statistics(trx_annt_ols)
user_mds, spending_mds = get_statistics(trx_annt_mds)
user_pl, spending_pl = get_statistics(trx_annt_pl)
user_ecomm, spending_ecomm = get_statistics(trx_annt_ecomm)

print('trx_all', user_all )
print('trx_ols', user_ols )
print('trx_mds', user_mds )
print('trx_pl', user_pl )
print('trx_ecomm', user_ecomm )

