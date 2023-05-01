from datetime import date
from stamps.core.utils import prepare_datetime_range

retailers = ['retailer:AMP', 'retailer:LAU','retailer:KCS','retailer:SCU','retailer:MMR','retailer:PL','retailer:MDS','retailer:Ecommerce']

store_ids = set(Store.objects
                    .filter(tags__name__in=retailers)
                    .values_list('id', flat=True))

start_date = date(2021, 12, 1)
end_date = date(2022, 11, 30)

transactions = Transaction.objects.exclude(status=Transaction.STATUS.canceled)\
                                .exclude(user_id=None)\
                                .filter(store__id__in=store_ids)\
                                .filter(created__range=prepare_datetime_range(start_date, end_date))

trx_above = transactions.values('user_id').annotate(counts=Count('id')).order_by('counts')

trx_above2 = trx_above.filter(counts__gte=2)
print('Active Member', len(trx_above))
print('Repeat member', len(trx_above2))

try:
    percentage = (len(trx_above2)/len(trx_above) or 0 )*100
except ZeroDivisionError:
    percentage = 0

print('percentage',percentage)
