import csv
from datetime import date
from dateutil.relativedelta import relativedelta
from stamps.core.utils import prepare_datetime_range

#PART 1 (BAGIAN ATAS)

merchant = Merchant.objects.get(slug='levis-indonesia')

start_date = date(2022, 10, 3)
end_date = date(2022, 10, 30)

age_30 = end_date - relativedelta(years=30)

# retailers = ['AMP', 'LAU', 'KCS', 'SCU', 'MMR']
# # retailers = ['MDS']
# retailers = ['PL']
retailers = ['Ecommerce']

labels_filter_male = [
                      #category, #labels_lists
                      ['mens-top', ['category:MENS TOP', 'category:TOPS-MEN','sub-category:TOPS-MEN', 'line:TOPS-MEN']],
                      ['mens-bottom', ['category:MENS BOTTOM','category:BOTTOMS-MEN','sub-category:BOTTOMS-MEN', 'line:BOTTOMS-MEN']],
                      ['mens-acc',['category:MENS ACCESSORIES', 'category:ACCESSORIES-MEN', 'sub-category:ACCESSORIES-MEN', 'line:ACCESSORIES-MEN']]
                    ]

labels_filter_female = [
                        #category, #labels_lists
                        ['womens-top', ['category:WOMENS TOP', 'category:TOPS-WOMEN', 'sub-category:TOPS-WOMEN','line:TOPS-WOMEN']],
                        ['womens-bottom', ['category:WOMENS BOTTOM','category:BOTTOMS-WOMEN','sub-category:BOTTOMS-WOMEN','line:BOTTOMS-WOMEN']],
                        ['womens-acc',['category:WOMENS ACCESSORIES', 'category:ACCESSORIES-WOMEN','sub-category:ACCESSORIES-WOMEN','line:ACCESSORIES-WOMEN']]
                        ]

store_ids = set(StoreTagValue.objects.filter(group__merchant=merchant)
                             .filter(group__name='Retailer')
                             .filter(name__in=retailers)
                             .values_list('stores', flat=True))

vip_stores = set(merchant.store_tags.filter(name='VIP Club').values_list('stores', flat=True))
#city_stores = set(merchant.store_tags.filter(name__istartswith="city").values_list('stores', flat=True))

store_ids=store_ids.intersection(vip_stores)

transactions = merchant.transactions\
                       .filter(created__range=prepare_datetime_range(start_date, end_date))\
                       .exclude(status=Transaction.STATUS.canceled)\
                       .filter(store_id__in=store_ids)\

aggregate = transactions.aggregate(rev=Sum('value'),trx=Count('id',distinct= True))

print('Revenue All', aggregate['rev'])
print('Transactions All', aggregate['trx'])

transactions_30below = transactions.filter(user__profile__birthday__gt=age_30)
transactions_30above = transactions.filter(user__profile__birthday__lte=age_30)
transactions_noage = transactions.exclude(id__in=transactions_30above)\
                                  .exclude(id__in=transactions_30below)

trx_30below_male = transactions_30below.filter(user__profile__gender=Profile.GENDER.male)
trx_30below_female = transactions_30below.filter(user__profile__gender=Profile.GENDER.female)
trx_30below_nogender = transactions_30below.filter(user__profile__gender__isnull=True)

trx_30above_male = transactions_30above.filter(user__profile__gender=Profile.GENDER.male)
trx_30above_female = transactions_30above.filter(user__profile__gender=Profile.GENDER.female)
trx_30above_nogender = transactions_30above.filter(user__profile__gender__isnull=True)

trx_noage_male = transactions_noage.filter(user__profile__gender=Profile.GENDER.male)
trx_noage_female = transactions_noage.filter(user__profile__gender=Profile.GENDER.female)
trx_noage_nogender = transactions_noage.filter(user__profile__gender__isnull=True)\
                                       .filter(user_id__isnull=False)

trx_nonmember = transactions_noage.filter(user_id__isnull=True)

def get_revenue_trx(transaction_query, prefix):
    aggregates = transaction_query.aggregate(trx=Count('id', distinct=True),
                                                revenue=Sum('value'), atv=Avg('value'))

    transaction_count = aggregates.get('trx') or 0
    revenue = aggregates.get('revenue') or 0

    print('Revenue', revenue, 'Transaction_count', transaction_count)
    print(prefix)
    print('BATAS HERE')
    return revenue, transaction_count

all_transactions = [
                   #transaction_queryset #prefix
                   [trx_30below_female, 'below30_female'],
                   [trx_30below_male, 'below30_male'],
                   [trx_30below_nogender, 'below30 no gender'],
                   [trx_30above_female, 'above30_female'],
                   [trx_30above_male, 'above30_male'],
                   [trx_30above_nogender, 'above30 no gender'],
                   [trx_noage_female, 'no age female'],
                   [trx_noage_male, 'no age male'],
                   [trx_noage_nogender, 'no age no gender member'],
                   [trx_nonmember, 'non member']
]


for transaction_queryset, prefix in all_transactions:
    get_revenue_trx(transaction_queryset, prefix)



# PART 2 (BAGIAN BAWAH)


transactions_list = [
                #transaction_queryset #prefix #labels_filter
                [trx_30below_male, 'below30_male', labels_filter_male],
                [trx_30above_male, 'above30_male', labels_filter_male],
                [trx_30below_female, 'below30_female', labels_filter_female],
                [trx_30above_female, 'above30_female', labels_filter_female]
               ]


def write_pc9_data(transaction_queryset, prefix, labels_filter):

    for category, labels_lists in labels_filter:

        product_ids_all = set()

        for label in labels_lists:

            product_ids = set(Product.objects.filter(labels__name__icontains=label).values_list('id', flat=True))
            product_ids_all.update(product_ids)


        items_aggregates = Item.objects.filter(product_id__in=product_ids_all)\
                                    .filter(transaction__in=transaction_queryset)\
                                    .values('product__name', 'product__display_name', 'product__id')\
                                    .annotate(revenue=Sum('price')
                                    ,trx=Count('transaction__id', distinct=True)
                                    ,item_count=Sum('quantity'))\
                                    .order_by('-trx')

        with open(f'/home/bintang/pc9/product_item_count-{category}_{prefix}_{retailers}.csv', 'w') as output:
            header = ['product_name',
                    'product_display_name',
                    'revenue',
                    'transaction_count',
                    'item_count',
                    'product_id']

            writer = csv.DictWriter(output, fieldnames=header)
            writer.writeheader()

            for product in items_aggregates:
                product_name = product.get('product__name')
                product_display_name = product.get('product__display_name')
                product_id = product.get('product__id')
                product_revenue = product.get('revenue') or 0
                product_transaction = product.get('trx') or 0
                product_count = product.get('item_count') or 0

                writer.writerow({
                                'product_name': product_name,
                                'product_display_name': product_display_name,
                                'revenue': product_revenue,
                                'transaction_count': product_transaction,
                                'item_count': product_count,
                                'product_id': product_id
                                })

for transaction_queryset, prefix, labels_filter in transactions_list:
    write_pc9_data(transaction_queryset, prefix, labels_filter)
