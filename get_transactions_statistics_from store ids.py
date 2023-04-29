import csv
from stamps.core.utils import prepare_datetime_range
from dateutil.relativedelta import relativedelta
from datetime import date
import pandas as pd

stores_lists = [203, 206, 364, 199, 369, 332, 415, 612, 793, 378, 388, 200, 16]

start_date = date(2020, 12, 1)
end_date = date(2021, 1, 3)

age_30 = end_date - relativedelta(years=30)

store_mapping = dict()
store_all = Store.objects.all()

for store in store_all:
    store_mapping[store.id] = store.display_name


def get_transaction_statistic(transactions, key_prefix: str = "") -> dict:
    aggregates = transactions.aggregate(
        Sum('value'), Count('id'), Count('user_id', distinct=True))

    aggregates_item = transactions.aggregate(Sum('items__quantity'))

    revenue = aggregates.get('value__sum') or 0
    transaction_count = aggregates.get('id__count') or 0
    unit = aggregates_item.get("items__quantity__sum") or 0

    try:
        atv = revenue /transaction_count
    except ZeroDivisionError:
        atv = 0

    try:
        upt = unit / transaction_count
    except ZeroDivisionError:
        upt = 0

    return {
        f"{key_prefix}_revenue": revenue,
        f"{key_prefix}_transaction_count": transaction_count,
        f"{key_prefix}_unit_count": unit,
        f"{key_prefix}_atv": atv,
        f"{key_prefix}_upt": upt

    }


def calculate_transactions(start_date, end_date, store_id):


    transactions = Transaction.objects.filter(store_id=store_id)\
                                    .exclude(status=Transaction.STATUS.canceled)\
                                    .filter(created__range=prepare_datetime_range(start_date, end_date))

    transactions_men = transactions.filter(user__profile__gender = Profile.GENDER.male)
    transactions_women = transactions.filter(user__profile__gender = Profile.GENDER.female)
    transactions_youth = transactions.filter(user__profile__birthday__gte = age_30)



    data = dict()

    data.update({"Start Date":start_date, "End Date":end_date})
    data.update(get_transaction_statistic(transactions, "total_all"))
    data.update(get_transaction_statistic(transactions_men, "total_men"))
    data.update(get_transaction_statistic(transactions_women, "total_women"))
    data.update(get_transaction_statistic(transactions_youth, "total_youth"))


    return data


def write_csv():
    with open(f'/home/bintang/transactions_per_store-{start_date}-{end_date}.csv', 'w') as output:
        header = [
            "Start Date",
            "End Date",
            'store_id',
            'store_name',
            "total_all_revenue",
            "total_all_transaction_count",
            "total_all_unit_count",
            "total_all_atv",
            "total_all_upt",
            "total_men_revenue",
            "total_men_transaction_count",
            "total_men_unit_count",
            "total_men_atv",
            "total_men_upt",
            "total_women_revenue",
            "total_women_transaction_count",
            "total_women_unit_count",
            "total_women_atv",
            "total_women_upt",
            "total_youth_revenue",
            "total_youth_transaction_count",
            "total_youth_unit_count",
            "total_youth_atv",
            "total_youth_upt"
        ]
        writer = csv.DictWriter(output, fieldnames=header)
        writer.writeheader()

        for store_id in stores_lists:

            result = calculate_transactions(start_date, end_date, store_id)

            writer.writerow({
                'Start Date': result['Start Date'],
                'End Date': result['End Date'],
                'store_id': store_id,
                'store_name': store_mapping.get(store_id),
                "total_all_revenue": result['total_all_revenue'],
                "total_all_transaction_count": result['total_all_transaction_count'],
                "total_all_unit_count": result['total_all_unit_count'],
                "total_all_atv": result['total_all_atv'],
                "total_all_upt": result['total_all_upt'],
                "total_men_revenue": result['total_men_revenue'],
                "total_men_transaction_count": result['total_men_transaction_count'],
                "total_men_unit_count": result['total_men_unit_count'],
                "total_men_atv": result['total_men_atv'],
                "total_men_upt": result['total_men_upt'],
                "total_women_revenue": result['total_women_revenue'],
                "total_women_transaction_count": result['total_women_transaction_count'],
                "total_women_unit_count": result['total_women_unit_count'],
                "total_women_atv": result['total_women_atv'],
                "total_women_upt": result['total_women_upt'],
                "total_youth_revenue": result['total_youth_revenue'],
                "total_youth_transaction_count": result['total_youth_transaction_count'],
                "total_youth_unit_count": result['total_youth_unit_count'],
                "total_youth_atv": result['total_youth_atv'],
                "total_youth_upt": result['total_youth_upt']
            })

write_csv()



























