from datetime import date

dates = [
        #start_date ,end_date
        [date(2021, 12, 1), date(2022, 11, 30)],
        [date(2020, 12, 1), date(2021, 11, 30)],
        [date(2022, 10, 31), date(2022, 11, 30)],
        [date(2021, 11, 1), date(2021, 11, 30)]
]

def check_data_member(start_date, end_date):
        merchant_slug = "levis-indonesia"
        merchant = Merchant.objects.get(slug=merchant_slug)

        membership = Membership.objects.filter(merchant_group=merchant.group)\
                                        .filter(created__date__range =[start_date, end_date])\
                                        .filter(user__has_incorrect_email=False)\
                                        .filter(allow_newsletters=True)
        total = len(membership)
        return total

for start_date, end_date in dates:
        hasil = check_data_member(start_date, end_date)
        print(hasil)
