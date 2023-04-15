import boto3
import csv
import zipfile
from io import BytesIO, StringIO

from django.conf import settings

from stamps.apps.newsletters.models import Newsletter
import csv
import datetime

from datetime import date

from django.utils import timezone

from dateutil.relativedelta import relativedelta
from stamps.core.ses_utils import get_event_count
from stamps.core.utils import prepare_datetime_range

# # EMAIL_STATISTIC_INPUTS = [1770, 1771, 1779, 1784, 1785, 1794, 1797, 1804, 1805, 1806]
# EMAIL_STATISTIC_INPUTS = [1750, 1759, 1760, 1763, 1764, 1765]
EMAIL_STATISTIC_INPUTS = [1770]

merchant = Merchant.objects.get(slug='levis-indonesia')

ols_retailers = ['retailer:KCS', 'retailer:SCU',
                 'retailer:LAU', 'retailer:MMR','retailer:AMP']
mds_retailers = ['retailer:MDS']
pl_retailers = ['retailer:PL']
ecomm_retailers = ['retailer:Ecommerce']

vip_store_ids = merchant.stores\
                        .filter(tags__name='VIP Club')\
                        .values_list('id', flat=True)

ols_store_ids = set(vip_store_ids\
                    .filter(tags__name__in=ols_retailers)
                    .values_list('id', flat=True))

mds_store_ids = set(vip_store_ids\
                    .filter(tags__name__in=mds_retailers)
                    .values_list('id', flat=True))

pl_store_ids = set(vip_store_ids\
                   .filter(tags__name__in=pl_retailers)
                   .values_list('id', flat=True))

ecomm_store_ids = set(vip_store_ids
                      .filter(tags__name__in=ecomm_retailers)
                      .values_list('id', flat=True))

all_store_ids = ols_store_ids\
    .union(mds_store_ids)\
    .union(pl_store_ids)\
    .union(ecomm_store_ids)


def get_status_from_unix_filter(status_reader, cut_off_date):

    email_set = set()

    next(status_reader)
    for row in status_reader:
        email = row[0]
        unix_time = int(float(row[1]))

        converted_unix = datetime.datetime.fromtimestamp(unix_time, datetime.timezone(datetime.timedelta(hours=7))).date()

        if converted_unix < cut_off_date:
            email_set.add(email)

    return email_set


def get_email_statistics_transactions(newsletter_id, range_days):

    newsletter = Newsletter.objects.get(id=newsletter_id)
    newsletter_start_date = timezone.localtime(newsletter.scheduled_time).date()
    newsletter_subject = newsletter.subject

    cut_off_date = newsletter_start_date + relativedelta(days=range_days) - relativedelta(days=1)

    kwargs = {
        'aws_access_key_id': settings.AWS_ACCESS_KEY_ID,
        'aws_secret_access_key': settings.AWS_SECRET_ACCESS_KEY,
        'region_name': settings.AWS_REGION,
    }

    s3 = boto3.client('s3', **kwargs)

    s3_object = s3.get_object(
        Bucket=settings.AWS_STORAGE_BUCKET_NAME,
        Key=newsletter.amazon_s3_key)

    zip_object = zipfile.ZipFile(BytesIO(s3_object['Body'].read()))

    send = zip_object.read('Send-event.csv')
    open_zip = zip_object.read('Open-event.csv')
    click = zip_object.read('Click-event.csv')
    bounce = zip_object.read('Bounce-event.csv')
    reject = zip_object.read('Reject-event.csv')

    send_reader = csv.reader(StringIO(send.decode()))
    open_reader = csv.reader(StringIO(open_zip.decode()))
    click_reader = csv.reader(StringIO(click.decode()))
    bounce_reader = csv.reader(StringIO(bounce.decode()))
    reject_reader = csv.reader(StringIO(reject.decode()))

    send_set = get_status_from_unix_filter(send_reader, cut_off_date)
    open_set = get_status_from_unix_filter(open_reader, cut_off_date)
    click_set = get_status_from_unix_filter(click_reader, cut_off_date)
    bounce_set = get_status_from_unix_filter(bounce_reader, cut_off_date)
    reject_set = get_status_from_unix_filter(reject_reader, cut_off_date)
    delivered_set = set(send_set).difference(reject_set).difference(bounce_set)


    return send_set, open_set, click_set, bounce_set, delivered_set, newsletter_start_date, cut_off_date, newsletter_subject


def filter_store_and_agg_trx(transactions_query, store_ids):

    transactions_filtered = transactions_query\
    .filter(store__in=store_ids)\
    .aggregate(revenue=Sum('value'),
                trx_count=Count('id', distinct=True),
                unique_responder=Count('user_id', distinct=True)
                )

    return transactions_filtered


def get_transaction(user_email_set, start_date, end_date):

    user_ids = set(User.objects
                       .filter(email__in=user_email_set)
                       .values_list('id', flat=True))

    transactions_query = merchant.transactions\
        .filter(created__range=prepare_datetime_range(start_date, end_date))\
        .filter(user_id__in=user_ids)\
        .exclude(status=Transaction.STATUS.canceled)

    all_store_trx_agg = filter_store_and_agg_trx(transactions_query, all_store_ids)
    ols_store_trx_agg = filter_store_and_agg_trx(transactions_query, ols_store_ids)
    mds_store_trx_agg = filter_store_and_agg_trx(transactions_query, mds_store_ids)
    pl_store_trx_agg = filter_store_and_agg_trx(transactions_query, pl_store_ids)
    ecomm_store_trx_agg = filter_store_and_agg_trx(transactions_query, ecomm_store_ids)

    return all_store_trx_agg, ols_store_trx_agg, mds_store_trx_agg, pl_store_trx_agg, ecomm_store_trx_agg


#7 DAYS OPEN
OUTPUT_PATH = f'/tmp/newsletter_statuses_open_before6march.csv'

header = [
        'campaign_name',
        'newsletter_id',
        'start_date',
        'end_date',
        'send',
        'delivered',
        'open',
        'open_rate',
        'click',
        'click_rate',
        'total_revenue_from_open_email',
        'total_trx_from_open_email',
        'unique_responder',
        'revenue_ols',
        'transaction_ols',
        'revenue_mds',
        'transaction_mds',
        'revenue_pl',
        'transaction_pl',
        'revenue_ecommerce',
        'transaction_ecommerce'
        ]

with open(OUTPUT_PATH, 'w') as output:
    writer = csv.DictWriter(output, fieldnames=header)
    writer.writeheader()

    for newsletter_id in EMAIL_STATISTIC_INPUTS:
        send_set7, open_set7, click_set7, bounce_set7, delivered_set7, newsletter_start_date7, cut_off_date7, newsletter_subject7 =  get_email_statistics_transactions(newsletter_id, 7)
        all_store_trx_agg7, ols_store_trx_agg7, mds_store_trx_agg7, pl_store_trx_agg7, ecomm_store_trx_agg7 = get_transaction(open_set7, newsletter_start_date7, cut_off_date7)


        writer.writerow({
            'campaign_name': newsletter_subject7,
            'newsletter_id': newsletter_id,
            'start_date': newsletter_start_date7,
            'end_date': cut_off_date7,
            'send': len(send_set7),
            'delivered': len(delivered_set7),
            'open': len(open_set7),
            'open_rate' : len(open_set7)/len(send_set7),
            'click': len(click_set7),
            'click_rate': len(click_set7)/len(send_set7),
            'total_revenue_from_open_email': all_store_trx_agg7['revenue'],
            'total_trx_from_open_email': all_store_trx_agg7['trx_count'],
            'unique_responder': all_store_trx_agg7['unique_responder'],
            'revenue_ols': ols_store_trx_agg7['revenue'],
            'transaction_ols': ols_store_trx_agg7['trx_count'],
            'revenue_mds': mds_store_trx_agg7['revenue'],
            'transaction_mds': mds_store_trx_agg7['trx_count'],
            'revenue_pl': pl_store_trx_agg7['revenue'],
            'transaction_pl': pl_store_trx_agg7['trx_count'],
            'revenue_ecommerce': ecomm_store_trx_agg7['revenue'],
            'transaction_ecommerce': ecomm_store_trx_agg7['trx_count']
            })
