import csv
import boto3
import zipfile
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from stamps.core.utils import prepare_datetime_range
from stamps.apps.newsletters.models import Newsletter
from io import BytesIO, StringIO

from django.conf import settings
#scheduled date = date sent nya.

# start_date = date(2021, 12, 1)
# end_date = date(2022, 2, 27)

# newsletters=Newsletter.objects.filter(scheduled_time__range=prepare_datetime_range(start_date,end_date))

newsletter_ids = [1069, 1070, 1071, 1072, 1078, 1079, 1080, 1081, 1089, 1090, 1097, 1098, 1111, 1112, 1113, 1114]



def generate_newsletter_stats(newsletter_id):

    newsletter = Newsletter.objects.get(id=newsletter_id)

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

    send_zip = zip_object.read('Send-event.csv')
    open_zip = zip_object.read('Open-event.csv')
    click_zip = zip_object.read('Click-event.csv')
    complaint_zip = zip_object.read('Complaint-event.csv')
    bounce_zip = zip_object.read('Bounce-event.csv')
    reject_zip = zip_object.read('Reject-event.csv')


    send_reader = csv.reader(StringIO(send_zip.decode()))
    open_reader = csv.reader(StringIO(open_zip.decode()))
    click_reader = csv.reader(StringIO(click_zip.decode()))
    complaint_reader = csv.reader(StringIO(complaint_zip.decode()))
    bounce_reader = csv.reader(StringIO(bounce_zip.decode()))
    reject_reader = csv.reader(StringIO(reject_zip.decode()))

    send_set = set(row[0] for row in send_reader if row[0] != 'Emails')
    open_set = set(row[0] for row in open_reader if row[0] != 'Emails')
    click_set = set(row[0] for row in click_reader if row[0] != 'Emails')
    complaint_set = set(row[0] for row in complaint_reader if row[0] != 'Emails')
    bounce_set = set(row[0] for row in bounce_reader if row[0] != 'Emails')
    reject_set = set(row[0] for row in reject_reader if row[0] != 'Emails')



    data = {}

    send_count = 0
    open_count = 0
    click_count = 0
    complaint_count = 0
    bounce_count = 0
    reject_count = 0

    for email in send_set:

        data[email] = []

        send_status = 'T'
        open_status = 'T' if email in open_set else 'F'
        click_status = 'T' if email in click_set else 'F'
        bounce_status = 'T' if email in bounce_set else 'F'
        complaint_status = 'T' if email in complaint_set else 'F'
        reject_status = 'T' if email in reject_set else 'F'

        if send_status == 'T':
            send_count += 1

        if open_status == 'T':
            open_count += 1

        if click_status == 'T':
            click_count += 1

        if bounce_status == 'T':
            bounce_count += 1

        if complaint_status == 'T':
            complaint_count += 1

        if reject_status == 'T':
            reject_count += 1

    summ_dict = ({
        'newsletter_id': newsletter_id,
        'send_count': send_count,
        'delivered_count': send_count - bounce_count - reject_count,
        'open_count': open_count,
        'click_count': click_count,
        'bounce_count': bounce_count,
        'complaint_count': complaint_count,
        'reject_count': reject_count
    })

    return summ_dict



OUTPUT_PATH = f'/home/bintang/newsletter_statuses_summary_eoss22_h1.csv'

header = [
        'newsletter_id',
        'send_count',
        'delivered_count',
        'open_count',
        'click_count',
        'bounce_count',
        'complaint_count',
        'reject_count'
        ]

with open(OUTPUT_PATH, 'w') as output:
    writer = csv.DictWriter(output, fieldnames=header)
    writer.writeheader()

    for newsletter_id in newsletter_ids:
        summ_dict = generate_newsletter_stats(newsletter_id)

        writer.writerow({
            'newsletter_id': summ_dict['newsletter_id'],
            'send_count': summ_dict['send_count'],
            'delivered_count': summ_dict['delivered_count'],
            'open_count': summ_dict['open_count'],
            'click_count': summ_dict['click_count'],
            'bounce_count': summ_dict['bounce_count'],
            'complaint_count': summ_dict['complaint_count'],
            'reject_count': summ_dict['reject_count']
            })
