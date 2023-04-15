import boto3
import csv
import zipfile
from io import BytesIO, StringIO

from django.conf import settings

from stamps.apps.newsletters.models import Newsletter

newsletter_ids = [1069, 1070, 1071, 1072]

def generate_newsletter_stats(newsletter_id):

    newsletter = Newsletter.objects.get(id=newsletter_id)
    newsletter_start_date = timezone.localtime(newsletter.scheduled_time).strftime("%Y-%m-%d")
    newsletter_subject = newsletter.subject

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

    send_reader = csv.reader(StringIO(send_zip.decode()))
    open_reader = csv.reader(StringIO(open_zip.decode()))
    click_reader = csv.reader(StringIO(click_zip.decode()))
    complaint_reader = csv.reader(StringIO(complaint_zip.decode()))
    bounce_reader = csv.reader(StringIO(bounce_zip.decode()))

    send_set = set(row[0] for row in send_reader if row[0] != 'Emails')
    open_set = set(row[0] for row in open_reader if row[0] != 'Emails')
    click_set = set(row[0] for row in click_reader if row[0] != 'Emails')
    complaint_set = set(row[0] for row in complaint_reader if row[0] != 'Emails')
    bounce_set = set(row[0] for row in bounce_reader if row[0] != 'Emails')

    user_dict = dict(User.objects.filter(email__in=send_set).values_list('email', 'id'))

    data = {}
    for email in send_set:

        data[email] = []

        user_id = user_dict.get(email)

        send_status = 'T'
        open_status = 'T' if email in open_set else 'F'
        click_status = 'T' if email in click_set else 'F'
        bounce_status = 'T' if email in bounce_set else 'F'
        complaint_status = 'T' if email in complaint_set else 'F'

        data[email].append({
            'campaign_name': newsletter_subject,
            'channel': 'email',
            'email': email,
            'user_id': user_id,
            'date_of_communication': newsletter_start_date,
            'send': send_status,
            'open': open_status,
            'click': click_status,
            'bounce': bounce_status,
            'complaint': complaint_status
        })

    OUTPUT_PATH = f'/home/bintang/newsletter_statuses_amazon_newsletter_id_{newsletter_id}.csv'

    header = [
            'campaign_name',
            'channel',
            'email',
            'user_id',
            'date_of_communication',
            'send',
            'open',
            'click',
            'bounce',
            'complaint'
            ]

    with open(OUTPUT_PATH, 'w') as output:
        writer = csv.DictWriter(output, fieldnames=header)
        writer.writeheader()

        for value in data.values():
            for items in value:
                email = items['email']
                user_id = items['user_id']
                send_status = items['send']
                open_status = items['open']
                click_status = items['click']
                bounce_status = items['bounce']
                complaint_status = items['complaint']

            writer.writerow({
                'campaign_name': newsletter_subject,
                'channel': 'email',
                'email': email,
                'user_id': user_id,
                'date_of_communication': newsletter_start_date,
                'send': send_status,
                'open': open_status,
                'click': click_status,
                'bounce': bounce_status,
                'complaint': complaint_status
                })


for newsletter_id in newsletter_ids:
    generate_newsletter_stats(newsletter_id)
