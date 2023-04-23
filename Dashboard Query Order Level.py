import csv
import datetime


from django.db.models import OuterRef, Subquery

from harvest_web.apps.stamps_payments.models import TransactionPayment
from harvest_web.apps.stamps_redemptions.models import Redemption
from harvest_web.apps.stamps_transactions.models import Transaction
from harvest_web.apps.omni_orders.models import Order
from harvest_web.apps.omni_promotions.models import OmniPromotion
from harvest_web.website.reports.utils import prepare_datetime_range
from harvest_web.constants import DeliveryType

start_date = datetime.date(2022, 11, 1)
end_date = datetime.date(2022, 11, 30)
date_range = prepare_datetime_range(start_date, end_date)

MERCHANTS = ['The Harvest Express', 'The Harvest']

annotate_last_payment_method = (TransactionPayment.objects.filter(transaction=OuterRef('pk')).order_by('-id')
                                                          .values('payment_method')[:1])
transactions_exclude_canceled = (
    Transaction.objects.filter(expiration_journal__created__range=date_range)
                       .filter(created__range=date_range)
                       .exclude(status=Transaction.STATUS.canceled)
                       .filter(merchant__name__in=MERCHANTS)
                       .order_by('created')
                       .select_related('user', 'store', 'merchant')
                       .annotate(last_payment_method=Subquery(annotate_last_payment_method))
                       .only('created', 'merchant_invoice_number', 'merchant__name', 'stamps', 'store__display_name',
                             'subtotal', 'type', 'user__email', 'user__name', 'user__phone', 'value')
)

orders = (Order.objects.filter(created__range=date_range)
                       .filter(status=Order.STATUS.completed)
                       .filter(merchant__name__in=MERCHANTS)
                       .order_by('created')
                       .select_related('user', 'store', 'merchant', 'delivery_info', 'pickup_data')
                       .only('channel', 'created', 'delivery_fee', 'delivery_info__address',
                             'delivery_info__delivery_time', 'delivery_info__recipient_name',
                             'delivery_info__recipient_phone', 'delivery_status', 'delivery_type', 'number',
                             'payment_method', 'payment_status', 'pos_order_number', 'pickup_data__recipient_name',
                             'pickup_data__recipient_phone', 'pickup_data__scheduled_pickup_time', 'type',
                             'user__email', 'user__mobile_number', 'user__name', 'merchant__name', 'discount_value',
                             'store__name', 'total', 'grand_total', 'stamps_earned')
)

crm_invoice_number = transactions_exclude_canceled.values_list('merchant_invoice_number', flat=True)

orders_omni_crm = orders.filter(number__in=set(crm_invoice_number))
orders_omni_only = orders.exclude(number__in=orders_omni_crm.values_list('number', flat=True))

set_order_ids = set(orders.values_list('id', flat=True))

redemptions = (Redemption.objects.select_related('voucher_template', 'voucher')
                                 .filter(merchant_invoice_number__in=crm_invoice_number)
                                 .exclude(status=Redemption.STATUS.canceled)
                                 .only('merchant_invoice_number', 'voucher__code', 'voucher_template__extra_data'))

promotions = (OmniPromotion.objects.select_related('application')
                                   .only('application__discount_percentage', 'application__discount_value', 'code'))

deliverytask = DeliveryTask.objects.filter(omni_order_id__in=set_order_ids)\
                                   .exclude(Q(status=None) | Q(status__in=[DeliveryTask.STATUS.canceled, DeliveryTask.STATUS.unable_to_connect_to_3pl, DeliveryTask.STATUS.error_returned_by_3pl]))\
                                   .only('driver_name')\

promotion_id_mapping = dict()
promotion_code_mapping = dict()

order_mapping_omni_crm = {order.number: order for order in orders_omni_crm}
redemption_mapping = {redemption.merchant_invoice_number: redemption for redemption in redemptions}

driver_mapping = {}
for delivery in deliverytask:

    omni_order_id=delivery.omni_order_id
    del_type = delivery.delivery_type

    if del_type == DeliveryType.internal:
        if delivery.rider is not None:
            driver = delivery.rider.user.name
        else:
            driver = None
    else:
        driver = delivery.driver_name

    driver_mapping[omni_order_id] = driver

for promotion in promotions:
    promotion_id_mapping[promotion.id] = promotion
    promotion_code_mapping[promotion.code] = promotion

with open(f'/tmp/summary_order_transaction_crm_omni_from_{start_date}_until_{end_date}.csv', 'w') as data:
    fieldnames = [
        'order_date',
        'channel',
        'brand',
        'transaction_code',
        'no_bill',
        'email_sender_customer',
        'sender_name',
        'sender_mobile_number',
        'receiver_name',
        'receiver_mobile_number',
        'receiver_address',
        'order_type',
        'delivery_or_pickup_date',
        'dispatch_outlet',
        'payment_method',
        'delivery_charge',
        'subtotal',
        'voucher_value',
        'grand_total',
        'status_payment',
        'delivery_status_pos',
        'delivery_type',
        'delivered_by',
        'point_earning',
        'promotion_code',
        'voucher_code',
        'voucher_disc'
    ]

    writer = csv.DictWriter(data, fieldnames=fieldnames)
    writer.writeheader()

    for transaction in transactions_exclude_canceled:
        order_date = transaction.created.astimezone().strftime("%Y-%m-%d")
        brand = transaction.merchant
        transaction_code = transaction.merchant_invoice_number
        order_data = order_mapping_omni_crm.get(transaction_code)
        redemption_data = redemption_mapping.get(transaction_code)

        if order_data:
            no_bill = order_data.pos_order_number
            order_type_number = order_data.type
            order_type = order_data.get_type_display()
            delivery_status = order_data.get_delivery_status_display()
            delivery_type = order_data.get_delivery_type_display()

            delivery_charge = order_data.delivery_fee
            status_payment = order_data.get_payment_status_display()
            channel = order_data.get_channel_display()
            delivered_by = driver_mapping.get(order_data.id)

            if order_type_number == Order.TYPE.delivery:
                delivery_info = order_data.delivery_info
                receiver_name = delivery_info.recipient_name
                receiver_phone = delivery_info.recipient_phone
                receiver_address = delivery_info.address

                if delivery_info.delivery_time:
                    delivery_or_pickup_date = delivery_info.delivery_time.strftime("%Y-%m-%d")
                else:
                    delivery_or_pickup_date = "-"
            elif order_type_number == Order.TYPE.pick_up:
                pickup_data = order_data.pickup_data
                receiver_name = pickup_data.recipient_name
                receiver_phone = pickup_data.recipient_phone
                receiver_address = None

                if pickup_data.scheduled_pickup_time:
                    delivery_or_pickup_date = pickup_data.scheduled_pickup_time.strftime("%Y-%m-%d")
                else:
                    delivery_or_pickup_date = "-"
            else:
                receiver_name = None
                receiver_phone = None
                receiver_address = None
                delivery_or_pickup_date = None

            if transaction.user:
                transaction_user = transaction.user
                receiver_name = transaction_user.name
                receiver_phone = transaction_user.phone
                receiver_address = None
                delivery_or_pickup_date = None

            else:
                transaction_user = None
                receiver_name = None
                receiver_phone = None
                receiver_address = None
                delivery_or_pickup_date = None

            if order_data.user:
                order_data_user = order_data.user
                email_sender_customer = order_data_user.email
                sender_name = order_data_user.name
                sender_mobile_number = order_data_user.mobile_number

            else:
                email_sender_customer = None
                sender_name = None
                sender_mobile_number = None

        else:
            no_bill = None
            order_type_number = None
            order_type = transaction.get_type_display()
            delivery_status = None
            delivery_type = None
            delivery_charge = None
            status_payment = None
            channel = None
            receiver_name = None
            receiver_phone = None
            receiver_address = None
            delivery_or_pickup_date = None
            delivered_by = None

            if transaction.user:
                transaction_user = transaction.user
                email_sender_customer = transaction_user.email
                sender_name = transaction_user.name
                sender_mobile_number = transaction_user.phone
            else:
                email_sender_customer = None
                sender_name = None
                sender_mobile_number = None

        if transaction.last_payment_method is not None:
            payment_method = TransactionPayment.PAYMENT_METHOD(transaction.last_payment_method).label
        elif order_data:
            payment_method = order_data.get_payment_method_display()
        else:
            payment_method = None

        if redemption_data:
            promotion_id = redemption_data.voucher_template.extra_data['ordering_system'].get('promotion_id')
            promotion_code = redemption_data.voucher_template.extra_data['ordering_system'].get('promotion_code')
            promotion = promotion_id_mapping.get(promotion_id) or promotion_code_mapping.get(promotion_code)

            if promotion:
                promotion_code = promotion.code
                promotion_application = promotion.application
                voucher_value = promotion_application.discount_value
                voucher_disc = promotion_application.discount_percentage

            else:
                voucher_value = None
                voucher_disc = None
            voucher_code = redemption_data.voucher.code

        else:
            promotion_code = None
            voucher_value = None
            voucher_disc = None
            voucher_code = None

        dispatch_outlet = transaction.store.display_name
        subtotal = transaction.subtotal
        grand_total = transaction.value
        points_earned = transaction.stamps

        writer.writerow({
            'order_date': order_date,
            'channel': channel,
            'brand': brand,
            'transaction_code': transaction_code,
            'no_bill': no_bill,
            'email_sender_customer': email_sender_customer,
            'sender_name': sender_name,
            'sender_mobile_number': sender_mobile_number,
            'receiver_name': receiver_name,
            'receiver_mobile_number': receiver_phone,
            'receiver_address': receiver_address,
            'order_type': order_type,
            'delivery_or_pickup_date': delivery_or_pickup_date,
            'dispatch_outlet': dispatch_outlet,
            'payment_method': payment_method,
            'delivery_charge': delivery_charge,
            'subtotal': subtotal,
            'voucher_value': voucher_value,
            'grand_total': grand_total,
            'status_payment': status_payment,
            'delivery_status_pos': delivery_status,
            'delivery_type': delivery_type,
            'delivered_by': delivered_by,
            'point_earning': points_earned,
            'promotion_code': promotion_code,
            'voucher_code': voucher_code,
            'voucher_disc': voucher_disc
        })

    for order in orders_omni_only:
        order_date = order.created.astimezone().strftime("%Y-%m-%d")
        channel = order.get_channel_display()
        brand = order.merchant
        transaction_code = order.number
        no_bill = order.pos_order_number

        if order.user:
            order_user = order.user
            email_sender_customer = order_user.email
            sender_name = order_user.name
            sender_mobile_number = order_user.mobile_number

        else:
            email_sender_customer = None
            sender_name = None
            sender_mobile_number = None

        order_type = order.get_type_display()
        order_type_number = order.type
        redemption_data = redemption_mapping.get(transaction_code)

        if order_type_number == Order.TYPE.delivery:
            delivery_info = order.delivery_info
            receiver_name = delivery_info.recipient_name
            receiver_phone = delivery_info.recipient_phone
            receiver_address = delivery_info.address
            delivered_by = driver_mapping.get(order.id)

            if delivery_info.delivery_time:
                delivery_or_pickup_date = delivery_info.delivery_time.strftime("%Y-%m-%d")
            else:
                delivery_or_pickup_date = "-"
        elif order_type_number == Order.TYPE.pick_up:
            pickup_data = order.pickup_data
            receiver_name = pickup_data.recipient_name
            receiver_phone = pickup_data.recipient_phone
            receiver_address = None
            if pickup_data.scheduled_pickup_time:
                delivery_or_pickup_date = pickup_data.scheduled_pickup_time.strftime("%Y-%m-%d")
            else:
                delivery_or_pickup_date = "-"
        else:
            receiver_name = None
            receiver_phone = None
            receiver_address = None
            delivery_or_pickup_date = None
            driver = None

        if redemption_data:
            promotion_id = redemption_data.voucher_template.extra_data['ordering_system'].get('promotion_id')
            promotion_code = redemption_data.voucher_template.extra_data['ordering_system'].get('promotion_code')
            promotion = promotion_id_mapping.get(promotion_id) or promotion_code_mapping.get(promotion_code)
            if promotion:
                promotion_code = promotion.code
                promotion_application = promotion.application
                voucher_value = promotion_application.discount_value
                voucher_disc = promotion_application.discount_percentage
            else:
                voucher_value = None
                voucher_disc = None
            voucher_code = redemption_data.voucher.code
        else:
            promotion_code = None
            voucher_value = order.discount_value
            voucher_disc = None
            voucher_code = None

        dispatch_outlet = order.store
        payment_method = order.get_payment_method_display()
        subtotal = order.total
        delivery_charge = order.delivery_fee
        grand_total = order.grand_total
        status_payment = order.get_payment_status_display()
        delivery_status = order.get_delivery_status_display()
        delivery_type = order.get_delivery_type_display()
        points_earned = order.stamps_earned

        writer.writerow({
            'order_date': order_date,
            'channel': channel,
            'brand': brand,
            'transaction_code': transaction_code,
            'no_bill': no_bill,
            'email_sender_customer': email_sender_customer,
            'sender_name': sender_name,
            'sender_mobile_number': sender_mobile_number,
            'receiver_name': receiver_name,
            'receiver_mobile_number': receiver_phone,
            'receiver_address': receiver_address,
            'order_type': order_type,
            'delivery_or_pickup_date': delivery_or_pickup_date,
            'dispatch_outlet': dispatch_outlet,
            'payment_method': payment_method,
            'delivery_charge': delivery_charge,
            'subtotal': subtotal,
            'voucher_value': voucher_value,
            'grand_total': grand_total,
            'status_payment': status_payment,
            'delivery_status_pos': delivery_status,
            'delivery_type': delivery_type,
            'delivered_by': delivered_by,
            'point_earning': points_earned,
            'promotion_code': promotion_code,
            'voucher_code': voucher_code,
            'voucher_disc': voucher_disc
        })
