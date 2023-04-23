import csv
import datetime

from django.db.models import OuterRef, Subquery

from harvest_web.apps.omni_items.models import Item
from harvest_web.apps.omni_orders.models import Order
from harvest_web.apps.omni_promotions.models import OmniPromotion
from harvest_web.apps.stamps_payments.models import TransactionPayment
from harvest_web.apps.stamps_redemptions.models import Redemption
from harvest_web.apps.stamps_transactions.models import StampsItem, Transaction
from harvest_web.website.reports.utils import prepare_datetime_range
from harvest_web.constants import DeliveryType


start_date = datetime.date(2022, 11, 1)
end_date = datetime.date(2022, 11, 15)

start_date_exp = datetime.date(2022, 11, 1)
end_date_exp = datetime.date(2022, 11, 30)

date_range= prepare_datetime_range(start_date, end_date)
date_range_exp= prepare_datetime_range(start_date_exp, end_date_exp)

MERCHANT = ['The Harvest Express', 'The Harvest']

annotate_last_payment_method = (TransactionPayment.objects.filter(transaction=OuterRef('transaction')).order_by('-id')
                                                          .values('payment_method')[:1])
stampsitems_all = (
    StampsItem.objects.filter(transaction__expiration_journal__created__range=date_range_exp)
                      .filter(transaction__created__range=date_range)
                      .exclude(transaction__status=Transaction.STATUS.canceled)
                      .filter(transaction__merchant__name__in=MERCHANT)
                      .select_related('product', 'transaction', 'transaction__merchant', 'transaction__store',
                                      'transaction__user')
                      .order_by('transaction__created')
                      .annotate(last_payment_method=Subquery(annotate_last_payment_method))
                      .only('price', 'product__name', 'quantity', 'transaction__created', 'transaction__merchant__name',
                            'transaction__merchant_invoice_number', 'transaction__stamps',
                            'transaction__store__display_name', 'transaction__type', 'transaction__user__email',
                            'transaction__user__name', 'transaction__user__phone')
)

items_all = (
    Item.objects.filter(order__created__range=date_range)
                .filter(order__status=Order.STATUS.completed)
                .filter(order__merchant__name__in=MERCHANT)
                .select_related('order', 'order__delivery_info', 'order__merchant', 'order__pickup_data',
                                'order__store', 'order__user', 'inventory__variant')
                .only('inventory__variant__name', 'notes', 'order__channel', 'order__created',
                      'order__delivery_info__address', 'order__delivery_info__delivery_time',
                      'order__delivery_info__recipient_name', 'order__delivery_info__recipient_phone',
                      'order__discount_value', 'order__merchant__name', 'order__number', 'order__payment_method',
                      'order__payment_status', 'order__pos_delivery_status', 'order__pos_order_number',
                      'order__pickup_data__recipient_name', 'order__pickup_data__recipient_phone',
                      'order__pickup_data__scheduled_pickup_time', 'order__stamps_earned', 'order__store__name',
                      'order__type', 'order__user__email', 'order__user__mobile_number', 'order__user__name',
                      'quantity', 'subtotal')
            )

crm_invoice_number = stampsitems_all.values_list('transaction__merchant_invoice_number', flat=True)

items_omni_crm = items_all.filter(order__number__in=set(crm_invoice_number))
items_omni_only = items_all.exclude(order__number__in=items_omni_crm.values_list('order__number', flat=True))

set_order_ids = set(items_all.values_list('order_id', flat=True))

voucher_redemptions = (Redemption.objects.filter(merchant_invoice_number__in=crm_invoice_number)
                                         .exclude(status=Redemption.STATUS.canceled)
                                         .select_related('voucher', 'voucher_template')
                                         .only('merchant_invoice_number', 'voucher__code',
                                               'voucher_template__extra_data'))

reward_redemptions = (Redemption.objects.filter(voucher__code__in={r.voucher.code for r in voucher_redemptions})
                                        .exclude(id__in={r.id for r in voucher_redemptions})
                                        .select_related('voucher'))\
                                        .only('stamps', 'voucher__code')

promotions = (OmniPromotion.objects.select_related('application')
                                   .only('application__discount_percentage', 'application__discount_value', 'code'))

deliverytask = DeliveryTask.objects.filter(omni_order_id__in=set_order_ids)\
                                   .exclude(Q(status=None) | Q(status__in=[DeliveryTask.STATUS.canceled, DeliveryTask.STATUS.unable_to_connect_to_3pl, DeliveryTask.STATUS.error_returned_by_3pl]))\
                                   .only('driver_name')

promotion_id_mapping = dict()
promotion_code_mapping = dict()


item_mapping_omni_crm = {item.order.number: item for item in items_omni_crm}
voucher_redemption_mapping = {redemption.merchant_invoice_number: redemption for redemption in voucher_redemptions}
reward_redemption_mapping = {redemption.voucher.code: redemption for redemption in reward_redemptions}

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

with open(f'/tmp/detail_order_item_hv_hvw-{start_date}-{end_date}-experiments.csv', 'w') as output:
    header = [
        'order_date', 'order_type', 'brand', 'transaction_code', 'no_bill', 'email_sender_customer', 'sender_name',
        'sender_mobile_number', 'receiver_name', 'receiver_phone', 'receiver_address', 'shipping_method',
        'delivery_or_pickup_date', 'dispatch_outlet', 'payment_method', 'product', 'description/wording', 'quantity',
        'price', 'status_payment', 'integrate_status', 'delivered_by', 'points_redeemed', 'points_earned',
        'voucher_code', 'voucher_disc', 'voucher_value'
    ]

    writer = csv.DictWriter(output, fieldnames=header)
    writer.writeheader()

    for item in stampsitems_all:
        dispatch_outlet = item.transaction.store.display_name
        price = item.price
        quantity = item.quantity
        brand = item.transaction.merchant
        transaction_code = item.transaction.merchant_invoice_number
        order_date = item.transaction.created.astimezone().strftime("%Y-%m-%d")
        shipping_method = item.transaction.get_type_display()
        item_data = item_mapping_omni_crm.get(transaction_code)
        redemption_data = voucher_redemption_mapping.get(transaction_code)

        order_type = None
        no_bill = None
        order_type_number = None
        status_payment = None
        receiver_name = None
        receiver_phone = None
        receiver_address = None
        delivery_or_pickup_date = None
        note = None
        integrate_status = None
        email_sender_customer = None
        sender_name = None
        sender_mobile_number = None
        voucher_code = None
        payment_method = None
        voucher_value = None
        points_redeemed = None
        promotion_code = None
        promotion_id = None
        voucher_disc = None
        reward_redeem = None
        points_redeemed = None

        if item_data:
            no_bill = item_data.order.pos_order_number
            order_type_number = item_data.order.type
            note = item_data.notes
            points_earned = item_data.order.stamps_earned
            delivered_by = driver_mapping.get(item_data.order.id)

            order_type = item_data.order.get_type_display()
            integrate_status = item_data.order.get_pos_delivery_status_display()
            status_payment = item_data.order.get_payment_status_display()
            if order_type_number == Order.TYPE.delivery:
                receiver_name = item_data.order.delivery_info.recipient_name
                receiver_phone = item_data.order.delivery_info.recipient_phone
                receiver_address = item_data.order.delivery_info.address
                if item_data.order.delivery_info.delivery_time:
                    delivery_or_pickup_date = (item_data.order.delivery_info.delivery_time.astimezone()
                                                                            .strftime("%Y-%m-%d"))
                else:
                    delivery_or_pickup_date = '-'

            elif order_type_number == Order.TYPE.pick_up:
                receiver_name = item_data.order.pickup_data.recipient_name
                receiver_phone = item_data.order.pickup_data.recipient_phone
                if item_data.order.pickup_data.scheduled_pickup_time:
                    delivery_or_pickup_date = (item_data.order.pickup_data.scheduled_pickup_time.astimezone()
                                                                          .strftime("%Y-%m-%d"))
                else:
                    delivery_or_pickup_date = '-'

            elif item.transaction.user:
                receiver_name = item.transaction.user.name
                receiver_phone = item.transaction.user.phone

            if item_data.order.user:
                email_sender_customer = item_data.order.user.email
                sender_name = item_data.order.user.name
                sender_mobile_number = item_data.order.user.mobile_number

        else:
            points_earned = item.transaction.stamps
            order_type = item.transaction.get_type_display()
            delivered_by = None

            if item.transaction.user:
                email_sender_customer = item.transaction.user.email
                sender_name = item.transaction.user.name
                sender_mobile_number = item.transaction.user.phone

        if item:
            product = item.product.name

        else:
            product = None

        if item.last_payment_method is not None:
            payment_method = TransactionPayment.PAYMENT_METHOD(item.last_payment_method).label

        elif item_data:
            payment_method = item_data.order.get_payment_method_display()

        else:
            payment_method = None

        if redemption_data:
            promotion_id = redemption_data.voucher_template.extra_data['ordering_system'].get('promotion_id')
            promotion_code = redemption_data.voucher_template.extra_data['ordering_system'].get('promotion_code')
            promotion = promotion_id_mapping.get(promotion_id) or promotion_code_mapping.get(promotion_code)
            if promotion:
                voucher_value =  promotion.application.discount_value
                voucher_disc =  promotion.application.discount_percentage
            else:
                voucher_value = None
                voucher_disc = None
            voucher_code = redemption_data.voucher.code

            reward_redeem = reward_redemption_mapping.get(voucher_code)

            if reward_redeem is not None:
                points_redeemed = reward_redeem.stamps

        else:
            voucher_value = None
            voucher_disc = None
            voucher_code = None

        writer.writerow({
            'order_date': order_date,
            'order_type': order_type,
            'brand': brand,
            'transaction_code': transaction_code,
            'no_bill': no_bill,
            'email_sender_customer': email_sender_customer,
            'sender_name': sender_name,
            'sender_mobile_number': sender_mobile_number,
            'receiver_name': receiver_name,
            'receiver_phone': receiver_phone,
            'receiver_address': receiver_address,
            'shipping_method': shipping_method,
            'delivery_or_pickup_date': delivery_or_pickup_date,
            'dispatch_outlet': dispatch_outlet,
            'payment_method': payment_method,
            'product': product,
            'description/wording': note,
            'quantity': quantity,
            'price': price,
            'status_payment': status_payment,
            'integrate_status': integrate_status,
            'delivered_by': delivered_by,
            'points_redeemed': points_redeemed,
            'points_earned': points_earned,
            'voucher_code': voucher_code,
            'voucher_disc': voucher_disc,
            'voucher_value': voucher_value
        })

    for item in items_omni_only:
        order_type_number = item.order.type
        brand = item.order.merchant
        transaction_code = item.order.number
        no_bill = item.order.pos_order_number
        delivered_by = driver_mapping.get(item.order.id)

        if item.order.user:
            email_sender_customer = item.order.user.email
            sender_name = item.order.user.name
            sender_mobile_number = item.order.user.mobile_number

        else:
            email_sender_customer = None
            sender_name = None
            sender_mobile_number = None

        dispatch_outlet = item.order.store
        product = item.inventory
        note = item.notes
        quantity = item.quantity
        price = item.subtotal
        points_earned = item.order.stamps_earned
        status_payment = item.order.get_payment_status_display()
        integrate_status = item.order.get_pos_delivery_status_display()
        payment_method = item.order.get_payment_method_display()
        shipping_method = item.order.get_type_display()
        order_date = item.order.created.astimezone().strftime("%Y-%m-%d")
        order_type = item.order.get_channel_display()
        redemption_data = voucher_redemption_mapping.get(transaction_code)

        delivery_or_pickup_date = None
        receiver_address = None
        delivery_or_pickup_date = None
        reward_redeem = None
        points_redeemed = None
        redeem = None
        promotion_code = None
        promotion_id = None
        voucher_value = None
        voucher_disc = None
        voucher_code = None

        if order_type_number == Order.TYPE.delivery:
            receiver_name = item.order.delivery_info.recipient_name
            receiver_phone = item.order.delivery_info.recipient_phone
            receiver_address = item.order.delivery_info.address

            if item.order.delivery_info.delivery_time:
                delivery_or_pickup_date = item.order.delivery_info.delivery_time.astimezone().strftime("%Y-%m-%d")
            else:
                delivery_or_pickup_date = '-'

        elif order_type_number == Order.TYPE.pick_up:
            receiver_name = item.order.pickup_data.recipient_name
            receiver_phone = item.order.pickup_data.recipient_phone
            if item.order.pickup_data.scheduled_pickup_time:
                delivery_or_pickup_date = item.order.pickup_data.scheduled_pickup_time.astimezone().strftime("%Y-%m-%d")
            else:
                delivery_or_pickup_date = '-'

        else:
            if item.order.user:
                receiver_name = item.order.user.name
                receiver_phone = item.order.user.mobile_number

            else:
                receiver_name = None
                receiver_phone = None

        if redemption_data:
            promotion_id = redemption_data.voucher_template.extra_data['ordering_system'].get('promotion_id')
            promotion_code = redemption_data.voucher_template.extra_data['ordering_system'].get('promotion_code')
            promotion = promotion_id_mapping.get(promotion_id) or promotion_code_mapping.get(promotion_code)
            if promotion:
                voucher_value = promotion.application.discount_value
                voucher_disc = promotion.application.discount_percentage

            else:
                voucher_value = None
                voucher_disc = None
            voucher_code = redemption_data.voucher.code

            reward_redeem = reward_redemption_mapping.get(voucher_code)

            if reward_redeem is not None:
                points_redeemed = reward_redeem.stamps

            else:
                points_redeemed = None

        else:
            voucher_value = item.order.discount_value
            voucher_disc = None
            voucher_code = None

        writer.writerow({
            'order_date': order_date,
            'order_type': order_type,
            'brand': brand,
            'transaction_code': transaction_code,
            'no_bill': no_bill,
            'email_sender_customer': email_sender_customer,
            'sender_name': sender_name,
            'sender_mobile_number': sender_mobile_number,
            'receiver_name': receiver_name,
            'receiver_phone': receiver_phone,
            'receiver_address': receiver_address,
            'shipping_method': shipping_method,
            'delivery_or_pickup_date': delivery_or_pickup_date,
            'dispatch_outlet': dispatch_outlet,
            'payment_method': payment_method,
            'product': product,
            'description/wording': note,
            'quantity': quantity,
            'price': price,
            'status_payment': status_payment,
            'integrate_status': integrate_status,
            'delivered_by': delivered_by,
            'points_redeemed': points_redeemed,
            'points_earned': points_earned,
            'voucher_code': voucher_code,
            'voucher_disc': voucher_disc,
            'voucher_value': voucher_value
        })
