import csv

merchant = Merchant.objects.get(id=2)
memberships = merchant.group.memberships.all()\
                      .select_related('user')\
                      .only('user_id', 'user__name','user__phone','user__email','redemption_allowed','member_status')


# membership_values = memberships.values('user_id', 'user__name','user__phone','user__email','redemption_allowed')

# memberships_is_blocked = {}


# memberships = Membership.objects.all()\
#                       .select_related('user')\
#                       .only('user_id', 'user__name','user__phone','user__email','redemption_allowed')


members_list = []
for membership in memberships.iterator():
    member_dict = {
                    'user_id': membership.user_id,
                    'name': membership.user.name,
                    'phone': membership.user.phone,
                    'email': membership.user.email,
                    'is_blocked': membership.is_blocked,
                    'redemption_allowed': membership.redemption_allowed
                }

    members_list.append(member_dict)




members_list = []  # Assuming you have populated the members_list



# filtered_list = [ member for member in member_list if member['is_blocked']=='True' or member['redemption_allowed']=='False']
# # ... code to create members_list ...

filtered_list = [member for member in members_list if member['is_blocked']==True or member['redemption_allowed']==False]
# filtered_list_reverse = [member for member in members_list if member['is_blocked']==False or member['redemption_allowed']==True]

# Specify the filename for the CSV file
csv_filename = '/home/bintang/members_isblocked_redallow_all.csv'

# Specify the field names for the CSV columns
field_names = ['user_id', 'name', 'phone', 'email', 'is_blocked', 'redemption_allowed']

# Open the CSV file in write mode
with open(csv_filename, 'w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=field_names)

    # Write the header row
    writer.writeheader()

    # Write the member data rows
    for member in members_list:
        writer.writerow(member)

print("CSV file created successfully.")
