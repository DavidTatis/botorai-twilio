import json
import logging
import requests
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta, time, timezone
from dateutil import parser
from zoneinfo import ZoneInfo
import uuid
import stripe
import calendar
import os
from twilio_utils import parse_incoming_twilio_event, get_mobile, send_message, send_template_message

# For Miami, Florida
time_zone = ZoneInfo("America/New_York")


TOKEN = os.getenv("TOKEN")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
stripe.api_key=os.getenv("STRIPE_KEY")

phone_number_id = 524839744037877
base_url = "https://graph.facebook.com/v17.0"
url = f"{base_url}/{phone_number_id}/messages"

FOOTER_TEXT = "BotorAI"
HOURS_TO_DELETE_AFTER_ASKING_NAME = 12
RECEIVER_PHONE_NUMBER = "+15550139876"
MINUTES_FOR_RESERVATIONS = 60
SUCCESS_PAYMENT_URL = "https://botorai.com"
CANCEL_PAYMENT_URL = "https://botorai.com"
APP_FEE_PRICE = 100
STRIPE_PERCENT_FEE = 2.9 / 100
STRIPE_BASE_FEE = 30
NUMBER_DAYS_TO_SHOW_RESERVATIONS = 7

headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer {}".format(TOKEN),
}

dynamodb = boto3.client("dynamodb")

# Name of the DynamoDB table
client_table_name = "Client-tp3xjyxjoffklgjrporm4ed4ay-staging"
staff_table_name = "Staff-tp3xjyxjoffklgjrporm4ed4ay-staging"
service_table_name = "Service-tp3xjyxjoffklgjrporm4ed4ay-staging"
book_table_name = "Book-tp3xjyxjoffklgjrporm4ed4ay-staging"
conv_status_table_name = "conversation_status_1"
conversation_table_name = "Conversation-tp3xjyxjoffklgjrporm4ed4ay-staging"
message_table_name = "Message-tp3xjyxjoffklgjrporm4ed4ay-staging"



def interactive_answer(data):

    return data["entry"][0]["changes"][0]["value"]["messages"][0]["interactive"]["button_reply"]["id"]


def preprocess(data):
    return data["entry"][0]["changes"][0]["value"]


def changed_field(data):
    return data["entry"][0]["changes"][0]["field"]




def get_name(data):
    contact = preprocess(data)
    if contact:
        return contact["contacts"][0]["profile"]["name"]


def get_delivery(data):
    data = preprocess(data)
    if "statuses" in data:
        return data["statuses"][0]["status"]


def detect_income_message(data):
    if "messages" in data["entry"][0]["changes"][0]["value"]:
        return True
    return False




def send_message_url(header, message, link, recipient_id):

    data = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": recipient_id,
        "type": "interactive",
        "interactive": {
            "type": "cta_url",
            "header": {"type": "text", "text": header},
            "body": {"text": message},
            "footer": {"text": FOOTER_TEXT},
            "action": {
                "name": "cta_url",
                "parameters": {"display_text": "Book", "url": link},
            },
        },
    }
    logging.info(f"Sending message with button to {recipient_id}")
    r = requests.post(f"{url}", headers=headers, json=data)
    if r.status_code == 200:
        logging.info(f"Message sent to {recipient_id}")
        return r.json()
    logging.info(f"Message not sent to {recipient_id}")
    logging.info(f"Status code: {r.status_code}")
    logging.info(f"Response: {r.json()}")
    return r.json()


def send_message_terms_with_button(message, button_text, button_url, recipient_id, recipient_type="individual"):
    data = {
        "messaging_product": "whatsapp",
        "recipient_type": recipient_type,
        "to": recipient_id,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "header": {
                "type": "document",
                "document": {"link": button_url, "filename": button_text},
            },
            "body": {"text": message},
            "action": {
                "buttons": [
                    {
                        "type": "reply",
                        "reply": {"id": "terms_accepted", "title": "Accept"},
                    }
                ]
            },
            "footer": {"text": FOOTER_TEXT},
        },
    }
    logging.info(f"Sending message with button to {recipient_id}")
    r = requests.post(f"{url}", headers=headers, json=data)
    if r.status_code == 200:
        logging.info(f"Message sent to {recipient_id}")
        return r.json()
    logging.info(f"Message not sent to {recipient_id}")
    logging.info(f"Status code: {r.status_code}")
    logging.info(f"Response: {r.json()}")
    return r.json()


def send_message_list_barbers(message, recipient_id, barber_list):
    data = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": recipient_id,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "header": {"type": "text", "text": "Choose a professional"},
            "body": {"text": message},
            "action": {
                "button": "PROFESSIONALS",
                "sections": [{"title": "SECTION_1_TITLE", "rows": barber_list}],
            },
            "footer": {"text": FOOTER_TEXT},
        },
    }
    logging.info(f"Sending message with button to {recipient_id}")
    r = requests.post(f"{url}", headers=headers, json=data)
    if r.status_code == 200:
        logging.info(f"Message sent to {recipient_id}")
        return r.json()
    logging.info(f"Message not sent to {recipient_id}")
    logging.info(f"Status code: {r.status_code}")
    logging.info(f"Response: {r.json()}")
    return r.json()


def send_message_list_services(message, recipient_id, services):
    data = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": recipient_id,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "header": {"type": "text", "text": "Choose a service"},
            "body": {"text": message},
            "action": {
                "button": "SERVICES",
                "sections": [{"title": "SECTION_1_TITLE", "rows": services}],
            },
            "footer": {"text": FOOTER_TEXT},
        },
    }
    logging.info(f"Sending message with button to {recipient_id}")
    r = requests.post(f"{url}", headers=headers, json=data)
    if r.status_code == 200:
        logging.info(f"Message sent to {recipient_id}")
        return r.json()
    logging.info(f"Message not sent to {recipient_id}")
    logging.info(f"Status code: {r.status_code}")
    logging.info(f"Response: {r.json()}")
    return r.json()


def send_message_list(message, recipient_id, header_message, button_name, sections):
    data = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": recipient_id,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "header": {"type": "text", "text": header_message},
            "body": {"text": message},
            "action": {"button": button_name, "sections": sections},
            "footer": {"text": FOOTER_TEXT},
        },
    }
    logging.info(f"Sending message with button to {recipient_id}")
    r = requests.post(f"{url}", headers=headers, json=data)
    if r.status_code == 200:
        logging.info(f"Message sent to {recipient_id}")
        return r.json()
    logging.info(f"Message not sent to {recipient_id}")
    logging.info(f"Status code: {r.status_code}")
    logging.info(f"Response: {r.json()}")
    return r.json()


def send_booking_handler(message, booking_id, staff_id, recipient_id):
    data = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": recipient_id,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "header": {"type": "text", "text": "Your Next Reservation"},
            "body": {"text": message},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": f"future_booking/cancel/{booking_id}", "title": "Cancel ⛔️"}},
                    {"type": "reply", "reply": {"id": f"future_booking/edit_date/{staff_id}", "title": "Modify 📆"}},
                ]
            },
            "footer": {"text": FOOTER_TEXT},
        },
    }
    logging.info(f"Sending message with button to {recipient_id}")
    r = requests.post(f"{url}", headers=headers, json=data)
    if r.status_code == 200:
        logging.info(f"Message sent to {recipient_id}")
        return r.json()
    logging.info(f"Message not sent to {recipient_id}")
    logging.info(f"Status code: {r.status_code}")
    logging.info(f"Response: {r.json()}")
    return r.json()


def send_review_handler(message, recipient_id, staff_id):
    data = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": recipient_id,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "header": {"type": "text", "text": "Give your review"},
            "body": {"text": message},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": f"1:{staff_id}", "title": "⭐️"}},
                    {"type": "reply", "reply": {"id": f"3:{staff_id}", "title": "⭐️⭐️⭐️"}},
                    {"type": "reply", "reply": {"id": f"5:{staff_id}", "title": "🌟🌟🌟🌟🌟"}},
                ]
            },
            "footer": {"text": FOOTER_TEXT},
        },
    }
    logging.info(f"Sending message with button to {recipient_id}")
    r = requests.post(f"{url}", headers=headers, json=data)
    if r.status_code == 200:
        logging.info(f"Message sent to {recipient_id}")
        return r.json()
    logging.info(f"Message not sent to {recipient_id}")
    logging.info(f"Status code: {r.status_code}")
    logging.info(f"Response: {r.json()}")
    return r.json()


def send_message_confirm_name_with_button(message, recipient_id):
    data = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": recipient_id,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "header": {"type": "text", "text": "Almost done!"},
            "body": {"text": message},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": "correct_name", "title": "Yes ✅"}},
                    {"type": "reply", "reply": {"id": "incorrect_name", "title": "No ❌"}},
                ]
            },
            "footer": {"text": FOOTER_TEXT},
        },
    }
    logging.info(f"Sending message with button to {recipient_id}")
    r = requests.post(f"{url}", headers=headers, json=data)
    if r.status_code == 200:
        logging.info(f"Message sent to {recipient_id}")
        return r.json()
    logging.info(f"Message not sent to {recipient_id}")
    logging.info(f"Status code: {r.status_code}")
    logging.info(f"Response: {r.json()}")
    return r.json()


def get_conv_status(mobile):
    response = dynamodb.get_item(TableName=conv_status_table_name, Key={"phone": {"S": mobile}})
    # Check if the item was found
    if "Item" in response:
        item = response["Item"]
        # Process the item or return it as a response
        return item
    else:
        return None


def get_client(mobile):
    response = dynamodb.scan(
        TableName=client_table_name,
        FilterExpression="#phone = :phone",
        ExpressionAttributeNames={"#phone": "phone"},
        ExpressionAttributeValues={":phone": {"S": mobile}},
    )
    if response["Items"]:
        return response["Items"][0]["id"]["S"]
    else:
        return None


def delete_client(mobile):
    response = dynamodb.scan(
        TableName=client_table_name,
        FilterExpression="#phone = :phone",
        ExpressionAttributeNames={"#phone": "phone"},
        ExpressionAttributeValues={":phone": {"S": mobile}},
    )

    items = response["Items"]
    if len(items) > 0:
        for item in items:
            dynamodb.delete_item(TableName=client_table_name, Key={"id": item["id"]})

    dynamodb.delete_item(TableName=conv_status_table_name, Key={"phone": {"S": str(mobile)}})


def update_conv_status(mobile, new_status, new_data=None):
    # Start with the basic update expression components.
    update_expression = "SET #s = :new_status, #t = :new_time"
    expression_attribute_values = {
        ":new_time": {"S": datetime.now(time_zone).isoformat()},
        ":new_status": {"S": new_status},
    }
    expression_attribute_names = {"#t": "time", "#s": "status"}  # Initialize with required keys

    # Check if new data is provided and should be updated.
    if new_data is not None:
        update_expression += ", #d = :new_data"
        expression_attribute_values[":new_data"] = {"S": new_data}
        expression_attribute_names["#d"] = "data"  # Add this key only if new_data is provided

    dynamodb.update_item(
        TableName=conv_status_table_name,
        Key={"phone": {"S": str(mobile)}},
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values,
    )


def calculate_free_intervals(
    reservations,
    num_days=1,
    start_labor_time=8,
    end_labor_time=20,
    minutes_fraction=MINUTES_FOR_RESERVATIONS,
):
    free_intervals_all_days = []

    # Get the current time in the desired timezone
    now = datetime.now(time_zone)

    for day_offset in range(num_days):
        # Calculate the day for which to find free intervals
        day = now + timedelta(days=day_offset)
        # Get free intervals for the specific day
        free_intervals = calculate_free_intervals_by_day(
            reservations, day, start_labor_time, end_labor_time, minutes_fraction
        )

        # Combine intervals from all days
        free_intervals_all_days.extend(free_intervals)

    return free_intervals_all_days


def calculate_free_intervals_by_day(
    reservations,
    day,
    start_labor_time=8,
    end_labor_time=20,
    minutes_fraction=MINUTES_FOR_RESERVATIONS,
):
    # Ensure the day is timezone-aware
    if day.tzinfo is None or day.tzinfo.utcoffset(day) is None:
        day = day.replace(tzinfo=time_zone)

    # Define workday start and end times
    workday_start = datetime.combine(day, datetime.min.time()).replace(tzinfo=time_zone) + timedelta(
        hours=start_labor_time
    )
    workday_end = datetime.combine(day, datetime.min.time()).replace(tzinfo=time_zone) + timedelta(hours=end_labor_time)
    now = datetime.now(time_zone)

    # Adjust start time if it's today and after start labor time
    if now.date() == day.date() and now.time().hour > start_labor_time:
        workday_start = max(
            workday_start, now.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=minutes_fraction)
        )

    # Ensure workday_start does not exceed workday_end
    workday_start = min(workday_start, workday_end)

    # Sort reservations by start time
    sorted_reservations = sorted(
        reservations, key=lambda x: parser.isoparse(x["start_date"]["S"]).astimezone(time_zone)
    )

    # Initialize intervals
    intervals = []
    last_end_time = workday_start

    # Find free intervals
    for reservation in sorted_reservations:
        start_date = parser.isoparse(reservation["start_date"]["S"]).astimezone(time_zone)
        end_date = parser.isoparse(reservation["end_date"]["S"]).astimezone(time_zone)

        if start_date.date() == day.date():
            if start_date > last_end_time:
                intervals.append((last_end_time, min(start_date, workday_end)))
            last_end_time = max(last_end_time, end_date)

    if last_end_time < workday_end:
        intervals.append((last_end_time, workday_end))

    # Split intervals into desired fractions
    final_intervals = []
    for start, end in intervals:
        while start + timedelta(minutes=minutes_fraction) <= end:
            final_intervals.append((start, start + timedelta(minutes=minutes_fraction)))
            start += timedelta(minutes=minutes_fraction)

    return final_intervals


def get_availability_by_day(free_intervals):
    availability_by_day = {}
    for start, end in free_intervals:
        day = start.date()
        start_time = start.time()

        if day not in availability_by_day:
            availability_by_day[day] = {"morning": 0, "evening": 0, "night": 0}

        if time(6, 0) <= start_time < time(12, 0):
            availability_by_day[day]["morning"] += 1
        elif time(12, 0) <= start_time < time(18, 0):
            availability_by_day[day]["evening"] += 1
        else:
            availability_by_day[day]["night"] += 1
    return availability_by_day


def select_booking_day_handler(staff_id):
    response = dynamodb.query(
        TableName=book_table_name,
        IndexName="byStaff",
        KeyConditionExpression="staffID = :staffID",
        FilterExpression="#statusAttr = :activeStatus",
        ProjectionExpression="start_date, end_date",
        ExpressionAttributeNames={"#statusAttr": "status"},
        ExpressionAttributeValues={":staffID": {"S": staff_id}, ":activeStatus": {"S": "active"}},
    )
    reservations = response.get("Items", [])
    free_intervals = calculate_free_intervals(reservations, num_days=NUMBER_DAYS_TO_SHOW_RESERVATIONS)
    current_time = datetime.now(time_zone).replace(tzinfo=None)

    time_list = []
    # Calculate the number of free intervals per morning, evening and night.

    availability_by_day = get_availability_by_day(free_intervals)
    print("availability_by_day", availability_by_day)
    for day, availability in availability_by_day.items():
        print("day, availability", day, availability)
        morning_count = availability["morning"]
        evening_count = availability["evening"]
        night_count = availability["night"]

        availability_string = (
            f"Availability: {morning_count} at morning, " f"{evening_count} at evening, {night_count} at night"
        )
        time_list.append(
            {
                "id": "day_selected/" + staff_id + "/" + day.strftime("%Y-%m-%d"),
                "title": day.strftime("%A, %Y-%m-%d"),
                "description": availability_string,
            }
        )

    section1 = {"title": "NEXT DAYS", "rows": time_list}
    sections = [section1]
    return sections


def create_time_slots(free_intervals, start_hour, end_hour, staffID):
    return [
        {
            "id": "hour_selected/" + staffID + "/" + time[0].strftime("%H:%M"),
            "title": time[0].strftime("%H:%M"),
            "description": time[0].strftime("%H:%M") + "-" + time[1].strftime("%H:%M"),
        }
        for time in free_intervals
        if start_hour <= int(time[0].strftime("%H")) < end_hour
    ]


def process_intervals(free_intervals, staffID, mobile, is_full_day):
    sections = []
    time_ranges = [
        (0, 12, "Morning Hours"),
        (12, 18, "Afternoon Hours"),
        (18, 24, "Night Hours"),
    ]

    for start_hour, end_hour, title in time_ranges:
        list_msg = create_time_slots(free_intervals, start_hour, end_hour, staffID)
        if list_msg:
            section_title = "AVAILABLE HOURS" if not is_full_day else title
            section = {"title": section_title, "rows": list_msg}
            sections.append(section)
            if not is_full_day:
                send_message_list(title, mobile, "", title.upper(), [section])

    if is_full_day and sections:
        send_message_list("All Day Hours", mobile, "Booking time", "HOURS", sections)
    elif not sections:
        send_message("No Available Hours", mobile)


def create_conversation(client_phone, businessID):
    # Initialize DynamoDB
    dynamodb_resource = boto3.resource("dynamodb")
    table = dynamodb_resource.Table(conversation_table_name)

    # Define the time threshold for an active conversation
    active_conversation_threshold = datetime.now(time_zone) - timedelta(
        hours=24
    )  # Replace X with your threshold in hours

    # Scan for existing conversation
    response = table.scan(
        FilterExpression="client_phone = :cp and start_date >= :sd",
        ExpressionAttributeValues={
            ":cp": client_phone,
            ":sd": active_conversation_threshold.isoformat(),
        },
    )

    # If an active conversation exists, return its ID
    for item in response["Items"]:
        return item["id"]  # Assuming you want the first matching conversation

    # If no active conversation, create a new one
    conversation_id = str(uuid.uuid4())  # Generates a unique ID for the conversation
    start_date = datetime.now(time_zone).isoformat()

    table.put_item(
        Item={
            "id": conversation_id,
            "start_date": start_date,
            "client_phone": client_phone,
            "businessID": businessID,
            # 'end_date' and 'state' can be updated later when the conversation ends or state changes
        }
    )

    return conversation_id


def create_message(phone_sender, phone_receiver, content, conversation_id):
    message_id = str(uuid.uuid4())  # Generates a unique ID for the message
    date = datetime.now(time_zone).isoformat()
    dynamodb.put_item(
        TableName=message_table_name,
        Item={
            "id": {"S": message_id},
            "phone_sender": {"S": phone_sender},
            "phone_receiver": {"S": phone_receiver},
            "date": {"S": date},
            "content": {"S": content},
            "conversationID": {"S": conversation_id},
        },
    )


def get_messages_by_conversation_id(conversation_id):
    dynamodb = boto3.client("dynamodb")
    response = dynamodb.query(
        TableName=message_table_name,
        IndexName="byConversation",  # Using your GSI name
        KeyConditionExpression="conversationID = :conv_id",
        ExpressionAttributeValues={":conv_id": {"S": conversation_id}},
    )
    return response["Items"]


def get_message_content(messages, keywords):
    most_recent_messages = {}
    for item in messages:
        content = item["content"]["S"]
        content_parts = content.split(";")
        keyword = content_parts[0]

        if keyword in keywords:
            message_date = datetime.fromisoformat(item["date"]["S"])
            # Check if this keyword already has a message and if this one is more recent
            if keyword not in most_recent_messages or message_date > most_recent_messages[keyword]["date"]:
                most_recent_messages[keyword] = {
                    "keyword": keyword,
                    "id": content_parts[1] if len(content_parts) > 1 else None,
                    "message": item,
                    "date": message_date,
                }

    return list(most_recent_messages.values())


def combine_date_time(date_str, time_str, minutes_to_add):
    # Combine the date and time strings into a single datetime object
    date_time_str = f"{date_str} {time_str}"
    start_date = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M")

    # Add the specified number of minutes to get the end_date
    end_date = start_date + timedelta(minutes=minutes_to_add)

    return start_date, end_date


def is_staff_free(business_id, staff_id, start_date, end_date):
    dynamodb = boto3.client("dynamodb")

    # start_date_utc = start_date.astimezone(timezone.utc).isoformat()
    # end_date_utc = end_date.astimezone(timezone.utc).isoformat()

    start_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"
    end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"

    statement = f"""SELECT * FROM "{book_table_name}" WHERE staffID = ? AND businessID = ? AND ((start_date <= ? AND end_date > ?) OR (start_date < ? AND end_date >= ?))"""
    response = dynamodb.execute_statement(
        Statement=statement,
        Parameters=[
            {"S": staff_id},
            {"S": business_id},
            {"S": start_date_str},
            {"S": start_date_str},
            {"S": end_date_str},
            {"S": end_date_str},
        ],
    )
    items = response.get("Items", [])
    return len(items) == 0


def create_booking(business_id, client_id, service_id, staff_id, start_date_local, end_date_local, payment_id):
    # Convert local start and end dates to UTC
    start_date_utc = start_date_local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    end_date_utc = end_date_local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    booking_id = str(uuid.uuid4())  # Generates a unique ID for the booking
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    dynamodb.put_item(
        TableName=book_table_name,
        Item={
            "id": {"S": booking_id},
            "businessID": {"S": business_id},
            "clientID": {"S": client_id},
            "createdAt": {"S": current_time},
            "end_date": {"S": end_date_utc},
            "paymentID": {"S": payment_id},
            "serviceID": {"S": service_id},
            "staffID": {"S": staff_id},
            "start_date": {"S": start_date_utc},
            "updatedAt": {"S": current_time},
            "_lastChangedAt": {"N": str(int(datetime.now().timestamp() * 1000))},
            "_version": {"N": "1"},
            "__typename": {"S": "Book"},
            "status": {"S": "active"},  # Add status field
            "cancelledAt": {"NULL": True},  # Placeholder for cancellation time
        },
    )
    return booking_id


def update_booking_dates(booking_id, new_start_date_local, new_end_date_local):
    # Convert local start and end dates to UTC
    new_start_date_utc = new_start_date_local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    new_end_date_utc = new_end_date_local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    # Current time in UTC for the 'updatedAt' field
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    # Update the item in DynamoDB
    response = dynamodb.update_item(
        TableName=book_table_name,
        Key={"id": {"S": booking_id}},
        UpdateExpression="SET start_date = :new_start_date, end_date = :new_end_date, updatedAt = :updated_at",
        ExpressionAttributeValues={
            ":new_start_date": {"S": new_start_date_utc},
            ":new_end_date": {"S": new_end_date_utc},
            ":updated_at": {"S": current_time},
        },
    )
    return response


def cancel_booking(booking_id):
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    dynamodb.update_item(
        TableName=book_table_name,
        Key={"id": {"S": booking_id}},
        UpdateExpression="SET #status = :status, #cancelledAt = :cancelledAt, #updatedAt = :updatedAt",
        ExpressionAttributeNames={
            "#status": "status",  # Use placeholders for reserved keywords
            "#cancelledAt": "cancelledAt",
            "#updatedAt": "updatedAt",
        },
        ExpressionAttributeValues={
            ":status": {"S": "cancelled"},
            ":cancelledAt": {"S": current_time},
            ":updatedAt": {"S": current_time},
        },
    )
    return "Booking cancelled."


def get_staff_name_by_id(staff_id):
    dynamodb = boto3.client("dynamodb")
    response = dynamodb.get_item(TableName=staff_table_name, Key={"id": {"S": staff_id}})

    # Check if the item was found
    if "Item" in response:
        staff_name = response["Item"]["name"]["S"]
        return staff_name
    else:
        return None


def get_service_by_id(service_id):
    dynamodb = boto3.client("dynamodb")
    response = dynamodb.get_item(TableName=service_table_name, Key={"id": {"S": service_id}})

    # Check if the item was found
    if "Item" in response:
        service_name = response["Item"]["name"]["S"]
        service_price = response["Item"]["price"]["N"]
        return service_name, service_price
    else:
        return None


def create_stripe_payment(
    destination_account,
    product_name,
    product_price,
    app_fee_price,
    total_fee_price,
    description,
    expiration_minutes=30,
):
    # Calculate expiration time (10 minutes from now)
    expiration_time = datetime.now() + timedelta(minutes=expiration_minutes)
    expiration_timestamp = int(expiration_time.timestamp())  # Convert to Unix timestamp

    session_response = stripe.checkout.Session.create(
        mode="payment",
        line_items=[
            {
                "price_data": {
                    "currency": "usd",
                    "product_data": {
                        "name": product_name,
                    },
                    "unit_amount": round(product_price),
                },
                "quantity": 1,
            },
            {
                "price_data": {
                    "currency": "usd",
                    "product_data": {
                        "name": "App fee",
                    },
                    "unit_amount": round(app_fee_price),
                },
                "quantity": 1,
            },
        ],
        payment_intent_data={
            "application_fee_amount": round(total_fee_price),
            "transfer_data": {"destination": destination_account},
            "description": description,
        },
        success_url=SUCCESS_PAYMENT_URL,
        cancel_url=CANCEL_PAYMENT_URL,
        expires_at=expiration_timestamp,
    )
    return session_response


def get_staff_list(businessID):
    response = dynamodb.query(
        TableName=staff_table_name,
        IndexName="byBusiness",
        KeyConditionExpression="businessID = :businessID",
        ExpressionAttributeNames={"#nm": "name", "#deleted": "_deleted"},
        ExpressionAttributeValues={
            ":businessID": {"S": businessID},
            ":trueValue": {"BOOL": True},
        },
        FilterExpression="#deleted <> :trueValue",
        ProjectionExpression="#nm,phone,id",
    )
    staff_list = [{"id": "barber/" + item["id"]["S"], "item": item["name"]["S"]} for item in response["Items"]]
    return staff_list


def get_service_list(businessID, staffID):
    response = dynamodb.query(
        TableName=service_table_name,
        IndexName="byBusiness",
        KeyConditionExpression="businessID = :businessID ",
        FilterExpression="staffID = :staffID",
        ExpressionAttributeNames={"#nm": "name"},
        ExpressionAttributeValues={
            ":businessID": {"S": businessID},
            ":staffID": {"S": staffID},
        },
        ProjectionExpression="#nm,staffID,id,price,note,time_mins",
    )

    service_list = [
        {
            "id": "service/" + staffID + "/" + item["id"]["S"],
            "title": item["name"]["S"],
            "description": "$"
            + item["price"]["N"]
            + " ~ "
            + item["time_mins"]["N"]
            + "mins"
            + " - "
            + item["note"]["S"],
        }
        for item in response["Items"]
    ]
    return service_list


def set_profile_name(status_item, data, mobile):
    date_string = status_item["time"]["S"]
    # Convert the date string to a datetime object
    date_object = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f")
    # Calculate the time difference in minutes
    current_time = datetime.now(time_zone)
    minutes_difference = (current_time - date_object.replace(tzinfo=time_zone)).total_seconds() / 60
    # delete client if the last message (asking the name) was 12hrs ago
    if minutes_difference > 60 * HOURS_TO_DELETE_AFTER_ASKING_NAME:
        delete_client(mobile)
    message = data["Body"][0]

    if len(message.split(" ")) > 2:
        response = send_message(
            "Please provide only your name (1 or 2 words).\nWhat's your name?",
            mobile,
        )
    else:
        variables={"1":message}
        response = send_template_message("terms_and_conditions_en", variables, mobile, )
        update_conv_status(mobile, "confirm_name", message)
    return response


def select_booking_time(staff_id, day, mobile):
    current_date = datetime.utcnow().isoformat() + "Z"  # Adding 'Z' to indicate UTC time
    response = dynamodb.query(
        TableName=book_table_name,
        IndexName="byStaff",
        KeyConditionExpression="staffID = :staffID and start_date >= :current_date",
        FilterExpression="#statusAttr = :activeStatus",
        ProjectionExpression="start_date,end_date",
        ExpressionAttributeNames={"#statusAttr": "status"},
        ExpressionAttributeValues={
            ":staffID": {"S": staff_id},
            ":current_date": {"S": current_date},
            ":activeStatus": {"S": "active"},
        },
    )

    reservations = response.get("Items", [])
    free_intervals = calculate_free_intervals_by_day(reservations, day=datetime.strptime(day, "%Y-%m-%d"))

    if len(free_intervals) >= 10:
        process_intervals(free_intervals, staff_id, mobile, False)
    else:
        process_intervals(free_intervals, staff_id, mobile, True)


def handle_future_booking(data, mobile):
    parsed_start_date = datetime.strptime(data["start_date_local"], "%Y-%m-%dT%H:%M:%S%z")
    formatted_date = parsed_start_date.strftime("%B %d at %-I:%M%p")
    staff_name = get_staff_name_by_id(data["staff_id"])
    service_name, service_price = get_service_by_id(data["service_id"])
    message = f"Check a overview of your reservation\nDate: {formatted_date}\nStaff: {staff_name}\nService: {service_name}\nTotal: ${service_price}"
    send_booking_handler(message, data["booking_id"], data["staff_id"], mobile)


def handle_create_booking(mobile, event, service_id, start_day, start_hour, start_date, end_date):
    staff_name = get_staff_name_by_id(event["staffID"])
    service_name, service_price = get_service_by_id(service_id)
    formatted_price = f"${float(service_price):,.2f}"
    message_text = (
        f"Professional: {staff_name} \n"
        f"Service: {service_name}\n"
        f"Time: {start_day} at {start_hour}\n"
        "\n"
        f"Service price: {formatted_price}\nFee $1.00\n*Total*: ${float(service_price)+APP_FEE_PRICE:,.2f}\n\n"
    )
    client_id = get_client(mobile)
    start_date_local = start_date.replace(tzinfo=time_zone)
    end_date_local = end_date.replace(tzinfo=time_zone)

    booking_id = create_booking(
        business_id=event["businessID"],
        client_id=client_id,
        service_id=service_id,
        staff_id=event["staffID"],
        start_date_local=start_date_local,
        end_date_local=end_date_local,
        payment_id="to_be_defined",
    )
    status_string = {
        "booking_id": booking_id,
        "business_id": event["businessID"],
        "client_id": client_id,
        "service_id": service_id,
        "staff_id": event["staffID"],
        "start_date_local": start_date_local.isoformat(),
        "end_date_local": end_date_local.isoformat(),
        "price_to_pay": "to_be_defined",
    }
    update_conv_status(mobile, "booking_created", json.dumps(status_string))
    return message_text, service_name, service_price


def confirm_action(message, confirmation_id, recipient_id):
    data = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": recipient_id,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "header": {"type": "text", "text": "Confirmation"},
            "body": {"text": message},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": confirmation_id, "title": "Yes ✅"}},
                    {"type": "reply", "reply": {"id": "cancel_denied", "title": "No ❌"}},
                ]
            },
            "footer": {"text": FOOTER_TEXT},
        },
    }
    logging.info(f"Sending message with button to {recipient_id}")
    r = requests.post(f"{url}", headers=headers, json=data)
    if r.status_code == 200:
        logging.info(f"Message sent to {recipient_id}")
        return r.json()
    logging.info(f"Message not sent to {recipient_id}")
    logging.info(f"Status code: {r.status_code}")
    logging.info(f"Response: {r.json()}")
    return r.json()


def success_response(body):
    response_object = {}
    response_object["statusCode"] = 200
    response_object["headers"] = {}
    response_object["headers"]["Content-Type"] = "application/json"
    response_object["body"] = json.dumps(json.dumps(body))
    return response_object


def action_handler(data, event):
    mobile = get_mobile(data)
    if event["action"] == "perfil_created":
        if mobile:
            send_template_message("main_menu_en", {}, mobile)
            return True
    elif event["action"] == "booking" and "businessID" in event:
        mobile = get_mobile(data)
        if mobile:
            # Create a new message and link it to the created conversation
            conversation_id = create_conversation(mobile, event["businessID"])
            create_message(mobile, RECEIVER_PHONE_NUMBER, "booking", conversation_id)
            staff_list = get_staff_list(event["businessID"])
            if staff_list:
                send_list_picker_content(mobile,staff_list,"Select a professional to see all the services available.", "Select")
            else:
                send_message("Sorry, there is not staff available.", mobile)
            return True
    elif event["action"] == "service" and event["businessID"] and event["staffID"]:
        mobile = get_mobile(data)
        if mobile:
            conversation_id = create_conversation(mobile, event["businessID"])
            message_content = f"professional;{event['staffID']}"
            create_message(mobile, RECEIVER_PHONE_NUMBER, message_content, conversation_id)

            service_list = get_service_list(event["businessID"], event["staffID"])

            if not service_list:
                send_message(
                    "Currently, there are no available services for this staff member.",
                    mobile,
                )
                staff_list = get_staff_list(event["businessID"])
                response = send_message_list_barbers(
                    "Please select another professional to see all the services available.",
                    mobile,
                    staff_list,
                )
            else:
                response = send_message_list_services(
                    "Select a service to see available times.",
                    mobile,
                    service_list,
                )

            return True
    elif event["action"] == "select_day" and event["businessID"] and event["staffID"] and event["serviceID"]:
        mobile = get_mobile(data)
        if mobile:
            conversation_id = create_conversation(mobile, event["businessID"])
            message_content = f"service;{event['serviceID']}"
            create_message(mobile, RECEIVER_PHONE_NUMBER, message_content, conversation_id)
            print("staff Id at slect_day", event["staffID"])
            sections = select_booking_day_handler(event["staffID"])
            send_message_list("Select a day", mobile, "Booking time", "DAYS", sections)
        return True
    elif event["action"] == "select_time" and event["businessID"] and event["staffID"] and event["day"]:
        mobile = get_mobile(data)
        if mobile:
            conversation_id = create_conversation(mobile, event["businessID"])
            message_content = f"select_day;{event['day']}"
            create_message(mobile, RECEIVER_PHONE_NUMBER, message_content, conversation_id)
            select_booking_time(event["staffID"], event["day"], mobile)
            return True
    elif event["action"] == "select_hour" and event["businessID"] and event["staffID"] and event["hour"]:
        mobile = get_mobile(data)
        if mobile:
            conversation_id = create_conversation(mobile, event["businessID"])
            message_content = f"select_hour;{event['hour']}"
            create_message(mobile, RECEIVER_PHONE_NUMBER, message_content, conversation_id)

            conversation = get_messages_by_conversation_id(conversation_id)

            messages_formated = get_message_content(
                conversation,
                ["professional", "select_day", "service", "select_hour"],
            )

            message_text = "\n ".join([f"{message['keyword']}:{message['id']}" for message in messages_formated])

            start_day = None
            start_hour = None
            service_id = None
            for message in messages_formated:
                if message["keyword"] == "select_day":
                    start_day = message["id"]
                if message["keyword"] == "select_hour":
                    start_hour = message["id"]
                if message["keyword"] == "service":
                    service_id = message["id"]

            start_date, end_date = combine_date_time(start_day, start_hour, MINUTES_FOR_RESERVATIONS)
            if is_staff_free(event["businessID"], event["staffID"], start_date, end_date):
                conv_status = get_conv_status(mobile)
                if conv_status["status"]["S"] == "edit_booking":
                    booking_data = json.loads(conv_status["data"]["S"])
                    update_booking_dates(booking_data["booking_id"], start_date, end_date)
                    #  update conversation satatus
                    start_date_local = start_date.replace(tzinfo=time_zone)
                    end_date_local = end_date.replace(tzinfo=time_zone)
                    booking_data["start_date_local"] = start_date_local.isoformat()
                    booking_data["end_date_local"] = end_date_local.isoformat()
                    update_conv_status(mobile, "booking_created", json.dumps(booking_data))
                    send_message(
                        f"Your reservation has been moved successfully to {start_date_local.strftime('%B %d at %-I:%M%p')}.",
                        mobile,
                    )
                    return True
                message_text, service_name, service_price = handle_create_booking(
                    mobile, event, service_id, start_day, start_hour, start_date, end_date
                )
            else:
                send_message(
                    "Sorry, That time has just been taken. Please select another date",
                    mobile,
                )
                select_booking_time(event["staffID"], start_date.strftime("%Y-%m-%d"), mobile)
                return True

            # response=send_message("https://buy.stripe.com/test_14k3cT1jHcEA13yfYY",mobile)

            # ============ CREATE ACCOUNT =============
            # stripe_response=stripe.Account.create(type="express")
            # account_link=stripe.AccountLink.create(
            #   account=stripe_response.id,
            #   refresh_url="https://example.com/reauth",
            #   return_url="https://example.com/return",
            #   type="account_onboarding",
            # )
            # print("----account_link.url",account_link.url)

            # resp=send_message_url("Finish the reserve",account_link.url,mobile)

            total_fee_price = 100 * float(service_price) * STRIPE_PERCENT_FEE + STRIPE_BASE_FEE + APP_FEE_PRICE
            session_response = create_stripe_payment(
                "acct_1OOsLg2Un73X5rjO",
                service_name,
                float(service_price) * 100,
                APP_FEE_PRICE,
                total_fee_price,
                message_text,
            )
            resp = send_message_url(
                "Confirmation",
                message_text + "If the everything is correct, proceed to the payment.",
                session_response.url,
                mobile,
            )
            return True

        # hour_selected/" + staffID + "/" + time[0].strftime("%H:%M")
    elif event["action"] == "cancel_booking" and event["businessID"] and event["type"] and event["booking_id"]:
        mobile = get_mobile(data)
        conversation_id = create_conversation(mobile, event["businessID"])
        message_content = f"cancel_booking;{event['booking_id']}"
        create_message(mobile, RECEIVER_PHONE_NUMBER, message_content, conversation_id)
        confirmation_message = "Are you sure you want to cancell your reservation?"
        confirmation_id = f"cancel_confirm/{event['booking_id']}"
        confirm_action(confirmation_message, confirmation_id, mobile)
        return True
    elif event["action"] == "cancel_confirm" and event["businessID"] and event["booking_id"]:
        mobile = get_mobile(data)
        conversation_id = create_conversation(mobile, event["businessID"])
        message_content = f"cancel_confirm;{event['booking_id']}"
        create_message(mobile, RECEIVER_PHONE_NUMBER, message_content, conversation_id)
        cancel_booking(event["booking_id"])
        update_conv_status(mobile, "booking_cancelled", event["booking_id"])
        send_message("Your reservation has been cancelled.", mobile)
        return True
    elif event["action"] == "edit_booking" and event["businessID"] and event["staff_id"]:
        mobile = get_mobile(data)
        conversation_id = create_conversation(mobile, event["businessID"])
        create_message(mobile, RECEIVER_PHONE_NUMBER, "edit_booking", conversation_id)
        sections = select_booking_day_handler(event["staff_id"])
        update_conv_status(mobile, "edit_booking")
        send_message_list("Select a day", mobile, "Booking time", "DAYS", sections)


def text_handler(data, event):
    mobile = get_mobile(data)
    if mobile:
        phone_number = mobile
        try:
            # Check if the record exists
            response = dynamodb.query(
                TableName=client_table_name,
                IndexName="phone-index",
                KeyConditionExpression="phone = :phone",
                ExpressionAttributeNames={"#nm": "name"},
                ExpressionAttributeValues={":phone": {"S": phone_number}},
                ProjectionExpression="#nm,phone",
            )
            
            if response["Count"] == 0:
                variables={"1":"Terms+and+conditions.pdf"}
                response = send_template_message("terms_and_conditions_en", variables, mobile)
            else:
                items = response["Items"]
                for item in items:
                    name = item["name"]["S"]
                    if response["Count"] == 1 :
                        # CHECK THE STATUS OF CONVERSATION
                        status_item = get_conv_status(mobile)
                        if status_item is not None:
                            if status_item["status"]["S"] == "profile_name":
                                response = set_profile_name(status_item, data, mobile)
                            elif status_item["status"]["S"] == "booking_created":
                                data = json.loads(status_item["data"]["S"])
                                parsed_start_date = datetime.strptime(data["start_date_local"], "%Y-%m-%dT%H:%M:%S%z")
                                # booking is pending
                                if parsed_start_date > datetime.now(time_zone):
                                    conversation_id = create_conversation(mobile, event["businessID"])
                                    create_message(mobile, RECEIVER_PHONE_NUMBER, "hanlde_booking", conversation_id)
                                    handle_future_booking(data, mobile)
                                else:  # booking passed
                                    conversation_id = create_conversation(mobile, event["businessID"])
                                    create_message(mobile, RECEIVER_PHONE_NUMBER, f"review_sent:{data['staff_id']}", conversation_id)
                                    update_conv_status(mobile, "review_sent", f"{status_item['data']['S']}")
                                    staff_name = get_staff_name_by_id(data["staff_id"])
                                    send_review_handler(
                                        f"How was your last reservation with {staff_name}?",
                                        mobile,
                                        data["staff_id"],
                                    )
                            else:
                                try:
                                    conversation_id = create_conversation(mobile, event["businessID"])
                                    create_message(mobile, RECEIVER_PHONE_NUMBER, "menu", conversation_id)
                                    response = send_template_message("main_menu_en", {}, mobile)
                                except Exception as e:
                                    print(response)
                                    logging.error("Error at send message menu for profile created", e)
                                return True
                    elif response["Count"] > 1:
                        logging.error(f"Multiple clients with that phone number {mobile}")

        except Exception as e:
            print(f"Error checking for the record: {str(e)}")
            raise e
        return success_response(response)
    else:
        delivery = get_delivery(data)
        if delivery:
            print(f"Message : {delivery}")
        else:
            print("No new message")
    return success_response("ok")


def lambda_handler(event, context):
    print(event)
    data = parse_incoming_twilio_event(event)
    if "action" in event:
        return action_handler(data, event)
    else:
        return text_handler(data, event)
