import json
import logging
import requests
import datetime
import boto3
from boto3.dynamodb.conditions import Key, Attr
import os
import re
from datetime import date
import calendar
from twilio.rest import Client
from urllib.parse import parse_qs


lambda_client = boto3.client("lambda")

TOKEN = os.getenv("TOKEN")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
phone_number_id = 524839744037877
base_url = "https://graph.facebook.com/v17.0"
url = f"{base_url}/{phone_number_id}/messages"
dynamodb = boto3.client("dynamodb")
conv_status_table_name = "conversation_status_1"
client_table_name = "Client-tp3xjyxjoffklgjrporm4ed4ay-staging"
staff_table_name = "Staff-tp3xjyxjoffklgjrporm4ed4ay-staging"
FOOTER_TEXT = "BotorAI"

businessID = "25d8cb1c-fd12-4fb5-8c11-86829324cc1d"
headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer {}".format(TOKEN),
}


sqs_client = boto3.client("sqs")


def send_to_sqs(event, queue_url, mobile):
    try:
        response = sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(event), MessageGroupId=mobile)
        logging.info(f"Message sent to SQS queue {queue_url} successfully.")
    except Exception as e:
        logging.error(f"Error sending message to SQS queue:{e}")


def invoke_check_client(event, mobile):
    queue_url = os.getenv("TASK_QUEUE_URL")

    event["type"] = "checkClient"
    send_to_sqs(event, queue_url, mobile)


def invoke_create_profile(event, mobile):
    queue_url = os.getenv("TASK_QUEUE_URL")
    event["type"] = "createClient"
    send_to_sqs(event, queue_url, mobile)


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

    delete_item_resp = dynamodb.delete_item(TableName=conv_status_table_name, Key={"phone": {"S": str(mobile)}})
    return delete_item_resp


def get_name_from_conv_status(mobile):
    try:
        response = dynamodb.query(
            TableName=conv_status_table_name,
            KeyConditionExpression="#p = :phone",
            ExpressionAttributeNames={"#p": "phone", "#d": "data"},
            ExpressionAttributeValues={":phone": {"S": str(mobile)}},
            ProjectionExpression="#d",
        )
    except Exception as e:
        print(e)
        raise e

    if response["Count"] > 0:
        item = response["Items"][0]
        profile_name = item["data"]["S"]
        return profile_name
    else:
        return None


def update_client_name(mobile):
    new_name = get_name_from_conv_status(mobile)
    if new_name:
        try:
            response = dynamodb.query(
                TableName=client_table_name,
                IndexName="phone-index",
                KeyConditionExpression="phone = :phone",
                ExpressionAttributeValues={":phone": {"S": mobile}},
                ProjectionExpression="id",
            )  # Replace 'primaryKey' with the actual attribute name of the primary key

            # Step 2: Update the item using the retrieved primary key
            primary_key = response["Items"][0]["id"]  # Assuming there is only one item returned

            response = dynamodb.update_item(
                TableName=client_table_name,
                Key={"id": primary_key},
                UpdateExpression="SET #n=:new_name, #a = :new_ad_status",
                ExpressionAttributeNames={"#n": "name", "#a": "active_ad"},
                ExpressionAttributeValues={":new_name": {"S": new_name}, ":new_ad_status": {"BOOL": True}},
            )
        except Exception as e:
            print("error updating", e)
            raise e

        return new_name
    else:
        raise "Error finding the name of client"


def update_conv_status(mobile, new_status, new_data):
    dynamodb.update_item(
        TableName=conv_status_table_name,
        Key={
            "phone": {"S": str(mobile)},
        },
        UpdateExpression="SET #s=:new_status, #t = :new_time, #d = :new_data",
        ExpressionAttributeNames={"#t": "time", "#d": "data", "#s": "status"},
        ExpressionAttributeValues={
            ":new_time": {"S": datetime.datetime.now().isoformat()},
            ":new_data": {"S": new_data},
            ":new_status": {"S": new_status},
        },
    )


def message_from_staff(mobile, business_id):
    statement = f"""SELECT * FROM "{staff_table_name}" WHERE phone = ? AND businessID = ?"""
    response = dynamodb.execute_statement(
        Statement=statement,
        Parameters=[
            {"S": mobile},
            {"S": business_id},
        ],
    )
    items = response.get("Items", [])
    if len(items):
        return items[0]
    return False


def get_staff_id_by_name(staff_name, business_id, staff_table_name):
    try:
        # Correctly formatted query for PartiQL (assuming staff_table_name is correctly handled)
        statement = f"""SELECT * FROM "{staff_table_name}" WHERE name = ? AND businessID = ?"""
        response = dynamodb.execute_statement(
            Statement=statement,
            Parameters=[
                {"S": staff_name},
                {"S": business_id},
            ],
        )
        items = response.get("Items", [])
        if items:
            return items[0]
        return False
    except Exception as error:
        # Corrected logging statement with error handling
        print("=====err======", error)
        return False


def success_response(body):
    response_object = {}
    response_object["statusCode"] = 200
    response_object["headers"] = {}
    response_object["headers"]["Content-Type"] = "application/json"
    response_object["body"] = json.dumps(json.dumps(body))
    return response_object


def extract_review_name(sentence):
    match = re.search(r"provided by (.+)", sentence)
    if match:
        name = match.group(1)
        return name
    else:
        return "Name not found"



def get_staff_name_by_id(staff_id):
    dynamodb = boto3.client("dynamodb")
    response = dynamodb.get_item(TableName=staff_table_name, Key={"id": {"S": staff_id}})

    # Check if the item was found
    if "Item" in response:
        staff_name = response["Item"]["name"]["S"]
        return staff_name
    else:
        return None


def handle_create_review(message, mobile):
    staff_name = extract_review_name(message)
    staff = get_staff_id_by_name(staff_name, businessID, staff_table_name)
    send_review_handler(f"Sure! How was the service with {staff['name']['S']}?", mobile, staff["id"]["S"])


from twilio_utils import parse_incoming_twilio_event, get_mobile, send_message

def lambda_handler(event, context):
    
    data = parse_incoming_twilio_event(event)
    mobile = get_mobile(data)
        
        try:
                if "ListId" in data:
                    answer_id = data['ListId'][0]
                else:
                    answer_id = data['ButtonPayload'][0]
                    
                answer_split_len = len(answer_id.split("/"))
                answer_split_topic = answer_id.split("/")[0]
                if answer_id == "accept_terms":
                    invoke_create_profile(event, mobile)
                elif answer_id == "correct_name":
                    update_client_name(mobile)
                    event["action"] = "perfil_created"
                    update_conv_status(mobile, "profile_created", "")
                    send_message(f"*Great!*\nYour profile is compleated.", mobile)
                    invoke_check_client(event, mobile)
                elif answer_id == "incorrect_name":
                    invoke_create_profile(event, mobile)
                elif answer_split_len >= 1:
                    if answer_split_topic == "booking":
                        # TODO: start a conversation here
                        event["action"] = "booking"
                        event["businessID"] = businessID
                        invoke_check_client(event, mobile)
                    elif answer_split_topic == "barber":
                        event["action"] = "service"
                        event["businessID"] = businessID
                        event["staffID"] = answer_id.split("/")[1]
                        invoke_check_client(event, mobile)
                    elif answer_split_topic == "service":
                        event["action"] = "select_day"
                        event["businessID"] = businessID
                        event["staffID"] = answer_id.split("/")[1]
                        event["serviceID"] = answer_id.split("/")[2]
                        invoke_check_client(event, mobile)
                    elif answer_split_topic == "day_selected":
                        event["action"] = "select_time"
                        event["businessID"] = businessID
                        event["staffID"] = answer_id.split("/")[1]
                        event["day"] = answer_id.split("/")[2]
                        invoke_check_client(event, mobile)
                    elif answer_split_topic == "hour_selected":
                        event["action"] = "select_hour"
                        event["businessID"] = businessID
                        event["staffID"] = answer_id.split("/")[1]
                        event["hour"] = answer_id.split("/")[2]
                        invoke_check_client(event, mobile)
                    elif answer_split_topic == "future_booking":
                        if answer_id.split("/")[1] == "cancel":
                            event["action"] = "cancel_booking"
                            event["businessID"] = businessID
                            event["type"] = answer_id.split("/")[1]
                            event["booking_id"] = answer_id.split("/")[2]
                            invoke_check_client(event, mobile)
                        elif answer_id.split("/")[1] == "edit_date":
                            event["action"] = "edit_booking"
                            event["businessID"] = businessID
                            event["type"] = answer_id.split("/")[1]
                            event["staff_id"] = answer_id.split("/")[2]
                            invoke_check_client(event, mobile)
                    elif answer_split_topic == "cancel_confirm":
                        event["action"] = "cancel_confirm"
                        event["businessID"] = businessID
                        event["booking_id"] = answer_id.split("/")[1]
                        invoke_check_client(event, mobile)
                    elif answer_split_topic == "review":
                        event["action"] = "cancel_confirm"
                        event["businessID"] = businessID
                        event["rate"] = answer_id.split("/")[1]
                        event["staff_id"] = answer_id.split("/")[2]
                        send_message("Thanks for your review!", mobile)
                        staff_name = get_staff_name_by_id(event["staff_id"])
                        send_reminder_handler(
                            f"Would you like me to remind you of an upcoming service with {staff_name}?",
                            mobile,
                            event["staff_id"],
                        )
                    elif answer_split_topic == "reminder":
                        send_message("Great, we'll talk until then",mobile)
        except Exception as e:
            print(" --- not a interactive answer ---", e)
            try:
                    message = data["Body"][0]
                    splited_message = message.upper().split(" ")
                    if "DELETECLIENT" in splited_message and "CRACK" in splited_message:
                        try:
                            response = delete_client(mobile)
                            send_message(f"Deleated.", mobile)
                        except Exception as e:
                            send_message(f"Deleated error.", mobile)
                    elif "SHOWSTATUS" in splited_message and "crack" in splited_message:
                        send_message(f"After accepted terms: profile_name.", mobile)
                    elif "SERVICE" in splited_message and "PROVIDED" in splited_message and "BY":
                        handle_create_review(message, mobile)
                    elif "STOCKS" in splited_message or "BOTOR" in splited_message:
                        invoke_trading(message, mobile)
                    else:
                        event["businessID"] = businessID
                        invoke_check_client(event, mobile)
            except Exception as realError:
                print("=====ERROR=======", realError)
        print("LAMBDA ATENDED")
        return success_response("ok")
