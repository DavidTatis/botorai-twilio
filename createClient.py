import json
import logging
import requests
import boto3
import datetime
import uuid
import os
from twilio_utils import parse_incoming_twilio_event, get_mobile, send_message, send_template_message


TOKEN = os.getenv("TOKEN")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")

phone_number_id = 524839744037877

base_url = "https://graph.facebook.com/v17.0"
url = f"{base_url}/{phone_number_id}/messages"

headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer {}".format(TOKEN),
}

dynamodb = boto3.client("dynamodb")
# Name of the DynamoDB table
client_table_name = "Client-tp3xjyxjoffklgjrporm4ed4ay-staging"
conv_status_table_name = "conversation_status_1"


def interactive_answer(data):
    logging.error("interactive")
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




def update_conv_status(mobile, new_status, new_data):
    dynamodb.update_item(
        TableName=conv_status_table_name,
        Key={"phone": {"S": str(mobile)}, "status": {"S": "profile_name"}},
        UpdateExpression="SET #s=:new_status, #t = :new_time, #d = :new_data",
        ExpressionAttributeNames={"#t": "time", "#d": "data", "#s": "status"},
        ExpressionAttributeValues={":new_time": {"S": datetime.datetime.now().isoformat()}, ":new_data": {"S": new_data}, ":new_status": {"S": new_status}},
    )


def lambda_handler(event, context):
    data = parse_incoming_twilio_event(event)
    mobile = get_mobile(data)
    answer_id = data['ButtonPayload'][0]
    if answer_id == "incorrect_name":
        try:
            dynamodb.put_item(TableName=conv_status_table_name, Item={"phone": {"S": str(mobile)}, "status": {"S": "profile_name"}, "time": {"S": datetime.datetime.now().isoformat()}})
        except Exception as e:
            raise f"at put in dynamo {e}"

        # create client in the DB
        response = send_message("*No problem!*\nWhat's your name? ", mobile)

    else:
        item = {"id": {"S": str(uuid.uuid4())}, "name": {"S": "None"}, "phone": {"S": str(mobile)}, "active_ad": {"BOOL": False}}
        try:
            dynamodb.put_item(TableName=client_table_name, Item=item)
            dynamodb.put_item(TableName=conv_status_table_name, Item={"phone": {"S": str(mobile)}, "status": {"S": "profile_name"}, "time": {"S": datetime.datetime.now().isoformat()}})
        except Exception as e:
            raise f"at dobule puts {e}"

        # create client in the DB
        response = send_message("*Perfect!* \nI only need your name and your registration is compleated!🚀\nWhat's your name? ", mobile)

    responseObject = {}
    responseObject["statusCode"] = 200
    responseObject["headers"] = {}
    responseObject["headers"]["Content-Type"] = "application/json"
    responseObject["body"] = json.dumps(json.dumps(response))
    return responseObject
    
    return responseObject
