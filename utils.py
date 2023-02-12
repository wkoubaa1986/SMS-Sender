import requests
import json
import re

AUTH_TOKEN_URL = 'https://api.orange.com/oauth/v3/token'


class SMS:
    def __init__(self, AUTH_TOKEN, SENDER_NAME):
        """
        AUTH_TOKEN: str-> required.
            This is the Authorization header you will copy from orage developer console.
            It should be of the form "Basic XXXXXXXXXX..."
        SENDER_NAME: str -> required
            This is the name of your mobile app. It is set up in the console when you create the app.
        """
        if not str(AUTH_TOKEN).startswith('Basic '):
            raise Exception(f"Invalid AUTH_TOKEN: AUTH_TOKEN == '{AUTH_TOKEN}' must start with 'Basic '")
        if SENDER_NAME is None or len(SENDER_NAME) == 0:
            raise Exception(f"Invalid SENDER_NAME. SENDER_NAME must be a non empty string.")
        self.AUTH_TOKEN = AUTH_TOKEN
        self.SENDER_NAME = SENDER_NAME

    def get_access_token(self):
        """
        It is a good idea to always save the access keys since they are valid for 1 hour.
        You could also set up somthing like a cloud function that runs every hour to update the access_keys and just read it from database.
        """
        headers = {
            "Authorization": self.AUTH_TOKEN,
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        }
        data = {"grant_type": "client_credentials"}
        r = requests.post(AUTH_TOKEN_URL, headers=headers, data=data)
        if r.status_code == 200:
            return r.json()['access_token']
        raise Exception(f"Failed with following response: '{r.text}'")

    def send_sms(self, dev_phone_number, recipient_phone_number, message):
        """
        All parameters are required
        params:
            dev_phone_number: str -> required
                This is the developer's phone number which was used when activating sms.
                Must be formatted as an international phone number beginning matching '[1-9][\d]{10,14}'
            recipient_phone_number: str -> required
                This is the phone number you want to sms to.
                Must be formatted as an international phone number beginning matching '[1-9][\d]{10,14}'
            message: str -> required
                This is the sms message to be sent to the recipient_phone_number by dev_phone_number.
        return: class `requests.Response`
            You must handle the response to see if the message was sent or not.
        """
        if not re.match('^[1-9][\d]{10,14}$', dev_phone_number):
            raise Exception(f"Invalid formate of dev_phone_number. {dev_phone_number} must match regex" + " '[1-9][\d]{10,14}'")
        if not re.match('^[1-9][\d]{10,14}$', recipient_phone_number):
            raise Exception(f"Invalid formate of recipient_phone_number. {recipient_phone_number} must match regex" + " '[1-9][\d]{10,14}'")
        if message is None or len(str(message)) == 0:
            raise Exception("Invalid sms message to send. You must provide a non empty string.")
        send_sms_url = f"https://api.orange.com/smsmessaging/v1/outbound/tel%3A%2B{dev_phone_number}/requests"
        access_token = self.get_access_token()
        headers = {
            "Authorization": "Bearer " + access_token,
            "Content-Type": "application/json",
        }
        data = {
            "outboundSMSMessageRequest":{
                "address": "tel:+" + recipient_phone_number,
                "senderAddress": "tel:+" + dev_phone_number,
                "senderName": self.SENDER_NAME,
                "outboundSMSTextMessage":{ "message": message }
            }
        }
        data = json.dumps(data)
        r = requests.post(send_sms_url, headers=headers, data=data)
        return  r
    def getUsageStats(self):
        access_token=self.get_access_token()
        response = requests.get('https://api.orange.com/sms/admin/v1/statistics',
        headers={
            "Authorization": "Bearer " + access_token}
        )
        stats = response.json()

        return stats
    def showBalanceSMS(self):
        access_token=self.get_access_token()
        response = requests.get('https://api.orange.com/sms/admin/v1/contracts', 
        headers={
            "Authorization": "Bearer " + access_token
        }
        )
        balance = response.json()
       #print(balance)

        return balance