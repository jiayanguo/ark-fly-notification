from datetime import date, timedelta, datetime
from pytz import timezone
import io
import csv
import boto3
import codecs
from botocore.exceptions import ClientError
from googleapiclient import errors, discovery
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from email.mime.text import MIMEText
import base64
from oauth2client import client, tools, file
import httplib2

OBJECT_KEY_PATTERN="newholdings/{today}-trading.csv"
SEND_NOTIFICATION_TO = "guojiayanc@gmail.com"
SENDER = "NO REPLY <jiayan.guo@outlook.com>"
S3_BUCKET ="ark-fly"

def get_from_s3(object_name):
    client = boto3.client('s3')
    try:
        response = client.get_object(Bucket = S3_BUCKET, Key = object_name)
        return response
    except ClientError as ex:
        raise

def get_csv(object_key):
    obj = get_from_s3(object_key)
    result = []
    for row in csv.reader(codecs.getreader("utf-8")(obj["Body"])):
        result.append(row)
    return result

def build_html_table(object_key):
    html_table = "<table border='1'><thead><tr>"
    data = get_csv(object_key)
    for header in data[0]:
        html_table += "<th>{col}</th>".format(col=header)
    html_table += "</tr></thead><tbody>"
    for row in range(1, len(data)):
        html_table += "<tr>"
        for col in data[row]:
            html_table += "<td>{col}</td>".format(col=col)
        html_table += "</tr>"
    html_table += "</tbody></table>"
    return html_table

def get_credentials():
  client_id = os.environ['GMAIL_CLIENT_ID']
  client_secret = os.environ['GMAIL_CLIENT_SECRET']
  refresh_token = os.environ['GMAIL_REFRESH_TOKEN']

  credentials = client.GoogleCredentials(None,
  client_id,
  client_secret,
  refresh_token,
  None,
  "https://accounts.google.com/o/oauth2/token",
  'my-user-agent')
  return credentials
    
def main(key):
    date = get_date()
    html_table = build_html_table(object_key)
    email_message = "Ark opened below new holdings on " + date + "\n" + html_table
    
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('gmail', 'v1', http=http, cache_discovery=False)
    message = create_message(SENDER, SEND_NOTIFICATION_TO, "ARK new holdings", email_message)
    send_message(service, 'me', message)

def get_date():
    tz = timezone('EST')
    today = datetime.now(tz).strftime("%Y-%m-%d")
    return today

def create_message(sender, to, subject, message_text):
    message = MIMEText(message_text, 'html')
    message['to'] = to
    # message['from'] = sender
    message['subject'] = subject
    raw_message = base64.urlsafe_b64encode(message.as_string().encode("utf-8"))
    return {
        'raw': raw_message.decode("utf-8"),
        'payload': {'mimeType': 'text/html'}
  }

def send_message(service, user_id, message):
  try:
    message = service.users().messages().send(userId=user_id, body=message).execute()
    print('Message Id: %s' % message['id'])
    return message
  except Exception as error:
    print('An error occurred: %s' % error)
    raise error

if __name__ == '__main__':
    main(OBJECT_KEY_PATTERN.format(today="2020-12-24"))

def lambda_handler(event, context):
    try:
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        main(key)
    except Exception as error:
        print("Failed to process trading information " + str(error))
    return {
        "status":200
    }