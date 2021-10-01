from typing import List
import logging

import azure.functions as func
import json

# from azure.cosmos import CosmosClient
from azure.eventhub import EventHubProducerClient, EventData

import datetime
import pytz
import psycopg2
# endpoint = "https://brt.documents.azure.com:443/"
# key = "wBK6ihuuWPkWDc7lfFD9aP8Y4bbe9OAbzA6jVaYqX5v4wmWITjVYvgelGXJIyVN6i1c62JpLwt6j7s2OZfpIlA=="
# database_name = "BRT_Rroute"
# container_name = "depart_and_door"

POSTGRESQL_URL = "captain.postgres.database.azure.com"
POSTGRESQL_DBNAME = "captain2"
POSTGRESQL_USER = "captain2@captain"
POSTGRESQL_PASSWORD = "p@ssw0rd!@#"
POSTGRESQL_PORT = "5432"

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://departuredoor.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=bHw7hnkg42aN4GpPB6ttBR7DR8NaeKaKHfu2K8nhig0="
EVENT_HUB_NAMESPACE = "departureanddoor"


def main(events: List[func.EventHubEvent], documents: func.DocumentList):
    # client = CosmosClient(endpoint, key)
    # database = client.get_database_client(database_name)
    # container = database.get_container_client(container_name)
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAMESPACE)
    conn = psycopg2.connect(host=POSTGRESQL_URL,
                            dbname=POSTGRESQL_DBNAME,
                            user=POSTGRESQL_USER,
                            password=POSTGRESQL_PASSWORD,
                            port=POSTGRESQL_PORT)
    conn.autocommit = True
    cur = conn.cursor()
    prev_record = {x['id']: dict(x) for x in documents}
    for event in events:
        # logging.info('Python EventHub trigger processed an event: %s',
        #  event.get_body().decode('utf-8'))
        pvsd = json.loads(event.get_body().decode('utf-8'))
        vehicle_state = pvsd['value']['regional'][0]['regExtValue']['captain']['snapshot'][0]['currentBusDrivingInfo']['cavCurrentProvidedInfo']['busTripStatus']['stationTripState']

        vehicle_id = pvsd['value']['regional'][0]['regExtValue']['captain']['vehicleID']['vehicleNumber']
        driving_state = vehicle_state['state']
        doorOpen = vehicle_state['doorOpen']
        line_number = pvsd['value']['regional'][0]['regExtValue']['captain']['snapshot'][0]['currentBusDrivingInfo']['cavCurrentProvidedInfo']['routeID']['lineNumber']

        curlink = pvsd['value']['regional'][0]['regExtValue']['captain']['snapshot'][0]['currentBusDrivingInfo']['cavCurrentProvidedInfo']['linkInfo']
        linkID = curlink['linkCodeInfo']['layerCode'] +\
            curlink['linkCodeInfo']['creationDate'] +\
            curlink['linkCodeInfo']['classificationCode'] +\
            str(curlink['uniqueNumber']).zfill(6)

        try:
            cur.execute(f"SELECT stationid \
                                FROM koti.brt_route\
                                WHERE hdlinkid = '{linkID}'")

            stationid = cur.fetchall()[0][0]
        except:
            stationid = 0
            import traceback
            traceback.print_exc()

        timestamp = datetime.datetime.now(pytz.timezone("Asia/Seoul"))
        timestamp = timestamp.strftime("%Y%m%d %H%M%S.%f")

        record = False
        if (prev_record[vehicle_id]['drivingState'] == 'finish') & (driving_state == 'start'):
            record = True

        if (prev_record[vehicle_id]['drivingState'] == 'start') & (driving_state == 'finish'):
            record = True

        if (prev_record[vehicle_id]['drivingState'] == 'moving') & (driving_state == 'finish'):
            record = True

        if (prev_record[vehicle_id]['drivingState'] == 'wait') & (driving_state == 'finish'):
            record = True

        if (prev_record[vehicle_id]['drivingState'] == 'unkown') & (driving_state == 'finish'):
            record = True

        if (prev_record[vehicle_id]['drivingState'] == 'moving') & (driving_state == 'start'):
            record = True

        if (prev_record[vehicle_id]['drivingState'] == 'wait') & (driving_state == 'start'):
            record = True

        if (prev_record[vehicle_id]['drivingState'] == 'unkown') & (driving_state == 'start'):
            record = True

        if (prev_record[vehicle_id]['doorOpen'] != doorOpen):
            record = True

        if record:
            out = {'timestamp': timestamp,
                   'vehicleNumber': vehicle_id,
                   'lineNumber': line_number,
                   'linkID': linkID,
                   'stationID': stationid,
                   'drivingState': driving_state,
                   'doorOpen': doorOpen
                   }

            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(json.dumps(out)))
            producer.send_batch(event_data_batch)

    cur.close()
    conn.close()
