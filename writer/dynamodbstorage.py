"""
Database Tables

Person
    - email (P.K)
    - hasCorona

Location
    - email (F.K)
    - lattitude
    - longitude
    - time


This file defines mongodb accessor class
 - addPerson(email)
 - addLocation(email, lattitue, longitude)

Sample Config = {
    "database": {
        "person": "corono.person",
        "location": "corono.location",
    }
}

"""

import boto3
import datetime
import json
import geocoder
import time
import decimal
from boto3.dynamodb.types import TypeSerializer, DYNAMODB_CONTEXT, TypeDeserializer
# Inhibit Inexact Exceptions
DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
# Inhibit Rounded Exceptions
DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0

typeserializer = TypeSerializer()
typedeserializer = TypeDeserializer()


SLEEPTIME=2

def caller(func):

    def _wrap(*args, **kwargs):

        retries = 2
        timelimit = 0.1

        while retries:
            start = time.time()
            try:
                func(*args, **kwargs)

                while time.time() - start < timelimit:
                    time.sleep(0.01)

                retries -= 1
            except Exception as e:
                print("Error", e)
                retries -= 1

    return _wrap


def trycatchwrapper(func):

    def _wrap(*args, **kwargs):
        try:
            func(*args, **kwargs)
            return True
        except:
            return False

    return _wrap

def getdynamodb():
    return boto3.client('dynamodb')

def convertToDynamoItem(item):
    global typeserializer
    return {
        k: typeserializer.serialize(v)
        for k, v in item.items()
    }

def decodeDynamoItem(item):
    global typedeserializer
    return {
        k: typedeserializer.deserialize(v)
        for k,v in item.items()
    }

jprint = lambda x: print(json.dumps(convertToDynamoItem(x), indent=4))

class DynamoDBStorage(object):

    def __init__(self, delete_tables=False):
        self._db = getdynamodb()
        self._tables = self._db.list_tables()['TableNames']
        if delete_tables and self._tables:
            self._delete_tables()
        if not self._tables:
            self._create_tables()
        print(self._tables)


    @trycatchwrapper
    def add_user(self, email, name, time=None, hasCorona=False):
        db_obj = {
            "email" : email,
            "name" : name,
            "hasCorona": hasCorona,
            "lastUpdated": str(datetime.datetime.now()) if not time else time
        }
        print("DynamoItem")
        jprint(db_obj)
        self._db.put_item(TableName='person', Item=convertToDynamoItem(db_obj))

    @trycatchwrapper
    def add_location(self, email, latitude, longitude):
        db_obj = {
            "email": email,
            "latitude": decimal.Decimal(latitude),
            "longitude": decimal.Decimal(longitude),
            "time": str(datetime.datetime.now())
        }
        print("DynamoItem")
        jprint(db_obj)
        self._db.put_item(TableName='location', Item=convertToDynamoItem(db_obj))

    def isAtRisk(self, email):
        """
        Find from now to time if a person was in contact with
        any person who has corona now
        """
        query_params = {
            "email": email,
        }
        row =  self._db.get_item(TableName='person', Key=convertToDynamoItem(query_params))
        result = decodeDynamoItem(row["Item"])
        if result:
            return result["hasCorona"]

    @trycatchwrapper
    def updateUser(self, email, hasCorona=False):
        """
        Update the corona status user with with email
        """
        query_params = {
            ":email": email,
            ":haCorona": hasCorona,
            ":lastUpdated": datetime.datetime.now()
        }

        self._db.update_item(
            TableName='person',
            Key = convertToDynamoItem(
                {
                    "email": email
                }
            ),
            UpdateExpression="SET hasCoronoa = :hasCorona, lastUpdated = :lastUpdated",
            ExpressionAttributeValues=convertToDynamoItem(query_params)
        )

    def getAllPeople(self):
        return self._db.scan(TableName='person')['Items']

    def getAllLocations(self):
        return self._db.scan(TableName='location')['Items']

    @caller
    def _create_tables(self):
        self._db.create_table(
            AttributeDefinitions=[
                {
	        	    'AttributeName': 'email',
	        	    'AttributeType': 'S'
                }
            ],
            TableName='person',
            KeySchema=[
                {
	        	    'AttributeName': 'email',
	        	    'KeyType': 'HASH'
                }
            ],
            ProvisionedThroughput={
	        	'ReadCapacityUnits': 10,
	        	'WriteCapacityUnits': 5
            }
        )
        time.sleep(SLEEPTIME)
        self._db.create_table(
            AttributeDefinitions=[
                {
	        	    'AttributeName': 'email',
	        	    'AttributeType': 'S'
                },
            ],
            TableName='location',
            KeySchema=[
                {
	        	    'AttributeName': 'email',
	        	    'KeyType': 'HASH'
                },
            ],
            ProvisionedThroughput={
	        	'ReadCapacityUnits': 10,
	        	'WriteCapacityUnits': 10
            }
        )
        time.sleep(SLEEPTIME)

        self._tables = self._db.list_tables()['TableNames']
        print(self._tables)

    def _delete_tables(self):
        for table in self._tables:
            self._db.delete_table(TableName=table)
            time.sleep(SLEEPTIME)
        self._tables = []



def main():
    email = "utkarshgautam247@gmail.com"
    db = DynamoDBStorage()
    # db.add_user("utkarshgautam247@gmail.com", "Utkarsh Gautam")
    # latlng = geocoder.ip('me').latlng
    # latlng = [10.50, 11.60]
    # db.add_location(email, latlng[0], latlng[1])
    print("Folks ! ", db.getAllLocations())
    print(f"{email} is at risk {db.isAtRisk(email)}")



if __name__ == "__main__":
    main()
