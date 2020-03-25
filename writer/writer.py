"""
Reauest handler layer to provide a restful interface 
to db layer

"""

from dynamodbstorage import DynamoDBStorage
from flask import Flask, jsonify, request, abort

ERROR_CODES = {
    "BAD_REQUEST": 500
}


app = Flask(__name__)

db = DynamoDBStorage()

@app.route('/gocoronago/api/v1.0/getAllPeople', methods=['GET'])
def get_all_people():
    return jsonify({"people": db.getAllPeople()})


@app.route('/gocoronago/api/v1.0/getAllLocations', methods=['GET'])
def get_all_locations():
    return jsonify({"locations": db.getAllLocations()})

@app.route('/gocoronago/api/v1.0/isAtRisk', methods=['GET'])
def get_is_at_risk():
    if not request.json or not 'user' in request.json:
        abort(ERROR_CODES["BAD_REQUEST"])

    user = request.json['user']
    atRisk = db.isAtRisk(user['email'])

    return jsonify({"isAtRisk": atRisk})

@app.route('/gocoronago/api/v1.0/updateUser', methods=['PUT'])
def update_user():
    if not request.json or not 'user' in request.json:
        abort(ERROR_CODES["BAD_REQUEST"])
    user = request.json['user']
    successful = db.updateUser(
        user['email'],
        user.get('hasCorona', False),
    )
    return jsonify({"success": successful})

@app.route('/gocoronago/api/v1.0/putUser', methods=['POST'])
def put_user():
    if not request.json or not 'user' in request.json:
        abort(ERROR_CODES["BAD_REQUEST"])
    user = request.json['user']
    successful = db.add_user(
        user['email'],
        user.get('name'),
        user.get('time', None),
        user.get('hasCorona', False),
    )
    return jsonify({"success": successful}), 201

@app.route('/gocoronago/api/v1.0/putLocation', methods=['POST'])
def put_location():
    if not request.json or not 'location' in request.json:
        abort(ERROR_CODES["BAD_REQUEST"])
    
    successful = db.add_location(
        request.json['email'],
        request.json['latitude'],
        request.json['longitude'],
    )

    return jsonify({"success": successful}), 201


if __name__ == "__main__":
    app.run(debug=True)
