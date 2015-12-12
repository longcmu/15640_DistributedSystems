from flask import session
from flask.ext.socketio import emit, join_room, leave_room
from .. import socketio
import urllib
import urllib2
import requests
import math


# pre-registered server host IP and port number
proposeAddr = 'http://localhost:3030/propose'
getroundAddr = 'http://localhost:3030/getround'
getvalueAddr = 'http://localhost:3030/getvalue?key='

# pre-registered usernames
CONST_NAME0 = 'Alice'
CONST_NAME1 = 'Bob'
CONST_NAME2 = 'Carl'

# pre-registered userIDs
CONST_ID0 = 0
CONST_ID1 = 1
CONST_ID2 = 2


# listen for joining users and print the incoming user on the page
@socketio.on('joined', namespace='/game')
def joined(message):
    """Sent by clients when they enter a room.
    A status message is broadcast to all people in the room."""
    room = session.get('room')
    join_room(room)
    emit('status', {'msg': session.get('name') + ' has entered the room.'}, room=room)


# listen for input messages and pass params to handelText
@socketio.on('text', namespace='/game')
def text(message):
    """Sent by a client when the user entered a new message.
    The message is sent to all people in the room."""
    room = session.get('room')
    name = session.get('name')
    msg = message['msg']

    if name == CONST_NAME0:
        handleText(msg, CONST_ID0, room)
    elif name == CONST_NAME1:
        handleText(msg, CONST_ID1, room)
    elif name == CONST_NAME2:
        handleText(msg, CONST_ID2, room)
    else:
        emit('message', {'msg': session.get('name') + ': User doesn\'t exist!'}, room=room)


# handle the incoming messages and print the responses from backend on the page
def handleText(msg, id, room):
    msgs = msg.split( )
    # check if input is valid
    if len(msgs) != 1:
        emit('message', {'msg': session.get('name') + ': Invalid Input!'}, room=room) 
    else:
        roundNum = requests.get(getroundAddr)
        roundNum = str(roundNum.content).rstrip()
        # sync current round number
        if int(roundNum) > 1: 
            value = requests.get(getvalueAddr + str(int(roundNum) - 1))
            value = str(value.content).rstrip()
            value = value.split( )[1]
            if int(msg) - int(value) > 2 or int(msg) - int(value) <= 0 or int(msg) > 30:
                emit('message', {'msg': session.get('name') + ': Invalid Value!'}, room=room)
                return
        # check is current round number is valid
        if int(roundNum) < 1 or int(roundNum) > 30:
            emit('message', {'msg': session.get('name') + ': Invalid Round Number!'}, room=room)
        else:
            ret = requests.post(proposeAddr, data = {"key":roundNum, "value":msg, "id":str(id)})
            ret = str(ret.content).split( )
            # check if this player commit
            if ret[0] == str(id):
                emit('message', {'msg': "===== ROUND " + str(roundNum) + " ====="}, room=room)
                emit('message', {'msg': session.get('name') + ': Got ' + ret[1] + "!"}, room=room)
                if ret[1] == '30':
                    emit('message', {'msg': session.get('name') + ': Win!'}, room=room)
            else:
                emit('message', {'msg': session.get('name') + ': Rejected ' + ret[1] + "!"}, room=room)


# listen for left messages and print the leaving user on the page
@socketio.on('left', namespace='/game')
def left(message):
    """Sent by clients when they leave a room.
    A status message is broadcast to all people in the room."""
    room = session.get('room')
    leave_room(room)
    emit('status', {'msg': session.get('name') + ' has left the room.'}, room=room)
