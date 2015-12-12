# Front end of the game

Run the server side:

./runBack.sh

Run the client side: (include configuration)

cd p3/front

virtualenv venv

. venv/bin/activate

sudo pip install -r requirements.txt

pip install Flask

pip install Flask-SocketIO

pip install Flask-WTF

pip install requests

python game.py

Visit http://127.0.0.1:5000 in the browser.

This game is designed to support 3 users, so please open 3 tabs and enter registered Names Alice, Bob, Carl and Room number (any number would work as long as they are the same).

Players are free to enter numbers from 1 to 30 but it must be current number + 1 or current number + 2, or it will be rejected as invalid value.
