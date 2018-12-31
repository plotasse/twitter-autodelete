# twitter-autodelete

## what is this

a script to delete tweets after some time

you will need need to install python 3, find twitter api keys and download a tweet archive

## how to install

this requires python 3

run `pip install tweepy python-dateutil` to install the dependencies

## how to set up

### configuration

you should check the `### CONFIG ###` part inside twitter_autodelete.py and make sure the values suit you

### api keys, authorization, archive

first you need to download an archive of your tweets on the twitter website

then, run `./twitter_autodelete.py setup` to connect to your twitter account (you need API keys)

then, run `./twitter_autodelete.py load-archive <filename>` where `<filename>` is the path to your twitter archive (\*.zip)

you should be ready to go, you may use `./twitter_autodelete.py status` to check the loaded tweet count

## how to use

`./twitter_autodelete.py setup` asks for your API keys then to authorize us to use your account

`./twitter_autodelete.py load-archive <filename>` loads a twitter archive into the database

`./twitter_autodelete.py delete-tweets` deletes the tweets that are in the database and have passed the deadline

`./twitter_autodelete.py update-tweets` reads your user timeline to find the newest tweets, and add them to database

`./twitter_autodelete.py status` shows useless stats

`./twitter_autodelete.py` does the same as running delete-tweets then update-tweets (and prints stats before and after each step)

use crontab or whatever scheduler you like to run this periodically
