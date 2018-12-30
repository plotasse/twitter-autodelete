#!/usr/bin/python
import sqlite3
import json
import tweepy
import sys
import bz2
import base64
import signal
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from dateutil.parser import parse as date_parse
from zipfile import ZipFile
from io import TextIOWrapper
from time import sleep

# number of tweets to be deleted simultaneously
n_workers = 5

path_data = Path('data')
path_db = path_data / "db.sqlite"
path_config = path_data / "config.json"

thounk = '''
QlpoOTFBWSZTWXZztUsAAAZQYXQwQABEQCEAEAAwAM02Ep6JFDQEp6p+qgAEKkaNDToHO87O97gH
p+LiBGIHy9jJIhVxiqj5EF2iC9OSk73q4CjmsGorG1ZpY1LRBQ8krm0nnbvM4NKrcJohts2s2a23
LEUFxTCmwCQJwFwJwhkNKySSuhJVQcfi7kinChIOznapYA==
'''

# create directories
path_data.mkdir(parents=True, exist_ok=True)
# connect to database
conn = sqlite3.connect(path_db)
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS tweet
			(id INTEGER PRIMARY KEY, time TEXT, removed INTEGER DEFAULT 0)''')
conn.commit()

def connect_twitter():
	global path_config, twitter
	with open(path_config) as f:
		config = json.load(f)
	auth = tweepy.OAuthHandler(config["ck"], config["cs"])
	auth.set_access_token(config["at"], config["ats"])
	twitter = tweepy.API(auth)

def add_tweet(tweet_id, tweet_time):
	global cur
	cur.execute("INSERT INTO tweet VALUES (?, datetime(?), 0)", (tweet_id, tweet_time))
	#print("add %d [%s]" % (tweet_id, tweet_time))

def load_archive(path_archive):
	global conn
	with ZipFile(path_archive) as archive:
		with TextIOWrapper(archive.open("data/js/tweet_index.js")) as f:
			while f.read(1) != "=":
				pass
			index = [i["file_name"] for i in json.load(f)]
		for i in index:
			sys.stdout.write("loading %s... " % i)
			sys.stdout.flush()
			count = 0
			with TextIOWrapper(archive.open(i)) as f:
				while f.read(1) != "=":
					pass
				for i in json.load(f):
					try:
						add_tweet(i["id"], date_parse(i["created_at"]))
						count+=1
					except sqlite3.IntegrityError:
						pass
			conn.commit()
			print("new tweets: %d" % count)

def delete_tweet(tweet):
	global twitter, stopped
	i, t = tweet
	if stopped:
		return None
	#sleep(1)
	#print("dummy delete %d [%s]" % (i,t))
	#return (i,)
	try:
		twitter.destroy_status(i)
		print("delete %d [%s]: ok" % (i,t))
		return (i,)
	except tweepy.error.TweepError as e:
		if e.api_code == 144:
			print("delete %d [%s]: already deleted" % (i,t))
			return (i,)
		else:
			print("error:", e)
			return None
def delete_tweets():
	global conn, cur, stopped, n_workers
	cur.execute('SELECT id, time FROM tweet WHERE time < datetime("now", "-25 day") AND removed = 0')
	tweets = cur.fetchall()
	print("Tweets to delete: %d" % len(tweets))
	try:
		connect_twitter()
	except:
		print("Error connecting to twitter, please check your config or run setup.")
		return
	stopped = False
	deleted = []
	try:
		pool = ThreadPoolExecutor(n_workers)
		r = pool.map(delete_tweet, tweets)
		pool.shutdown()
	except KeyboardInterrupt:
		# first ^C: stop deleting tweets
		print("Aborting...")
		stopped = True
	# ignore any new interruption, we need to update the database
	signal.signal(signal.SIGINT, signal.SIG_IGN)
	for i in r:
		if i is not None:
			deleted.append(i)
	print("Deleted tweets: %d" % len(deleted))
	print("Updating database...")
	cur.executemany('UPDATE tweet SET removed = 1 WHERE id = ?', deleted)
	conn.commit()
	signal.signal(signal.SIGINT, signal.SIG_DFL)

def setup():
	global path_config
	print("=== SETUP ===")
	print()
	ck = input("Twitter consumer key: ")
	cs = input("Twitter consumer secret: ")
	print(bz2.decompress(base64.b64decode(thounk)).decode())
	auth = tweepy.OAuthHandler(ck, cs)
	try:
		print("Please visit %s" % auth.get_authorization_url())
	except:
		print("Invalid key or secret.")
		exit(1)
	verif = input("Verification code: ")
	try:
		at, ats = auth.get_access_token(verif)
	except:
		print("Invalid verification code.")
		exit(1)
	auth.set_access_token(at, ats)
	twitter = tweepy.API(auth)
	me = twitter.me()
	print()
	print("Identified as: %s @%s" % (me.screen_name, me.name))
	config = {"ck":ck,"cs":cs,"at":at,"ats":ats}
	with open(path_config,"w") as f:
		json.dump(config, f)
	print()
	print("Configuration saved !")


try:
	if len(sys.argv) == 3 and sys.argv[1] == "load-archive":
		load_archive(sys.argv[2])
	elif len(sys.argv) == 2 and sys.argv[1] == "delete-tweets":
		delete_tweets()
	elif len(sys.argv) == 2 and sys.argv[1] == "setup":
		setup()
	else:
		print("Usage: %s" % sys.argv[0])
		print("\t\tload-archive <filename>")
		print("\t\tdelete-tweets")
		print("\t\tsetup")
except KeyboardInterrupt:
	pass
