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

### CONFIG ###

# number of tweets to be deleted simultaneously
n_workers = 5

# when do we remove tweets
datetime_modifier = "-25 day"

# path to data (contains connection information and tweet database)
path_data = Path('data')

### END CONFIG ###

path_db = path_data / "db.sqlite"
path_keys = path_data / "keys.json"

thounk = '''
QlpoOTFBWSZTWetNC1UAABnQYX2wQABEQAEAEAAwATqlQSnokkNAKYAAEmoSanpMjgQOUWuHe7O3
e9fbgQKZAPAwVC8KFkXEY1llytzbbCNLZjWWjIrLG0sV9gAE/QACVrWihBBErkJEsMquZXpC9w45
xdcn2ExxN30sCFkTSRFBI4HdtE8cvtDl4epk666gYh+YbSKHSGJItssfULQybukTNcO3Ii1jxPzk
E1wR6ePSGMFgWrLqsZxDzlS00O0lMkUkKZBZv+LuSKcKEh1poWqg
'''

# constants for 'removed' column inside database
S_TO_DELETE = 0
S_DELETED = 1
S_TO_KEEP = 2
S_CANNOT_DELETE = 3

# create directories
path_data.mkdir(parents=True, exist_ok=True)
# connect to database
conn = sqlite3.connect(path_db)
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS tweet
			(id INTEGER PRIMARY KEY, time TEXT, removed INTEGER DEFAULT 0)''')
conn.commit()

def connect_twitter():
	global path_keys, twitter
	with open(path_keys) as f:
		keys = json.load(f)
	auth = tweepy.OAuthHandler(keys["ck"], keys["cs"])
	auth.set_access_token(keys["at"], keys["ats"])
	twitter = tweepy.API(auth)

def add_tweet(tweet_id, tweet_time):
	global cur
	cur.execute("INSERT INTO tweet (id, time) VALUES (?, datetime(?))", (tweet_id, tweet_time))
	#print("add %d [%s]" % (tweet_id, tweet_time))

def load_archive(path_archive):
	global conn
	with ZipFile(path_archive) as archive:
		with TextIOWrapper(archive.open("data/js/tweet_index.js")) as f:
			while f.read(1) not in ("=",""):
				pass
			index = [i["file_name"] for i in json.load(f)]
		for i in index:
			sys.stdout.write("loading %s... " % i)
			sys.stdout.flush()
			count = 0
			with TextIOWrapper(archive.open(i)) as f:
				while f.read(1) not in ("=",""):
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
	#return {"id": i, "removed": S_DELETED}
	try:
		twitter.destroy_status(i)
		print("delete %d [%s]: ok" % (i,t))
		return {"id": i, "removed": S_DELETED}
	except tweepy.error.TweepError as e:
		if e.api_code in {34, 144}:
			print("delete %d [%s]: already deleted" % (i,t))
			return {"id": i, "removed": S_DELETED}
		elif e.api_code == 63:
			print("delete %d [%s]: account suspended, marking as CANNOT_DELETE" % (i,t))
			return {"id": i, "removed": S_CANNOT_DELETE}
		else:
			print("delete %d [%s]:" % (i,t), e)
			return None
def delete_tweets():
	global conn, cur, stopped, n_workers
	cur.execute('SELECT id, time FROM tweet WHERE time < datetime("now", ?) AND removed = ?', (datetime_modifier, S_TO_DELETE))
	tweets = cur.fetchall()
	print("Tweets to delete: %d" % len(tweets))
	try:
		connect_twitter()
	except:
		raise RuntimeError("Could not connect to twitter, please check keys.json or run setup.")
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
	count = 0
	for i in r:
		if i is not None:
			deleted.append(i)
			if i["removed"] == S_DELETED:
				count += 1
	print("Deleted tweets: %d" % count)
	print("Updating database...")
	cur.executemany('UPDATE tweet SET removed = :removed WHERE id = :id', deleted)
	conn.commit()
	signal.signal(signal.SIGINT, signal.SIG_DFL)

def setup():
	global path_keys
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
	keys = {"ck":ck,"cs":cs,"at":at,"ats":ats}
	with open(path_keys,"w") as f:
		json.dump(keys, f)
	print()
	print("Configuration saved !")

def update_tweets():
	global conn, cur, twitter
	try:
		connect_twitter()
	except:
		raise RuntimeError("Could not connect to twitter, please check keys.json or run setup.")
	cur.execute("SELECT max(id) FROM tweet")
	page = 1
	since_id, = cur.fetchone()
	if since_id is None:
		raise RuntimeError("No tweet in database, please run load-archive.")
	print("Last tweet ID: %d" % since_id)
	print("Reading timelines...")
	tl = twitter.user_timeline(since_id = since_id, page = page)
	while len(tl) != 0:
		print("Page %d, tweets to add: %d" % (page, len(tl)))
		for t in tl:
			add_tweet(t.id, t.created_at)
		page += 1
		tl = twitter.user_timeline(since_id = since_id, page = page)
	print("Updating database...")
	conn.commit()

def status():
	print()
	print("-----------------------------------")
	cur.execute("SELECT count(*) FROM tweet")
	print("Known tweets:                %d" % cur.fetchone())
	cur.execute("SELECT count(*) FROM tweet WHERE removed = ?", (S_TO_DELETE,))
	print("Tweets that can be deleted:  %d" % cur.fetchone())
	cur.execute("SELECT count(*) FROM tweet WHERE removed = ?", (S_DELETED,))
	print("Tweets already deleted:      %d" % cur.fetchone())
	cur.execute("SELECT count(*) FROM tweet WHERE removed = ?", (S_TO_KEEP,))
	print("Tweets we should keep:       %d" % cur.fetchone())
	cur.execute("SELECT count(*) FROM tweet WHERE removed = ?", (S_CANNOT_DELETE,))
	print("Tweets we failed to delete:  %d" % cur.fetchone())
	cur.execute('SELECT count(*) FROM tweet WHERE time < datetime("now", ?) AND removed = ?', (datetime_modifier,S_TO_DELETE))
	print("Tweets to be deleted now:    %d" % cur.fetchone())
	print("-----------------------------------")
	print()

if __name__ == "__main__":
	try:
		if len(sys.argv) == 3 and sys.argv[1] == "load-archive":
			load_archive(sys.argv[2])
		elif len(sys.argv) == 2 and sys.argv[1] == "delete-tweets":
			delete_tweets()
		elif len(sys.argv) == 2 and sys.argv[1] == "update-tweets":
			update_tweets()
		elif len(sys.argv) == 2 and sys.argv[1] == "status":
			status()
		elif len(sys.argv) == 2 and sys.argv[1] == "setup":
			setup()
		elif len(sys.argv) == 1:
			status()
			update_tweets()
			status()
			delete_tweets()
			status()
		else:
			print("Usage: %s" % sys.argv[0])
			print("\t\tsetup")
			print("\t\tload-archive <filename>")
			print("\t\tdelete-tweets")
			print("\t\tupdate-tweets")
			print("\t\tstatus")
			print("If no command is specified, update and delete tweets")
	except KeyboardInterrupt:
		pass
	except RuntimeError as e:
		print(e)
