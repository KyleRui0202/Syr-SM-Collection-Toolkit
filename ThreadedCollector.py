#-------------------------------------------------------------------------------
# Name:        Tweet collector.
# Purpose:     Collects tweets based on search terms specified in a terms file
#              Sends tweets as JSON to a file.
#
# Author:      jhemsley
#
# Created:     2013/07/10
# Copyright:   (c) Social Media Lab 2013
# Licence:     <your licence>
#
# Code for this is based on both of these
#   https://github.com/tweepy/tweepy/blob/master/examples/streaming.py
#   https://github.com/arngarden/TwitterStream
#   http://log.widnyana.web.id/2012/05/simple-python-twitter-client-using-tweepy.txt
#
# HOW IT WORKS:
# This process reads a document in the mongo config collection. As of this writting
# the document looks like this:
#   {'module': 'collector', 'run': 1, 'collect': 0, 'update': 0}
# When run=1 this system enters a loop and checks mongo for changes to the signal
# flags: run, collect & update. In the conditions noted below, a worker thread is
# started that collects data from Twitter's streaming API while the main thread
# continues to watch mongo for signals.
#   When RUN is set to 0, stop thread and exit process, otherwise continue
#   When collect is 1, start worker thread. if collect is 0, stop worker thread.
#   When UPDATE is 1, stop thread, change update flag in mongo to 0. note that
#      if the collect flag is 1, then the process will start a worker
#
# Also of note, this system reads platform.ini for config items. One of these sets
# how frequently output JSON files are rolled over. The file name is built using
# three items in the platform.ini:
#  raw_tweets_file_path is the directory location where the output files will go
#  tweets_file_date_frmt is a date format string and is used to build the tweets
#         out file name as well as indicate when the file should rool over. The
#         fastest possible roll over (not reccomended) is seconds and is specified
#         with %Y%m%d-%H%M%S. For testing use minutes (%Y%m%d-%H%M) r hours
#         (%Y%m%d-%H), but for collection use hours or days (%Y%m%d)
#  tweets_file is a suffix for the file name.
#  the final constructed file name will be something like:
#         ./tweets/20130822-1030tweets_out.json
#
#-------------------------------------------------------------------------------

from tweepy.streaming import StreamListener, Stream
from tweepy.error import TweepError
from tweepy import OAuthHandler
from tweepy.api import API
from pymongo import MongoClient

import httplib
from socket import timeout
import ssl
import threading
import os.path
import json
import ConfigParser
import datetime
import logging
import logging.config
import time
import traceback
import sys

# Config file includes paths, parameters, and oauth information for this module
# Complete the directions in "example_platform.ini" for configuration before proceeding
PLATFORM_CONFIG_FILE = 'platform.ini'

# Connect to Mongo
connection = MongoClient()
db = connection.config
mongo_config = db.config

# Program thread
e = threading.Event()

class fileOutListener(StreamListener):
    """ This listener handles tweets as they come in by converting them
    to JSON and sending them to a file. Each line in the file is a tweet.
    """
    def __init__(self, tweetsOutFilePath, tweetsOutFileDateFrmt, tweetsOutFile, logger, collection_type):
        self.logger = logger
        self.logger.info('COLLECTION LISTENER: Initializing Stream Listener...')
        self.buffer = ''
        self.tweet_count = 0
        self.rate_limit_count = 0
        self.error_code = 0

        self.tweetsOutFilePath = tweetsOutFilePath
        self.tweetsOutFileDateFrmt = tweetsOutFileDateFrmt
        self.tweetsOutFile = tweetsOutFile
        self.collection_type = collection_type
        self.config_name = 'collector-' + collection_type

        timestr = time.strftime(self.tweetsOutFileDateFrmt)
        self.tweetsOutFileName = self.tweetsOutFilePath + timestr + '-' + self.collection_type + '-' + self.tweetsOutFile
        self.logger.info('COLLECTION LISTENER: initial data collection file: %s' % self.tweetsOutFileName)


    def on_data(self, data):
        self.buffer += data

        if data.endswith('\r\n') and self.buffer.strip():
            # complete message received so convert to JSON and proceed
            message = json.loads(self.buffer)
            self.buffer = ''
            msg = ''
            # Rate limiting logging
            if message.get('limit'):
                self.logger.warning('COLLECTION LISTENER: Rate limiting caused us to miss %s tweets' % (message['limit'].get('track')))
                print 'Rate limiting caused us to miss %s tweets' % (message['limit'].get('track'))

                # Logs info to mongo
                now = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
                rate_limit_info = { now: int(message['limit'].get('track')) }
                mongo_config.update({
                    "module":self.config_name},
                    {"$push": {"rate_limit.counts": rate_limit_info}})

                # Total tally
                self.rate_limit_count += int(message['limit'].get('track'))
            # Disconnect message handling
            elif message.get('disconnect'):
                self.logger.info('COLLECTION LISTENER: Got disconnect: %s' % message['disconnect'].get('reason'))
                raise Exception('Got disconnect: %s' % message['disconnect'].get('reason'))
            # Warning handling
            elif message.get('warning'):
                self.logger.info('COLLECTION LISTENER: Got warning: %s' % message['warning'].get('message'))
                print 'Got warning: %s' % message['warning'].get('message')
            # Else good to go, read data
            else:
                self.tweet_count += 1

                # this is a timestamp using the format in the config
                timestr = time.strftime(self.tweetsOutFileDateFrmt)
                # this creates the filename. If the file exists, it just adds to it, otherwise it creates it
                JSONfileName = self.tweetsOutFilePath + timestr + '-' + self.collection_type + '-' + self.tweetsOutFile
                if not os.path.isfile(JSONfileName):
                    self.logger.info('Creating new file: %s' % JSONfileName)
                myFile = open(JSONfileName,'a')
                myFile.write(json.dumps(message).encode('utf-8'))
                myFile.write('\n')
                myFile.close()
                return True

    # Twitter's http error codes are listed here:
    # https://dev.twitter.com/streaming/overview/connecting
    # Starts retry loop for:
    #   A) 420 - rate limited
    #   B) 503 - service unavailable
    # Otherwise, stops stream & logs error info
    def on_error(self, status):
        self.error_code = status

        # Retries if rate limited (420) or unavailable (520)
        if status in [420, 503]:
            if status == 420:
                self.logger.error('COLLECTION LISTENER: Twitter rate limited our connection with error code: %d. Retrying.' % status)
                print 'COLLECTION LISTENER: Twitter rate limited our connection with error code: %d. Retrying.' % status
            else:
                self.logger.error('COLLECTION LISTENER: Twitter service is currently unavailable with error code: %d. Retrying.' % status)
                print 'COLLECTION LISTENER: Twitter service is currently unavailable with error code: %d. Retrying.' % status
            return True # Initiates retry backoff loop
        else:
            self.logger.error('COLLECTION LISTENER: Twitter refused or aborted our connetion with the following error code: %d' % status)
            print 'COLLECTION LISTENER: Twitter refused or aborted our connetion with the following error code: %d' % status
            return False # Breaks stream

class ToolkitStream(Stream):

    host = 'stream.twitter.com'

    def __init__(self, auth, listener, logger, **options):
        self.auth = auth
        self.listener = listener
        self.running = False
        self.timeout = options.get("timeout", 300.0)
        self.retry_count = options.get("retry_count")
        # values according to https://dev.twitter.com/docs/streaming-apis/connecting#Reconnecting
        self.retry_time_start = options.get("retry_time", 5.0)
        self.retry_420_start = options.get("retry_420", 60.0)
        self.retry_time_cap = options.get("retry_time_cap", 320.0)
        self.snooze_time_step = options.get("snooze_time", 0.25)
        self.snooze_time_cap = options.get("snooze_time_cap", 16)
        self.buffer_size = options.get("buffer_size",  1500)
        if options.get("secure", True):
            self.scheme = "https"
        else:
            self.scheme = "http"

        self.api = API()
        self.headers = options.get("headers") or {}
        self.parameters = None
        self.body = None
        self.retry_time = self.retry_time_start
        self.snooze_time = self.snooze_time_step

        self.logger = logger
        self.logger.info('TOOLKIT STREAM: Stream initialized.')
        print 'TOOLKIT STREAM: Stream initialized.'

    def _run(self):
        # Authenticate
        url = "%s://%s%s" % (self.scheme, self.host, self.url)

        # Connect and process the stream
        error_counter = 0
        conn = None
        exception = None
        while self.running:
            if self.retry_count is not None and error_counter > self.retry_count:
                self.logger.info('TOOLKIT STREAM: Stream stopped after %d retries.' % self.retry_count)
                break
            try:
                if self.scheme == "http":
                    conn = httplib.HTTPConnection(self.host, timeout=self.timeout)
                else:
                    conn = httplib.HTTPSConnection(self.host, timeout=self.timeout)
                self.auth.apply_auth(url, 'POST', self.headers, self.parameters)
                conn.connect()
                conn.request('POST', self.url, self.body, headers=self.headers)
                resp = conn.getresponse()
                if resp.status != 200:
                    if self.listener.on_error(resp.status) is False:
                        break
                    error_counter += 1
                    if resp.status == 420:
                        self.retry_time = max(self.retry_420_start, self.retry_time)
                    sleep(self.retry_time)
                    self.retry_time = min(self.retry_time * 2, self.retry_time_cap)
                else:
                    error_counter = 0
                    self.retry_time = self.retry_time_start
                    self.snooze_time = self.snooze_time_step
                    self.listener.on_connect()
                    self._read_loop(resp)
            except (timeout, ssl.SSLError) as exc:
                # If it's not time out treat it like any other exception
                if isinstance(exc, ssl.SSLError) and not (exc.args and 'timed out' in str(exc.args[0])):
                    exception = exc
                    break

                if self.listener.on_timeout() == False:
                    break
                if self.running is False:
                    break
                conn.close()
                sleep(self.snooze_time)
                self.snooze_time = min(self.snooze_time + self.snooze_time_step,
                                       self.snooze_time_cap)
            except Exception as exception:
                # any other exception is fatal, so kill loop
                break

        # cleanup
        self.running = False
        if conn:
            conn.close()

        if exception:
            # call a handler first so that the exception can be logged.
            self.listener.on_exception(exception)
            raise

    def disconnect(self):
        print 'TOOLKIT STREAM: Got disconnect signal.'
        self.logger.info('TOOLKIT STREAM: Got disconnect signal.')
        if self.running is False:
            return
        self.running = False

if __name__ == "__main__":
    try:
        # collection_type = sys.argv[1]
        collection_type = 'track'
    except IndexError:
        print "To run: python ThreadedCollector.py {track | follow}"
        sys.exit()

    if collection_type not in ['track', 'follow']:
        print "To run: python ThreadedCollector.py {track | follow}"
        sys.exit()
    else:
        config_name = 'collector-' + collection_type
        oauth_config = 'oauth-' + collection_type

    Config = ConfigParser.ConfigParser()
    Config.read(PLATFORM_CONFIG_FILE)

    # Grabs logging info (directory, filename) from config file
    # logDir = Config.get('files', 'log_dir', 0)
    logConfigFile = Config.get('files', 'log_config_file', 0)
    logging.config.fileConfig(logConfigFile)
    # logging.addLevelName('root', config_name)
    logger = logging.getLogger(config_name)

    # Sets current date as starting point
    tmpDate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info('Starting collection system at %s' % tmpDate)

    # Grabs collection name from config
    collectionName = Config.get('collection', 'name', 0)
    logger.info('Collection name: %s' % collectionName)

    # Grabs terms list file from config
    termsListFile = Config.get('files', 'terms_file', 0)

    # Grabs tweets out file info from config
    tweetsOutFilePath = Config.get('files', 'raw_tweets_file_path', 0)
    if not os.path.exists(tweetsOutFilePath):
        os.makedirs(tweetsOutFilePath)
    tweetsOutFileDateFrmt = Config.get('files', 'tweets_file_date_frmt', 0)
    tweetsOutFile = Config.get('files', 'tweets_file', 0)

    consumerKey = Config.get(oauth_config, 'consumer_key', 0)
    consumerSecret = Config.get(oauth_config, 'consumer_secret', 0)
    accessToken = Config.get(oauth_config, 'access_token', 0)
    accessTokenSecret = Config.get(oauth_config, 'access_token_secret', 0)

    # Authenticates via app info
    auth = OAuthHandler(consumerKey, consumerSecret)
    auth.set_access_token(accessToken, accessTokenSecret)

    # Sets Mongo collection; sets rate_limitng & error counts to 0
    mongoConfigs = mongo_config.find_one({"module" : config_name})
    mongo_config.update({
        "module" : config_name},
        {'$set' : {'rate_limit': { 'counts': [], 'total': 0 }}})
    mongo_config.update({"module" : config_name}, {'$set' : {'error_code': 0}})

    # Should be 1 by default
    runCollector = mongoConfigs['run']

    if runCollector:
        print 'Starting process'
        logger.info('Collection start signal %d' % runCollector)
    collectingData = False

    i = 0
    myThreadCounter = 0
    runLoopSleep = 0

    while runCollector:
        i += 1

        # Finds Mongo collection & grabs signal info
        # If Mongo is offline throws an exception and continues
        exception = None
        try:
            mongoConfigs = mongo_config.find_one({"module" : config_name})
            runCollector = mongoConfigs['run']
            collectSignal = mongoConfigs['collect']
            updateSignal = mongoConfigs['update']
        except Exception as exception:
            print 'Mongo connection refused with exception: %s' % exception
            logger.error('Mongo connection refused with exception: %s' % exception)

        """
        Collection process is running, and:
        A) An update has been triggered -OR-
        B) The collection signal is not set -OR-
        C) Run signal is not set
        """
        if collectingData and (updateSignal or not collectSignal or not runCollector):
            # Update has been triggered
            if updateSignal:
                logger.info('MAIN: received UPDATE signal. Attempting to stop collection thread')
                mongo_config.update({"module" : config_name}, {'$set' : {'update': 0}})
            # Collection thread triggered to stop
            if not collectSignal:
                logger.info('MAIN: received STOP signal. Attempting to stop collection thread')
            # Entire process trigerred to stop
            if not runCollector:
                logger.info('MAIN: received EXIT signal. Attempting to stop collection thread')
                mongo_config.update({"module" : config_name}, {'$set' : {'collect': 0}})
                mongo_config.update({"module" : config_name}, {'$set' : {'update': 0}})
                collectSignal = 0

            # Send stream disconnect signal, kills thread
            stream.disconnect()
            collectingData = False

            logger.info('COLLECTION THREAD: stream stopped after %d tweets' % l.tweet_count)
            logger.info('COLLECTION THREAD: lost %d tweets to rate limit' % l.rate_limit_count)
            print 'COLLECTION THREAD: stream stopped after %d tweets' % l.tweet_count

            if not l.error_code == 0:
                mongo_config.update({"module" : config_name}, {'$set' : {'collect': 0}})
                mongo_config.update({"module" : config_name}, {'$set' : {'error_code': l.error_code}})

            if not l.rate_limit_count == 0:
                mongo_config.update({
                    "module" : config_name},
                    {'$set' : {'rate_limit.total': l.rate_limit_count}})

        # Collection has been signaled & main program thread is running
        # TODO - Check Mongo for handle:ID pairs
        # Only call for new pairs
        if collectSignal and (threading.activeCount() == 1):
            # Names collection thread & adds to counter
            myThreadCounter += 1
            myThreadName = 'collector-' + collection_type + '%s' % myThreadCounter

            # Reads & logs terms list
            with open(termsListFile) as f:
                termsList = f.read().splitlines()

            print 'Terms list length: ' + str(len(termsList))

            # Grab IDs for follow stream
            if collection_type == 'follow':
                # First find handles/ID pairs that have already been established
                print 'MAIN: Finding stored handle:id pairs in Mongo...'
                logger.info('MAIN: Finding stored handle:id pairs in Mongo...')
                cursor = mongo_config.find({'module':'collector-follow'})
                doc = cursor[0]
                # Creates termsList array for handle:id pairs if not created
                if 'termsList' not in doc.keys():
                    mongo_config.update({'module': 'collector-follow'},
                        {'$set': {'termsList': []}})
                    cursor = mongo_config.find({'module':'collector-follow'})
                    doc = cursor[0]

                stored_terms = doc['termsList']
                stored_handles = []
                for user in stored_terms:
                    user_handle = str(user.keys()[0])
                    stored_handles.append(user_handle)
                print 'MAIN: %d handle:id pairs found in Mongo!' % len(stored_handles)
                logger.info('MAIN: %d handle:id pairs found in Mongo!' % len(stored_handles))

                # Loop thru & query (except handles that have been stored)
                print 'MAIN: Querying Twitter API for new handle:id pairs...'
                logger.info('MAIN: Querying Twitter API for new handle:id pairs...')
                twitter_api = API(auth_handler=auth)
                failed_handles = []
                success_handles = []
                for handle in termsList:
                    if handle in stored_handles:
                        pass
                    else:
                        try:
                            user = twitter_api.get_user(screen_name=handle)
                        except TweepError as tweepy_exception:
                            error_message = tweepy_exception.args[0][0]['message']
                            code = tweepy_exception.args[0][0]['code']
                            if code == 88:
                                print 'MAIN: User ID grab rate limited. Sleeping for 15 minutes.'
                                logger.exception('MAIN: User ID grab rate limited. Sleeping for 15 minutes.')
                                sys.exit()
                            elif code == 34:
                                print 'MAIN: User w/ handle %s does not exist.' % handle
                                logger.exception('MAIN: User w/ handle %s does not exist.' % handle)
                                failed_handles.append(handle)
                        else:
                            user_id = user._json['id_str']
                            terms_info = { handle: user_id }
                            mongo_config.update({'module':'collector-follow'},
                                {'$push': {'termsList': terms_info }})
                            success_handles.append(handle)

                print 'MAIN: Collected %d new ids for follow stream.' % len(success_handles)
                logger.info('MAIN: Collected %d new ids for follow stream.' % len(success_handles))
                print 'MAIN: %d handles failed to be found.' % len(failed_handles)
                logger.info('MAIN: %d handles failed to be found.' % len(failed_handles))
                logger.info(failed_handles)
                print failed_handles
                print 'MAIN: Grabbing full list of follow stream IDs from Mongo.'
                logger.info('MAIN: Grabbing full list of follow stream IDs from Mongo.')

                # Grab list from Mongo
                cursor = mongo_config.find({'module':'collector-follow'})
                doc = cursor[0]
                stored_terms = doc['termsList']
                ids = []
                for user in stored_terms:
                    ids.append(user.values()[0])
                termsList = ids

            print termsList

            logger.info('Terms list: %s' % str(termsList).strip('[]'))

            print 'COLLECTION THREAD: Initializing Tweepy listener instance...'
            logger.info('COLLECTION THREAD: Initializing Tweepy listener instance...')
            l = fileOutListener(tweetsOutFilePath, tweetsOutFileDateFrmt, tweetsOutFile, logger, collection_type)

            print 'TOOLKIT STREAM: Initializing Tweepy stream listener...'
            logger.info('TOOLKIT STREAM: Initializing Tweepy stream listener...')

            # Initiates async stream via Tweepy, which handles the threading
            stream = ToolkitStream(auth, l, logger, retry_count=100)
            if collection_type == 'track':
                stream.filter(track=termsList, async=True)
            elif collection_type == 'follow':
                stream.filter(follow=termsList, async=True)
            else:
                sys.exit('ERROR: Unrecognized stream filter.')

            collectingData = True
            print 'MAIN: Collection thread started (%s)' % myThreadName
            logger.info('MAIN: Collection thread started (%s)' % myThreadName)


        #if threading.activeCount() == 1:
        #    print "MAIN: %d iteration with no collection thread running" % i
        #else:
        #    print "MAIN: %d iteration with collection thread running (%d)" % (i, threading.activeCount())

        # Incrementally delays loop if Mongo is offline, otherwise 2 seconds
        if exception:
            print "Exception caught, sleeping for: %d" % runLoopSleep
            runLoopSleep += 2
            time.sleep(runLoopSleep)
        else:
            time.sleep( 2 )

    logger.info('Exiting Collection Program...')
    print 'Exiting Collection Program...'

    #logging.shutdown()



