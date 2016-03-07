import pyspark
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SQLContext #, HiveContext
#from pyspark.storagelevel import StorageLevel
import atexit
from pyspark_cassandra import CassandraSparkContext
from datetime import tzinfo, timedelta, datetime
from pytz import timezone

conf = SparkConf()

#conf.setMaster("local")
conf.setAppName("My app")
conf.set("spark.cassandra.connection.host", "10.0.40.42")

sc = CassandraSparkContext(conf = conf)
atexit.register(lambda: sc.stop())

rdd = sc.cassandraTable("el_test", "cockpit2_testIndexes")


# for( d in range 2015-10-01 ~ 2015-10-10 ) do:
#
#    SELECT url,date,site,cnts,cnt from cockpit2_allTogether where `date` = d and site = giga and tags contains resort:android
#
# after this query, every row has to be updated with new value for cnts:
#
# UPDATE cnts = ga_videoPlays + sda_downloads + fb_socialFacebookLikes + fb_socialFacebookShares + fb_socialFacebookComments + tw_socialTwitterShares + ga_socialGooglePlusShares + gigya_socialComments

def filterDateRage(_from, _to, col):
    loc = timezone('Europe/Berlin')
    dtf = loc.localize(datetime.strptime(_from, "%Y-%d-%m %H:%M"))
    dtt = loc.localize(datetime.strptime(_to, "%Y-%d-%m %H:%M"))
    def inner(row):
        return row[col]>dtf and row[col]<dtt

    return inner


def sumCounts(row):
    # {u'ga_videoPlaysForThisPost': 0, u'ga_VideoPlaysThisPost': 0, u'gigya_socialComments': 0, u'fb_socialFacebookComments': 0, u'cp_socialShares': 0, u'tw_socialTwitterShares': 0, u'sda_downloads': 0, u'fb_socialFacebookLikes': 0, u'fb_socialFacebookShares': 0, u'ga_videoPlays': 0, u'ga_videoPlaysInThisPost': 0, u'fb_socialSignalsSum': 0, u'ga_socialGooglePlusShares': 0}
    counts = row['counts']
    row['cnt'] = counts['ga_videoPlays'] + counts['sda_downloads'] + counts['fb_socialFacebookLikes'] + counts['fb_socialFacebookShares'] + counts['fb_socialFacebookComments'] + counts['tw_socialTwitterShares'] + counts['ga_socialGooglePlusShares'] + counts['gigya_socialComments']
    return row

data = rdd.select("url", "date", "counts", "cnt") \
    .where('"tags" contains ?', "channel:apple") \
    .filter(filterDateRage("2015-10-01 00:00", "2015-10-10 00:00", "date")) \
    .map(sumCounts)

rdd.saveToCassandra(data.collect())
