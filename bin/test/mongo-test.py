#!/usr/bin/env python
from pymongo import MongoClient
import datetime

# has to be n1 to write
client = MongoClient('n1', 27017)
print "client"+str(client)
db = client.posts
print str(db)
posts = db.posts
print str(posts)
post = {"author": "Mike",
	"text": "My first blog post!",
	"tags": ["mongodb","python","pymongo"],
	"date": datetime.datetime.utcnow()}
print "inserting "+str(post)
# this can be multiple posts
post_id = posts.insert(post)
print post_id
print str(posts.find_one({'_id':post_id}))



