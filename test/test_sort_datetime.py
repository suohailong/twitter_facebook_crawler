from pymongo import MongoClient,DESCENDING,ReturnDocument
client = MongoClient('mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin')
dbs = client.TUserPost  # config['mongo_config']['db']

dbs.Tuser_post.distinct('message').length

