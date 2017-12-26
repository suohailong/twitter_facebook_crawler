import tweepy
import pprint
import json
pp = pprint.PrettyPrinter(indent=4)


consumer_key = 'c58jPuNxqLex5QttLkoVF621T'
consumer_secret = "qU2EfulVxZ9a9mSPVm0bww4HXDyC8qk4a2gQrq7bgy4dKOqfup"
access_token = "930249938012798978-BJCWSdIgciyVZ0IUKLXVXLlc1A3D2my"
access_secret = "HjDrf1nvRDZIT5NSXioGVeOeZoev26Ibi08hCBQMhMof4"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


# user = api.get_user('BColwell_EMGMKT')
# print(user)
posted = api.user_timeline('BColwell_EMGMKT')

for post in posted:
    print('\n')
    if post.id == int('941172077838635009'):
        print(post)
        print('\n')
        print(post.entities)
        print('\n')