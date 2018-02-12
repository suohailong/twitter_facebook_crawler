import json


if __name__ == '__main__':
    with open('fbids.json') as f:
        fbids = json.load(f)['RECORDS']
    with open('facebook_user_ids.json') as f:
        fb_user_ids = json.load(f)['ids']
    wei = []
    for id in fbids:
        if id['facebookId'] in fb_user_ids:
            pass;
        else:
            wei.append()