import json

def merge_user_ids():
    user_ids = {
        'ids':[]
    }
    for name in ['twitter_user_ids%s.json' % i if i != 0 else 'twitter_user_ids.json' for i in range(0,3)]:
        print(name)
        with open(name,'r') as f:
          user_ids['ids'].extend(json.load(f)['ids'])
    print('去重前')
    print(len(user_ids['ids']))
    user_ids['ids'] = list(set(user_ids['ids']))
    print('去重后')
    print(len(user_ids['ids']))
    with open('twitter_user_ids.json','w') as f:
        json.dump(user_ids, f, ensure_ascii=False, indent=4)



if __name__ == '__main__':
    merge_user_ids()