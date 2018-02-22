import json
from datetime import datetime

if __name__ == '__main__':
    utctime = datetime.utcfromtimestamp('1483665133').strftime('%Y-%m-%dT%H:%M:%S.000Z')
    print(utctime)