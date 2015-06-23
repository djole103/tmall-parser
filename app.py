from pymongo import MongoClient
from pprint import pformat

host = "127.0.0.1"
client = MongoClient(host, 27017)
database = client['DataCrawler']
collection = database['product-tmall']

filteredObjs = {}
missed = []

def get(entry,field):
    def id():
        if 'id' in entry: return entry['id']
        else: return 'NaN'
    def prodParameters():
        if 'prodDetails' in entry and 'prodParameters' in entry['prodDetails']:
            return entry['prodDetails']['prodParameters']
        else: return None
    def usualPrice():
        if 'salesDetails' in entry and 'usualPrice' in entry['salesDetails']:
            return entry['salesDetails']['usualPrice']
        else: return None
    def monthlySale():
        if 'salesDetails' in entry and 'monthlySale' in entry['salesDetails']:
            return entry['salesDetails']['monthlySale']
        else: return None

    cases = {
            'id'             : id,
            'prodParameters' : prodParameters,
            'usualPrice'     : usualPrice,
            'monthlySale'    : monthlySale
            }

    return cases[field]()

def parseParams(params):
    if params is None: return None
    tabDelim = params.split('\t')
    colonDelim = {}
    for entry in tabDelim:
        pair = entry.split(':')
        if len(pair)!=2:
            pass
        else:
            colonDelim[pair[0]] = pair[1]
    return colonDelim

for entry in collection.find():
    try:
        id = get(entry,'id')
        filteredObjs[id] = dict( id          = id,
                                 usualPrice  = get(entry,'usualPrice'),
                                 monthlySale = get(entry,'monthlySale')
                                )
        prodParameters  = get(entry,'prodParameters')
        parsedParams = parseParams(prodParameters)
        filteredObjs[id]['prodParameters'] = parsedParams

    except Exception as e:
        print(e)
        missed.append(entry['id'])
        continue

with open("mongodbJSON",'w+') as f:
    f.write(pformat(filteredObjs))