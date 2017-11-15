import getopt
import json
import time
from multiprocessing import Pool
import requests
import os
import sys
import tarfile
from tqdm import tqdm
import datetime

POST_URL = 'https://robin-api.oneprice.co.kr/items'
GET_URL = "https://robin-api.oneprice.co.kr/items/query"


def load(name):
    data = {}
    dir_path = os.path.dirname(os.path.realpath(__file__))
    tar = tarfile.open(dir_path + "/data/items.json.tar.gz")
    for member in tar.getmembers():
        if member.name == name + 'sec.json':
            f = tar.extractfile(member)
            data = json.loads(f.read().decode("utf-8"))
            break

    r_list = []
    for i in range(0, data.__len__()):
        data[str(i)]['data'][0]['meta'] = {}
        r_list.append(data[str(i)])
    return r_list


def post(data):
    mall_count = 0
    try:
        requests.post(POST_URL, json=data)
        mall_count = data['data'][0]['nodes'].__len__()
        # print('Post success ', data['mid'])
    except Exception as e:
        print(e)
        print("failed to send: ", data['mid'])
    return mall_count


def get(data):
    mall_count = 0
    try:
        url = GET_URL + '/' + data['id'];
        if data['data'][0]['pkey']:
            url += '_' + data['data'][0]['pkey']
        url += "?access_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImV0aGFuQHRoZWNvbW1lcmNlLmNvLmtyIiwiaWF0IjoxNTEwMTEyMTc3LCJleHAiOjE1MTI3MDQxNzcsImlzcyI6InRoZWNvbW1lcmNlLmNvLmtyIn0.xflfMQgnKmXwdxM1ewWD3k_kcvdkb6UvTfgSkxHZbks"
        requests.get(url)
    except Exception as e:
        print(e)
        print("failed to send: ", data['mid'])
    return mall_count


def run(name, thread, kind):
    item_list = load(name)
    print('--- Finish Loading ---')
    print('--- %d items loaded ---' % (item_list.__len__()))
    pool = Pool(processes=int(thread))

    print('--- Start Sending ---', kind)
    start_time = time.time()
    print(datetime.datetime.now())
    if kind is 'post':
        count_list = pool.map(post, tqdm(item_list))
    else:
        count_list = pool.map(get, tqdm(item_list))

    print(datetime.datetime.now())
    print("--- %s seconds ---" % (time.time() - start_time))
    item_count = 0
    mall_count = 0
    for item in count_list:
        item_count += 1
        mall_count += item
    print("--- Items: %d, Malls: %d ---" % (item_count, mall_count))


if __name__ == '__main__':
    file_name = '10'
    thread = 10
    kind = 'post'
    try:
        opts, args = getopt.getopt(sys.argv[1:], "k:s:t:", ["kind=", "sec=", "thread="])
    except getopt.GetoptError as err:
        print(str(err))
        sys.exit(2)
    for o, a in opts:
        if o in ("-s", "--sec"):
            file_name = a
        elif o in ("-t", "--thread"):
            thread = a
        elif o in ("-k", "--kind"):
            kind = a
        else:
            assert False, "unhandled option"
    run(file_name, thread, kind)
