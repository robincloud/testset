import getopt
import json
import time
from multiprocessing import Pool
import requests
import os
import sys
import tarfile

POST_URL = 'https://robin-api.oneprice.co.kr/items'


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
    try:
        requests.post(POST_URL, json=data)
        print('Post success ', data['mid'])
    except Exception as e:
        print(e)
        print("failed to send: ", data['mid'])


def run(name, thread):
    item_list = load(name)
    print('--- Finish Loading ---')
    print('--- %d items loaded ---' % (item_list.__len__()))
    pool = Pool(processes=thread)

    print('--- Start Sending ---')
    start_time = time.time()
    pool.map(post, item_list)
    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    file_name = '10'
    thread = 0
    try:
        opts, args = getopt.getopt(sys.argv[1:], "s:t:", ["sec=", "thread="])
    except getopt.GetoptError as err:
        print(str(err))
        sys.exit(2)
    for o, a in opts:
        if o in ("-s", "--sec"):
            file_name = a
        elif o in ("-t", "--thread"):
            thread = a
        else:
            assert False, "unhandled option"
    run(file_name, thread)
