#!/usr/bin/env python -*- coding: utf-8 -*-
import requests
import subprocess
import json
import time
import os
from pyaria2 import Aria2RPC
# from slugify import slugify
from datetime import datetime
from subprocess import CalledProcessError
from logger import getNasLogger


NTV_EDA_JIVAYA_I_MERTVAYA_JSON_URL = 'http://www.ntv.ru/m/v10/prog/Eda_jivaya_i_mertvaya/'
NTV_PEREDELKA_JSON_URL = 'http://www.ntv.ru/m/v10/prog/peredelka/'
NTV_DACHA_OTVET_JSON_URL = 'http://www.ntv.ru/m/v10/prog/dacha_otvet/'
NTV_CHUDO_TEHNIKI_JSON_URL = 'http://www.ntv.ru/m/v10/prog/chudo_tehniki/'
NTV_URLS = [NTV_EDA_JIVAYA_I_MERTVAYA_JSON_URL, NTV_PEREDELKA_JSON_URL,
            NTV_DACHA_OTVET_JSON_URL, NTV_CHUDO_TEHNIKI_JSON_URL]

DOWNLOADED_TXT = '/opt/ntv-dl/downloaded.txt'


NTV_CLIENT_USER_AGENT = 'ru.ntv.client_v4.9'
HEADERS = {'User-Agent': NTV_CLIENT_USER_AGENT}
DOWNLOAD_FOLDER = '/srv/dev-disk-by-label-media/downloads'

logger = getNasLogger('ntv-dl')


def download_json(json_url):
    # print('download_json(), jsonUrl: ', json_url)
    logger.info('download_json(), jsonUrl: %s', json_url)
    resp = requests.get(url = json_url, headers = HEADERS)
    data = resp.json()
    menus = data['data']['menus']

    video_item_list = []
    for item in menus:
        ms = 0
        sharelink = ''
        hi_video = ''
        lo_video = ''
        if 'issues' in item['data']:
            issues = item['data']['issues']
            for issue in issues:
                id = issue['id']
                title = issue['title']
                if 'video_list' in issue:
                    video_list = issue['video_list']
                    for video in video_list:
                        ms = video['ts']
                        #print(datetime.utcfromtimestamp(ms//1000).replace(microsecond=ms%1000*1000))
                        sharelink = video['sharelink']
                        hi_video = video['hi_video']
                        lo_video = video['video']
                videoItem = {}
                videoItem['id'] = id
                videoItem['ms'] = ms
                videoItem['title'] = title
                videoItem['sharelink'] = sharelink
                videoItem['hi_video'] = hi_video
                videoItem['lo_video'] = lo_video
                video_item_list.append(videoItem)
    return video_item_list


def get_video_url(video_item):
    # print('  get_video_url(', video_item, ')')
    logger.info('get_video_url(%s)', video_item)
    urls = [video_item['hi_video'], video_item['lo_video']]
    for url in urls:
        # print('    get_video_url(', url, ')')
        logger.info('get_video_url(%s)', video_item)
        r = requests.head(url, headers = HEADERS)
        # print('    get_video_url(), r.status_code: ', r.status_code)
        logger.info('get_video_url(), r.status_code: %s', r.status_code)
        if r.status_code == 200:
            # print('    get_video_url() found url: ', url)
            logger.info('get_video_url(), found url: %s', url)
            return url
    # print('    get_video_url() ERROR no url found!')
    logger.info('get_video_url(), ERROR no url found!')
    return None


def download(url, file_name):
    # print('  download(', url, ',', file_name, ')')
    logger.info('download(%s, %s)', url, file_name)
    if url is not None:
        #subprocess.run(['wget', '-P', DOWNLOD_FOLDER, '-N', '-U', NTV_CLIENT_USER_AGENT, '-O', videoItem['title'] + '.mp4', url])
        #subprocess.run(['wget', '-P', DOWNLOD_FOLDER, '-N', '-U', NTV_CLIENT_USER_AGENT, url])
        command = ['aria2c',
                   '--enable-rpc',
                   '--auto-file-renaming=false',
                   '--user-agent=' + NTV_CLIENT_USER_AGENT,
                   '--file-allocation=none',
                   '--dir=' + DOWNLOAD_FOLDER,
                   '--out=' + file_name,
                   url]
        try:
            output = subprocess.run(command)
            # print('    download(), command output: ', output.returncode)
            logger.info('download(), command output: ', output.returncode)
            if output.returncode == 0:
                return True
        except ConnectionRefusedError as e:
            output = e.output.decode()
            # print('    ERROR in command: ', output)
            logger.error('ERROR in download()', exc_info=True)
    return False


def downloadByRpc(url, file_name):
    # print('  downloadByRpc(', url, ',', file_name, ')')
    logger.info('downloadByRpc(%s, %s)', url, file_name)
    if url is not None:
        server = Aria2RPC()
        try:
            options = {}
            options['auto-file-renaming'] = 'false'
            options['user-agent'] = NTV_CLIENT_USER_AGENT
            options['out'] = file_name

            gid = server.addUri([url], options)
            status = server.tellStatus(gid)
            while status['status'] == 'active':
                status = server.tellStatus(gid)
                # print('    downloadByRpc', 'status:', status)
                logger.info('downloadByRpc(), status: %s', status)
                time.sleep(5)

            if status['status'] == 'complete':
                # print('    downloadByRpc(), file gid: ', status['gid'])
                logger.info('downloadByRpc(), file gid: %s', status['gid'])
                return True
        except Exception as e:
            # print('    ERROR in command: ', output)
            logger.error('ERROR in downloadByRpc()', exc_info=True)
    return False


def notify_downloaded(file_name):
    # print('  notify_downloaded(', file_name, ')')
    logger.info('notify_downloaded(%s)', file_name)
    notifier_script = '/opt/nas-scripts/notifier.py'
    if os.path.isfile(notifier_script):
        try:
            subprocess.run(['python3', notifier_script, 'Aria2 downloaded: ' + file_name, '-c' '#nas-transmission'])
        except Exception as e:
            # print('    ERROR in notify_downloaded: ', e)
            logger.error('ERROR in notify_downloaded', exc_info=True)
    else:
        # print('    ERROR notify script is not exists')
        logger.error('ERROR notify script is not exists')


def store_downloaded(video_item):
    # print('  store_downloaded(', video_item, ')')
    logger.info('store_downloaded(%s)', video_item)
    with open(DOWNLOADED_TXT, 'a') as f:
        json.dump(video_item, f)
        print('', file=f)


def read_downloaded():
    # print('read_downloaded()')
    logger.info('read_downloaded()')
    data_store = []
    try:
        with open(DOWNLOADED_TXT, 'r') as f:
            line = 'value'
            while line:
                line = f.readline()
                data_store.append(json.loads(line))
    except Exception as e:
        # print('  ERROR in read_downloaded: ', e)
        logger.error('ERROR in read_downloaded', exc_info=True)
    return data_store


def is_item_already_downloaded(video_item, data_store):
    # print('  is_item_already_downloaded(', video_item, ')')
    logger.info('is_item_already_downloaded(%s)', video_item)
    for item in data_store:
        id_equals = item['id'] == video_item['id']
        title_equals = item['title'] == video_item['title']
        sharelink_equals = item['sharelink'] == video_item['sharelink']
        if id_equals and title_equals and sharelink_equals:
            # print('    is_item_already_downloaded() True')
            logger.info('is_item_already_downloaded() True')
            return True
    # print('    is_item_already_downloaded() False')
    logger.info('is_item_already_downloaded() False')
    return False


def format_time(time):
    return datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')


def format_time_simple(time):
    return datetime.fromtimestamp(time).strftime('%Y-%m-%d %H.%M')


def get_time_stamp():
    ts = time.time()
    return format_time(ts)


def process_urls():
    # print('process_urls(), time: ', get_time_stamp())
    logger.info('process_urls() %s', get_time_stamp())
    for url in NTV_URLS:
        video_item_list = download_json(url)
        # video_item_list.sort(key = attrgetter('ms'), reverse = False)
        video_item = video_item_list[0]
        if not is_item_already_downloaded(video_item, downloaded_video_item_list):
            url = get_video_url(video_item)
            file_name = video_item['title'] + ' (' + format_time_simple(video_item['ms']/1000.0) + ').mp4'
            #file_name = slugify(file_name)

            if downloadByRpc(url, file_name):
                # print('  process_urls(), downloaded SUCCESS')
                logger.info('process_urls() downloaded SUCCESS')
                store_downloaded(video_item)
                notify_downloaded(file_name)
                return True
            else:
                # print('  process_urls(), downloaded FAIL')
                logger.info('process_urls() downloaded FAIL')
    return False

if __name__ == '__main__':
    # print('main(), time:', get_time_stamp())
    logger.info('main(), time: %s', get_time_stamp())

    # if downloadByRpc('http://packages.openmediavault.org/public/dists/arrakis-proposed/main/binary-amd64/Packages.gz', 'downloaded-packages.gz'):
    #    store_downloaded('value1')
    #    notify_downloaded('Packages.gz')


    downloaded_video_item_list = read_downloaded()
    # print('downloaded_video_item_list: ', downloaded_video_item_list)
    logger.info('downloaded_video_item_list: %s', downloaded_video_item_list)

    for x in range(15):
        # print('main(): #', x)
        logger.info('main(): #%d', x)
        if process_urls():
            break
        # print('main(), sleep')
        logger.info('main(), sleep')
        time.sleep(60)

    # print('main(), done, time:', get_time_stamp())
    logger.info('main(), done, time: %s', get_time_stamp())

