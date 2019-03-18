#!/usr/bin/env python -*- coding: utf-8 -*-
import requests
import subprocess
from datetime import datetime
from operator import attrgetter
from subprocess import check_output, CalledProcessError, STDOUT
#import sqlite3

NTV_EDA_JIVAYA_I_MERTVAYA_JSON_URL = 'http://www.ntv.ru/m/v10/prog/Eda_jivaya_i_mertvaya/'
NTV_PEREDELKA_JSON_URL = 'http://www.ntv.ru/m/v10/prog/peredelka/'
NTV_DACHA_OTVET_JSON_URL = 'http://www.ntv.ru/m/v10/prog/dacha_otvet/'
NTV_CHUDO_TEHNILI_URL = 'http://www.ntv.ru/m/v10/prog/chudo_tehniki/'

NTV_CLIENT_USER_AGENT = 'ru.ntv.client_v4.9'
HEADERS = {'User-Agent': NTV_CLIENT_USER_AGENT}
DOWNLOAD_FOLDER = '/srv/dev-disk-by-label-media/downloads'


def downloadJson(jsonUrl):
    print('downloadJson(), jsonUrl: ', jsonUrl)
    resp = requests.get(url = jsonUrl, headers = HEADERS)
    data = resp.json()
    menus = data['data']['menus']

    videoItemList = []
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
                videoItemList.append(videoItem)
    return videoItemList


def getVideoUrl(videoItem):
    urls = [videoItem['hi_video'], videoItem['lo_video']]
    for url in urls:
        print('head for url: ', url)
        r = requests.head(url, headers = HEADERS)
        print('r.status_code: ', r.status_code)
        if r.status_code == 200:
            return url
    return None

# def createDb():
#     print('createDb()')
#     conn = sqlite3.connect('ntv-videos.db')
#     conn.execute('CREATE TABLE VIDEOS (id INT PRIMARY KEY NOT NULL), ms INT, title TEXT, sharelink TEXT, hi_video TEXT, lo_video TEXT')
#     print('Table created successfully')
#     conn.close()


def download(url):
    print('download(', url, ')')
    if url is not None:
        #subprocess.run(['wget', '-P', DOWNLOD_FOLDER, '-N', '-U', NTV_CLIENT_USER_AGENT, '-O', videoItem['title'] + '.mp4', url])
        #subprocess.run(['wget', '-P', DOWNLOD_FOLDER, '-N', '-U', NTV_CLIENT_USER_AGENT, url])
        command = ['aria2c', '--auto-file-renaming=false', '--dir=' + DOWNLOAD_FOLDER, '--user-agent=' + NTV_CLIENT_USER_AGENT, '--file-allocation=none', url]
        try:
            output = subprocess.run(command)
            print('Command output: ', output.returncode)
            if output.returncode == 0:
                return True
        except CalledProcessError as e:
            output = e.output.decode()
            print('ERROR in command: ', output)
    return False


def notify_downloaded(file_name):
    print('notify_downloaded(', file_name, ')')
    try:
        subprocess.run(['python3' '/opt/nas-scripts/notifier.py', 'Downloaded: ' + file_name, '-c' '#nas-transmission'])
    except Exception as e:
        print('ERROR in notify_downloaded: ', e)


if __name__ == '__main__':
    print('main')
    #createDb()

    if download('http://packages.openmediavault.org/public/dists/arrakis-proposed/main/binary-amd64/Packages.gz'):
        notify_downloaded('Packages.gz')

#    urls = [NTV_EDA_JIVAYA_I_MERTVAYA_JSON_URL, NTV_PEREDELKA_JSON_URL,
#            NTV_DACHA_OTVET_JSON_URL, NTV_CHUDO_TEHNILI_URL]
#
#    for url in urls:
#        print('url: ', url)
#        videoItemList = downloadJson(url)
#        #videoItemList.sort(key = attrgetter('ms'), reverse = False)
#        videoItem = videoItemList[0]
#        print('To download:', videoItem)
#        url = getVideoUrl(videoItem)
#        if download(url):
#            print('Downloaded SUCCESS')
#        else:
#            print('Downloaded FAIL')
