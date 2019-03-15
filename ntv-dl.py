#!/usr/bin/env python -*- coding: utf-8 -*-
import requests
import subprocess
from datetime import datetime
from operator import attrgetter

NTV_EDA_JIVAYA_I_MERTVAYA_JSON_URL = 'http://www.ntv.ru/m/v10/prog/Eda_jivaya_i_mertvaya/'
NTV_PEREDELKA_JSON_URL = 'http://www.ntv.ru/m/v10/prog/peredelka/'
NTV_DACHA_OTVET_JSON_URL = 'http://www.ntv.ru/m/v10/prog/dacha_otvet/'
NTV_CHUDO_TEHNILI_URL = 'http://www.ntv.ru/m/v10/prog/chudo_tehniki/'

NTV_CLIENT_USER_AGENT = 'ru.ntv.client_v4.9'
HEADERS = {'User-Agent': NTV_CLIENT_USER_AGENT}
DOWNLOD_FOLDER = '/srv/dev-disk-by-label-media/downloads'


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

if __name__ == '__main__':
    print('main')
    urls = [NTV_EDA_JIVAYA_I_MERTVAYA_JSON_URL, NTV_PEREDELKA_JSON_URL,
            NTV_DACHA_OTVET_JSON_URL, NTV_CHUDO_TEHNILI_URL]
    for url in urls:
        print('url: ', url)
        videoItemList = downloadJson(url)
        #videoItemList.sort(key = attrgetter('ms'), reverse = False)
        videoItem = videoItemList[0]
        print('To download:', videoItem)
        url = getVideoUrl(videoItem)
        print('Url to download:', url)
        if url is not None:
            print('Url start download:', url)
            #subprocess.run(['wget', '-P', DOWNLOD_FOLDER, '-N', '-U', NTV_CLIENT_USER_AGENT, '-O', videoItem['title'] + '.mp4', url])
            subprocess.run(['wget', '-P', DOWNLOD_FOLDER, '-N', '-U', NTV_CLIENT_USER_AGENT, url])

