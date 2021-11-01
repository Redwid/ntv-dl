#!/usr/bin/env python -*- coding: utf-8 -*-
import requests
import subprocess
import json
import time
import os
import argparse
from pyaria2 import Aria2RPC
# from slugify import slugify
from datetime import datetime
from subprocess import CalledProcessError
from logger import getNasLogger
from xml.sax.saxutils import escape

import pyodbc
import sqlalchemy as sal
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import or_
from sqlalchemy.sql import func


NTV_EDA_JIVAYA_I_MERTVAYA_JSON_URL = 'http://www.ntv.ru/m/v10/prog/Eda_jivaya_i_mertvaya/'
NTV_PEREDELKA_JSON_URL = 'http://www.ntv.ru/m/v10/prog/peredelka/'
NTV_DACHA_OTVET_JSON_URL = 'http://www.ntv.ru/m/v10/prog/dacha_otvet/'
NTV_CHUDO_TEHNIKI_JSON_URL = 'http://www.ntv.ru/m/v10/prog/chudo_tehniki/'
NTV_URLS = [NTV_EDA_JIVAYA_I_MERTVAYA_JSON_URL, NTV_PEREDELKA_JSON_URL,
            NTV_DACHA_OTVET_JSON_URL, NTV_CHUDO_TEHNIKI_JSON_URL]

DOWNLOADED_TXT = '/opt/ntv-dl/downloaded.txt'


NTV_CLIENT_USER_AGENT = 'ru.ntv.client_v4.9'
HEADERS = {'User-Agent': NTV_CLIENT_USER_AGENT}
DOWNLOAD_FOLDER = '/mnt/media/data/downloads'

logger = getNasLogger('ntv-dl')

Base = declarative_base()

class Downloaded(Base):
    __tablename__ = 'downloaded'
    id = Column('id', Integer, index = True, primary_key = True, autoincrement = True)
    downloaded_id = Column('downloaded_id', Text)
    title = Column('title', Text)
    lo_video = Column('lo_video', Text)
    hi_video = Column('hi_video', Text)
    program_title = Column('program_title', Text)
    text = Column('text', Text)
    preview = Column('preview', Text)
    sharelink = Column('sharelink', Text)
    time = Column('time', DateTime)

    def __str__(self):
        return "Download[id:{}, downloaded_id:{}, title:{}, sharelink: {}, time: {}".format(self.id, self.downloaded_id, self.title, self.sharelink, self.time)


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
        text = ''
        preview = ''
        program_title = ''
        if 'issues' in item['data']:
            issues = item['data']['issues']
            for issue in issues:
                id = issue['id']
                title = issue['title']
                text = issue['txt']
                program_title = issue['program_title']
                if 'video_list' in issue:
                    video_list = issue['video_list']
                    for video in video_list:
                        ms = video['ts']
                        #print(datetime.utcfromtimestamp(ms//1000).replace(microsecond=ms%1000*1000))
                        sharelink = video['sharelink']
                        hi_video = video['hi_video']
                        lo_video = video['video']
                        preview = video['preview']
                videoItem = {}
                videoItem['id'] = id
                videoItem['ms'] = ms
                videoItem['title'] = sanitize_after_xml(title)
                videoItem['sharelink'] = sharelink
                videoItem['hi_video'] = hi_video
                videoItem['lo_video'] = lo_video
                videoItem['text'] = sanitize_after_xml(text)
                videoItem['preview'] = preview
                videoItem['program_title'] = program_title
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
                   '--enable-rpc=true',
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


def download_by_rpc(url, dir_program_path, file_name):
    # print('  download_by_rpc(', url, ',', file_name, ')')
    logger.info('download_by_rpc(%s, %s, %s)', url, dir_program_path, file_name)

    # if True:
    #     return

    if url is not None:
        server = Aria2RPC()
        try:
            options = {}
            options['auto-file-renaming'] = 'false'
            options['user-agent'] = NTV_CLIENT_USER_AGENT
            # options['dir'] = dir_path
            options['out'] = dir_program_path + '/' + file_name

            gid = server.addUri([url], options)
            status = server.tellStatus(gid)
            while status['status'] == 'active':
                status = server.tellStatus(gid)
                # print('    download_by_rpc', 'status:', status)
                logger.info('download_by_rpc(), status: %s', status)
                time.sleep(5)

            if status['status'] == 'complete':
                # print('    download_by_rpc(), file gid: ', status['gid'])
                logger.info('download_by_rpc(), file gid: %s', status['gid'])
                return True
        except Exception as e:
            # print('    ERROR in command: ', output)
            logger.error('ERROR in download_by_rpc()', exc_info=True)
    return False


def notify_downloaded(file_name):
    # print('  notify_downloaded(', file_name, ')')
    logger.info('notify_downloaded(%s)', file_name)
    notifier_script = '/opt/nas-scripts/notify/aria_done_notify.sh'
    if os.path.isfile(notifier_script):
        try:
            subprocess.run([notifier_script, file_name])
        except Exception as e:
            # print('    ERROR in notify_downloaded: ', e)
            logger.error('ERROR in notify_downloaded', exc_info=True)
    else:
        # print('    ERROR notify script is not exists')
        logger.error('ERROR notify script is not exists')


def store_nfo_file(video_item, dir_path, file_name_nfo):
    logger.info('store_nfo_file(%s, %s, %s)', video_item, dir_path, file_name_nfo)
    with open(dir_path + '/' + file_name_nfo, 'w+') as f:
        f.write('<?xml version="1.0" encoding="utf-8"?>\n')
        f.write('<episodedetails>\n')
        f.write('    <title>' + escape(video_item['title']) + '</title>\n')
        f.write('    <showtitle>' + escape(video_item['program_title']) + '</showtitle>\n')
        f.write('    <plot>' + escape(video_item['text']) + '</plot>\n')
        f.write('    <season>1</season>\n')
        f.write('    <thumb aspect="banner">' + escape(video_item['preview']) + '</thumb>\n')
        f.write('    <aired>' + format_time_simple(video_item['ms']/1000.0) + '</aired>\n')
        f.write('</episodedetails>\n')
    return


def store_downloaded(video_item, session):
    try:
        store_downloaded_to_db(video_item, session)
    except Exception as e:
        # print('  ERROR in read_downloaded: ', e)
        logger.error('ERROR in store_downloaded to db failed saving to file', exc_info=True)
        store_downloaded_to_file(video_item)


def store_downloaded_to_file(video_item):
    # print('  store_downloaded(', video_item, ')')
    logger.info('store_downloaded(%s)', video_item)
    with open(DOWNLOADED_TXT, 'a') as f:
        json.dump(video_item, f)
        print('', file=f)

def store_downloaded_to_db(video_item, session):
    # print('  store_downloaded(', video_item, ')')
    logger.info('store_downloaded(%s)', video_item)

    try:
        downloaded = Downloaded(downloaded_id = get_value(video_item, 'id'),
                                title = get_value(video_item, 'title'),
                                lo_video = get_value(video_item, 'lo_video'),
                                hi_video = get_value(video_item, 'hi_video'),
                                program_title = get_value(video_item, 'program_title'),
                                text = get_value(video_item, 'text'),
                                preview = get_value(video_item, 'preview'),
                                time = time.strftime('%Y-%m-%d %H:%M:%S'))
        session.add(downloaded)
        session.commit()
    except Exception as e:
        # print('  ERROR in read_downloaded: ', e)
        logger.error('ERROR in store_downloaded_to_db', exc_info=True)


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


def is_item_already_downloaded_in_db(video_item, session):
    logger.info('is_item_already_downloaded_in_db(%s)', video_item)

    #records = session.query(Downloaded).filter(or_(Downloaded.downloaded_id == video_item['id'],
    #                                               Downloaded.title == video_item['title'],
    #                                               Downloaded.sharelink == video_item['sharelink'])).all()

    records_by_id = session.query(Downloaded).filter(Downloaded.downloaded_id == video_item['id']).all()

    if len(records_by_id) != 0:
        logger.info('is_item_already_downloaded_in_db(), records_by_id: %s', records_by_id[0])
        return True

    records_by_title = session.query(Downloaded).filter(Downloaded.title == video_item['title']).all()
    if len(records_by_title) != 0:
        logger.info('is_item_already_downloaded_in_db(), records_by_title: %s', records_by_title[0])
        return True

    records_by_sharelink = session.query(Downloaded).filter(Downloaded.sharelink == video_item['sharelink']).all()
    if len(records_by_sharelink) != 0:
        logger.info('is_item_already_downloaded_in_db(), records_by_sharelink: %s', records_by_sharelink[0])
        return True

    logger.info('is_item_already_downloaded_in_db(), False')
    return False


def format_time(time):
    return datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')


def format_time_simple(time):
    return datetime.fromtimestamp(time).strftime('%Y-%m-%d %H.%M')


def get_time_stamp():
    ts = time.time()
    return format_time(ts)


def process_urls(downloaded_list, session):
    # print('process_urls(), time: ', get_time_stamp())
    logger.info('process_urls() %s', get_time_stamp())
    for url in NTV_URLS:
        video_item_list = download_json(url)
        # video_item_list.sort(key = attrgetter('ms'), reverse = False)
        video_item = video_item_list[0]
        if not is_item_already_downloaded_in_db(video_item, session):
            url = get_video_url(video_item)
            file_name = video_item['title'] + ' (' + format_time_simple(video_item['ms']/1000.0) + ')'
            file_name = sanitize_for_file_system(file_name)
            file_name_mp4 = file_name + '.mp4'
            file_name_nfo = file_name + '.nfo'

            dir_program_path = sanitize_for_file_system(video_item['program_title'])
            dir_full_path = DOWNLOAD_FOLDER + '/' + dir_program_path
            os.makedirs(dir_full_path, exist_ok=True)

            if download_by_rpc(url, dir_program_path, file_name_mp4):
                # print('  process_urls(), downloaded SUCCESS')
                logger.info('process_urls() downloaded SUCCESS')
                store_nfo_file(video_item, dir_full_path, file_name_nfo)
                store_downloaded(video_item, session)
                notify_downloaded(dir_program_path + '/' + file_name)
                return True
            else:
                # print('  process_urls(), downloaded FAIL')
                logger.info('process_urls() downloaded FAIL')
    return False

def sanitize_for_file_system(file_name):
    logger.info('sanitize_for_file_system(), file_name: %s', file_name)
    file_name = file_name.replace(":", "-")
    file_name = file_name.replace("?", "")
    file_name = file_name.replace("/", "-")
    file_name = file_name.replace("\\", "-")
    file_name = file_name.replace("&", " AND ")
    file_name = file_name.replace("|", "-")
    file_name = file_name.replace(">", "-")
    file_name = file_name.replace("<", "-")
    file_name = file_name.replace("«", "")
    file_name = file_name.replace("»", "")
    logger.info('sanitize_for_file_system(), new file_name: %s', file_name)
    return file_name


def sanitize_after_xml(text):
    text = text.replace("\n", "")
    text = text.replace("   ", " ")
    text = text.replace("  ", " ")
    return text

def get_value(item, key):
    if key in item:
        return item[key]

    return ""

#Convert DB to utf8mb4 format
#https://mathiasbynens.be/notes/mysql-utf8mb4#utf8-to-utf8mb4
#
def get_db_session(db_user_name, db_user_password):
    url = "mysql+mysqlconnector://{}:{}@192.168.1.29:3306/ntv".format(db_user_name, db_user_password)
    engine = sal.create_engine(url, echo = False)
    connection = engine.connect()
    metadata = sal.MetaData()

    Base.metadata.create_all(engine)

    Session = sessionmaker(bind = engine)
    Session.configure( bind = engine)
    session = Session()
    return session


def migrate_to_db(db_user_name, db_user_password, data_store):
    session = get_db_session(db_user_name, db_user_password)
    try:
        for item in data_store:
            logger.info('migrate_to_db() item: %s', item)
            downloaded = Downloaded(downloaded_id = get_value(item, 'id'),
                                    title = get_value(item, 'title'),
                                    lo_video = get_value(item, 'lo_video'),
                                    hi_video = get_value(item, 'hi_video'),
                                    program_title = get_value(item, 'program_title'),
                                    text = get_value(item, 'text'),
                                    preview = get_value(item, 'preview'),
                                    sharelink = get_value(item, 'sharelink'),
                                    time = datetime.now())
            logger.info('migrate_to_db() downloaded: %s', downloaded)
            try:
                session.add(downloaded)
            except Exception as e:
                logger.error('ERROR in migrate_to_db', exc_info=True)
    except Exception as e:
        # print('  ERROR in read_downloaded: ', e)
        logger.error('ERROR in migrate_to_db', exc_info=True)
    session.commit()



if __name__ == '__main__':
    # print('main(), time:', get_time_stamp())
    logger.info('main(), time: %s', get_time_stamp())

    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--user", help="The db user name")
    parser.add_argument("-p", "--password", help="The db user password")
    args = parser.parse_args()

    logger.info('main() db_user_name: %s, db_user_password: %s', args.user, args.password)

    # if download_by_rpc('http://packages.openmediavault.org/public/dists/arrakis-proposed/main/binary-amd64/Packages.gz', 'downloaded-packages.gz'):
    #    store_downloaded('value1')
    #    notify_downloaded('Packages.gz')

    main_session = get_db_session(args.user, args.password)

    downloaded_video_item_list = []#read_downloaded()

    # migrate_to_db(args.user, args.password, downloaded_video_item_list)

    # print('downloaded_video_item_list: ', downloaded_video_item_list)
    logger.info('downloaded_video_item_list: %s', downloaded_video_item_list)

    count = 0
    for x in range(40):
        # print('main(): #', x)
        logger.info('main(): #%d', x)
        if process_urls(downloaded_video_item_list, main_session):
            count = count + 1
            logger.info('main(): #%d, count: %d', x, count)
            if count >= 2:
                logger.info('main(): #%d, break', x)
                break
        # print('main(), sleep')
        logger.info('main() #%d, sleep', x)
        time.sleep(60*5) #5 min

    # print('main(), done, time:', get_time_stamp())
    logger.info('main(), done, time: %s', get_time_stamp())

