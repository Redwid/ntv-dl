#!/usr/bin/env python -*- coding: utf-8 -*-
import logging
import os
from logging.handlers import RotatingFileHandler


def getNasLogger(name):
    formatString = '%(asctime)s: %(name)s: %(threadName)s - %(levelname)s - %(message)s'

    logging.basicConfig(level=logging.DEBUG,
                        format=formatString)
    logger = logging.getLogger(name)
    try:
        path = '{}.log'
        if os.path.isfile('/var/log/nas-scripts/'):
            path = '/var/log/nas-scripts/{}.log'
        handler = RotatingFileHandler(path.format(name), maxBytes=1048576, backupCount=3)
        formatter = logging.Formatter(formatString)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    except PermissionError as e:
        print('PermissionError in init logger', e)
        logger.error('PermissionError in init logger ', exc_info=True)
    except FileNotFoundError as e:
        print('FileNotFoundError in init logger', e)
        logger.error('FileNotFoundError in init logger ', exc_info=True)
    return logger