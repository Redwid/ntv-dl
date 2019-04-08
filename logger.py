#!/usr/bin/env python -*- coding: utf-8 -*-
import logging
from logging.handlers import RotatingFileHandler

def getNasLogger(name):
    formatString = '%(asctime)s: %(name)s: %(threadName)s - %(levelname)s - %(message)s'

    logging.basicConfig(level=logging.DEBUG,
                        format=formatString)
    logger = logging.getLogger(name)
    try:
        handler = RotatingFileHandler('/var/log/nas-scripts/{}.log'.format(name), maxBytes=1048576, backupCount=3)
        formatter = logging.Formatter(formatString)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    except PermissionError:
        logger.error('PermissionError in init logger ', exc_info=True)
    except FileNotFoundError:
        logger.error('FileNotFoundError in init logger ', exc_info=True)
    return logger