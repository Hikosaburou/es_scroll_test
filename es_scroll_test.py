#!/usr/bin/env python
import boto3
import os
from os import path
import sys
import re
import logging
import datetime
from datetime import date, timedelta
import argparse
import toml
import json
from dateutil import parser, tz, zoneinfo
from time import time
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers
from elasticsearch.exceptions import ElasticsearchException
import requests
from requests_aws4auth import AWS4Auth
import smtplib
from email.mime.text import MIMEText
import time


def set_query(delete_retention):
    '''
    Description: 日付を元にESに投げるクエリを作成する
    '''

    data = {
        'query': {
            'range': {
                'timestamp': {
                    'lte': 'now-{}d/d'.format(delete_retention),
                    'time_zone': '+09:00'
                }
            }
        }
    }

    return json.dumps(data)


def ess_client(args):
    es = Elasticsearch(
        hosts=[args.host],
        port=args.port,
        scheme=args.scheme
    )

    return es


def main():
    psr = argparse.ArgumentParser()
    psr.add_argument('--host', default='localhost')
    psr.add_argument('--port', default='9922')
    psr.add_argument('--scheme', default='http')
    psr.add_argument('--index', default='twitter')
    args = psr.parse_args()
    print(args)

    # Get ES Clinet
    es_client = ess_client(args)

    ###
    query = set_query(delete_retention=10)
    print(query)
    response = es_client.search(
        index=args.index, body=query, scroll='5m', size='1000')
    print(response)


if __name__ == '__main__':
    main()
