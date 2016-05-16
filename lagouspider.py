#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import proxies
import requests
import sys
import MySQLdb
from job import Job
import logging
from bs4 import BeautifulSoup
reload(sys)
sys.setdefaultencoding('utf-8')

req_url = 'http://www.lagou.com/jobs/positionAjax.json?'
headers = {'content-type': 'application/json;charset=UTF-8'}


def readweb(p):
    configmap = {}
    while(True):
        proxies = p.getproxies()
        try:
            r = requests.get(url='http://www.lagou.com/',
                             proxies=proxies, timeout=60)
            break
        except Exception, e:
            p.nextip()
            logging.warning(str(Exception) + ":" + str(e))
    soup = BeautifulSoup(r.content, 'html.parser')
    for menubox in soup.select('.menu_box'):
        jobtype = menubox.h2.text.strip()
        joblist = list()
        if jobtype != '':
            for a in menubox.find_all('a'):
                jobname = a.text.strip().replace('/', '')
                job_obj = Job(jobname, jobname)
                joblist.append(job_obj)
            configmap[jobtype] = joblist
    logging.info('configmap.size:'+str(len(configmap.keys())))
    return configmap


def scrapy(jobname, p):
    while(True):
        proxies = p.getproxies()
        try:
            maxpagenum = \
                requests.post(req_url, params={'first': 'false', 'pn': 1, 'kd': jobname}, headers=headers, timeout=60, proxies=proxie, allow_redirects=False).json()['content'][
                    'totalPageCount']
            break
        except Exception, e:
            p.nextip()
            logging.debug(str(Exception) + ":" + str(e))
    flag = True
    num = 1
    while flag:
        payload = {'first': 'false', 'pn': num, 'kd': jobname}
        while (True):
            try:
                response = requests.post(
                    req_url, params=payload, headers=headers, proxies=proxies, timeout=60)
                break
            except Exception, e:
                p.nextip()
                proxies = p.getproxies()
                logging.debug(str(Exception) + ":" + str(e))
        if num > maxpagenum:
            flag = False
        if response.status_code == 200:
            try:
                job_json = response.json()['content']['result']
            except Exception, e:
                logging.debug(str(Exception) + ":" + str(e))
        else:
            print('connect error! url = ' + req_url)
            return None
        num += 1
    return job_json

def normalize(value):
    if '-' in value:
        values = value.split('-')
        min = int(values[0].split('k')[0]) * 1000
        max = int(values[1].split('k')[0]) * 1000
        result = int((min + max) / 2)
    else:
        result = int(value.split('k')[0]) * 1000

    return result

def write_db(job_json):
    db = MySQLdb.connect("localhost", "root", "123123", "jobs",use_unicode=True, charset="utf8")
    cursor = db.cursor()
    for each_job_info_obj in job_json:
        values = []
        values.append(each_job_info_obj['positionId'])
        values.append(each_job_info_obj['companyId'])
        values.append(each_job_info_obj['formatCreateTime'].encode('utf-8'))
        values.append(each_job_info_obj['workYear'].encode('utf-8'))
        values.append(each_job_info_obj['positionName'].encode('utf-8'))
        values.append(each_job_info_obj['positionType'].encode('utf-8'))
        values.append(each_job_info_obj['companyName'].encode('utf-8'))
        values.append(each_job_info_obj['city'].encode('utf-8'))
        values.append(each_job_info_obj['education'].encode('utf-8'))
        values.append(each_job_info_obj['industryField'].encode('utf-8'))
        values.append(each_job_info_obj['financeStage'].encode('utf-8'))
        values.append(each_job_info_obj['salary'].encode('utf-8'))
        values.append(each_job_info_obj['companySize'].encode('utf-8'))
        values.append(normalize(each_job_info_obj['salary']))
        values.append('')
        try:
            cursor.execute(
                'insert into lagoujobs values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', values)
            db.commit()
        except Exception, e:
            logging.debug(str(Exception)+":"+str(e))
            continue
        values = []
        values.append(each_job_info_obj['companyId'])
        values.append(each_job_info_obj['companyName'].encode('utf-8'))
        values.append('')
        values.append('')
        try:
            cursor.execute(
                'insert into company values (%s,%s,%s,%s)', values)
            db.commit()
        except Exception, e:
            logging.debug(str(Exception) + ":" + str(e))
            continue
    cursor.close()
    db.close()
    return


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logfile = logging.FileHandler("log.txt")
    logger.addHandler(logfile)
    import coloredlogs
    coloredlogs.install(level='DEBUG')
    p = proxies.Proxy()
    readweb(p)
