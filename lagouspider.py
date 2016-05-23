#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
from proxies import Proxy
import requests
import sys
import json
import MySQLdb
import logging
import multiprocessing
from bs4 import BeautifulSoup
import time
import pickle

reload(sys)
sys.setdefaultencoding('utf-8')
date = time.strftime('%Y-%m-%d-%X', time.localtime()).replace(':', '-')
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s line:%(lineno)d [%(levelname)s]:%(message)s',
                    filename='logs/' + date + '.log',
                    filemode='a')
dbadd = 'localhost'
user = 'root'
password = '123123'
database = 'jobs'


# 读取职位列表
def readweb():
    print u'get jobdict.....'
    configmap = {}
    jobnamesfile = 'jobnames.pkl'
    if os.path.isfile(jobnamesfile):
        configmap = pickle.load(open(jobnamesfile))
        return configmap
    p = Proxy()
    while (True):
        proxies = p.getproxies()
        try:
            r = requests.get(url='http://www.lagou.com/',
                             proxies=proxies, timeout=60)
            break
        except Exception, e:
            p.nextip()
            logging.debug(str(e))
    soup = BeautifulSoup(r.content, 'html.parser')
    for menubox in soup.select('.menu_box'):
        jobtype = menubox.h2.text.strip()
        joblist = list()
        if jobtype != '':
            for a in menubox.find_all('a'):
                jobname = a.text.strip().replace('/', '')
                joblist.append(jobname)
            configmap[jobtype] = joblist
    logging.info('configmap.size:' + str(len(configmap.keys())))
    print 'configmap.size:' + str(len(configmap.keys()))
    output = open(jobnamesfile, 'w')
    pickle.dump(configmap, output)
    output.close()
    return configmap


# 根据职位名爬取招聘信息
def scrapy(jobname):
    # print 'crawling ' + jobname + '.....'
    p = Proxy()
    db = MySQLdb.connect(dbadd, user, password, database,
                         use_unicode=True, charset="utf8")
    cursor = db.cursor()
    req_url = 'http://www.lagou.com/jobs/positionAjax.json?'
    headers = {'content-type': 'application/json;charset=UTF-8'}
    while (True):
        proxies = p.getproxies()
        try:
            req = requests.post(req_url, params={'first': 'false', 'pn': 1, 'kd': jobname}, headers=headers, timeout=60,
                                proxies=proxies, allow_redirects=False)
            totalCount = req.json()['content']['positionResult']['totalCount']
            pageSize = req.json()['content']['positionResult']['pageSize']
            maxpagenum = totalCount / pageSize

            break
        except Exception, e:
            p.nextip()
            logging.debug(str(e))
    flag = True
    num = 1
    print jobname + ' contain ' + str(totalCount)
    logging.info('maxpagenum:' + str(maxpagenum))
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
                logging.debug(str(e))
        if num > maxpagenum:
            flag = False
        if response.status_code == 200:
            try:
                job_json = response.json()['content'][
                    'positionResult']['result']
                write_db(job_json, cursor)
                db.commit()
            except Exception, e:
                logging.debug(str(e))
        else:
            print('connect error! url = ' + req_url)
        num += 1
    cursor.close()
    db.close()
    return True


# 多进程爬取json


def crawl_json():
    jobdict = readweb()
    pool = multiprocessing.Pool(processes=4)
    for jobtype in jobdict:
        for jobname in jobdict[jobtype]:
            pool.apply_async(scrapy, (jobname,))
    print 'start crawling.....'
    pool.close()
    pool.join()
    print 'done!'
    # db.close()


# 计算平均工资
def normalize(value):
    if '-' in value:
        values = value.split('-')
        min = int(values[0].split('k')[0]) * 1000
        max = int(values[1].split('k')[0]) * 1000
        result = int((min + max) / 2)
    else:
        result = int(value.split('k')[0]) * 1000

    return result


# 写入数据库
def write_db(job_json, cursor):
    for each_job_info_obj in job_json:
        values = []
        values.append(each_job_info_obj['positionId'])
        values.append(each_job_info_obj['companyId'])
        values.append(each_job_info_obj['createTime'].encode('utf-8'))
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
        except Exception, e:
            if 'Duplicate' not in str(e):
                logging.debug(str(e))
            else:
                cursor.execute(
                    'update lagoujobs set formatCreateTime = %s where positionId = %s',
                    [each_job_info_obj['createTime'].encode('utf-8'), each_job_info_obj['positionId']])
            continue
        values = []
        values.append(each_job_info_obj['companyId'])
        values.append(each_job_info_obj['companyName'].encode('utf-8'))
        values = values + ['', '', '', '', '', '', '', '', '', '']
        try:
            cursor.execute(
                'insert into company values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', values)
        except Exception, e:
            if 'Duplicate' not in str(e):
                logging.debug(str(e))
            continue


# 分析每个职位页面详情,返回职位详细信息


def get_job_info_byid(job_id, p):
    req_url = 'http://www.lagou.com/jobs/' + str(job_id) + '.html'
    str_txt = ''
    while (True):
        try:
            proxies = p.getproxies()
            req = requests.get(req_url, proxies=proxies, timeout=30)
            html = req.content
            if u'亲，你来晚了，该信息已经被删除鸟~' in html:
                return 'delete'
            html_soup = BeautifulSoup(html, 'html.parser')
            job_bt_soup = html_soup.select('.job_bt')
            str_txt = job_bt_soup[0].text
            break
        except Exception, e:
            logging.debug(str(e))
            p.nextip()
    return str_txt


def crawl_job_detail():
    db = MySQLdb.connect(dbadd, user, password, database,
                         use_unicode=True, charset="utf8")
    cursor = db.cursor()
    cursor.execute("select positionId from lagoujobs where description = \"\"")
    i = 0
    fetchallist = []
    pool = multiprocessing.Pool(processes=4)
    for idi in cursor.fetchall():
        fetchallist.append(idi)
        i += 1
        if i % 40 == 0:
            pool.apply_async(get_job_description, (fetchallist,))
            fetchallist = []
    pool.apply_async(get_job_description, (fetchallist,))
    print 'start.....'
    pool.close()
    pool.join()
    cursor.close()
    db.close()
    print 'done!'


# 获取description为空的职位的职位信息
def get_job_description(fetchallist):
    db = MySQLdb.connect(dbadd, user, password, database,
                         use_unicode=True, charset="utf8")
    cursor = db.cursor()
    p = Proxy()
    for idi in fetchallist:
        details = get_job_info_byid(idi[0], p).replace('\n', '*#')
        print idi[0], details[:2]
        values = [details, idi[0]]
        cursor.execute(
            'update lagoujobs set description = %s where positionId = %s', values)
        db.commit()
    cursor.close()
    db.close()


# 根据公司id从公司页面获取公司信息
def get_company_info_byid(company_id, p):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
        'Host': 'www.lagou.com',
        'Upgrade-Insecure-Requests': 1,
        'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4,zh-TW;q=0.2',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Cache-Control': 'max-age=0'}
    proxies = p.getproxies()
    req_url = 'http://www.lagou.com/gongsi/' + str(company_id) + '.html'
    try:
        req = requests.get(req_url, proxies=proxies,
                           timeout=60, headers=headers)
    except Exception, e:
        logging.debug(str(e))
        if 'passport.lagou.com' in str(e) or 'disappear.html' in str(e):
            return ['delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete']
        return
    html = req.content
    html_soup = BeautifulSoup(html, 'html.parser')
    job_bt_soup = html_soup.select('#companyInfoData')
    if job_bt_soup is None:
        return ['delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete']
    try:
        json_obj = json.loads(job_bt_soup[0].text)
    except Exception, e:
        return
    companyUrl = ''
    companyIntroduce = ''
    companyName = ''
    companyShortName = ''
    detailPosition = ''
    industryField = ''
    companySize = ''
    city = ''
    financeStage = ''
    companyProfile = ''
    if 'coreInfo' in json_obj.keys():
        if 'companyUrl' in json_obj['coreInfo'].keys():
            companyUrl = json_obj['coreInfo']['companyUrl']
        if 'companyName' in json_obj['coreInfo'].keys():
            companyName = json_obj['coreInfo']['companyName']
        if 'companyShortName' in json_obj['coreInfo'].keys():
            companyShortName = json_obj['coreInfo']['companyShortName']
        if 'companyIntroduce' in json_obj['coreInfo'].keys():
            companyIntroduce = json_obj['coreInfo']['companyIntroduce']
    if 'location' in json_obj.keys():
        if len(json_obj['location']) > 0:
            if 'detailPosition' in json_obj['location'][0].keys():
                detailPosition = json_obj['location'][0]['detailPosition']
    if 'baseInfo' in json_obj.keys():
        if 'industryField' in json_obj['baseInfo'].keys():
            industryField = json_obj['baseInfo']['industryField']
        if 'companySize' in json_obj['baseInfo'].keys():
            companySize = json_obj['baseInfo']['companySize']
        if 'city' in json_obj['baseInfo'].keys():
            city = json_obj['baseInfo']['city']
        if 'financeStage' in json_obj['baseInfo'].keys():
            financeStage = json_obj['baseInfo']['financeStage']
    if 'introduction' in json_obj.keys():
        if 'companyProfile' in json_obj['introduction'].keys():
            companyProfile = json_obj['introduction']['companyProfile']
    values = [companyUrl, companyIntroduce, companyName, companyShortName, detailPosition, industryField, companySize,
              city, financeStage, companyProfile]
    return values


# 获取公司信息不全的公司id并更新
def get_company_description(fetchallist):
    db = MySQLdb.connect(dbadd, user, password, database,
                         use_unicode=True, charset="utf8")
    cursor = db.cursor()
    p = Proxy()
    for id in fetchallist:
        while (True):
            try:
                values = get_company_info_byid(id[0], p)
                values.append(id[0])
                cursor.execute(
                    'update company set companyUrl = %s,description = %s,fullName = %s,shortName = %s,detailPosition = %s,industryField = %s,companySize = %s,city = %s,financeStage = %s,profile = %s where companyId = %s',
                    values)
                db.commit()
                print u"update：", id[0]
                break
            except Exception, e:
                logging.debug(str(e))
                p.nextip()
    cursor.close()
    db.close()


def crawl_company_detail():
    db = MySQLdb.connect(dbadd, user, password, database,
                         use_unicode=True, charset="utf8")
    cursor = db.cursor()
    cursor.execute("select companyId,companyName from company where companyUrl = \"\" and fullName = \"\"")
    i = 0
    fetchallist = []
    pool = multiprocessing.Pool(processes=4)
    for idi in cursor.fetchall():
        fetchallist.append(idi)
        i += 1
        if i % 100 == 0:
            pool.apply_async(get_company_description, (fetchallist,))
            fetchallist = []
    pool.apply_async(get_company_description, (fetchallist,))
    print 'start.....'
    pool.close()
    pool.join()
    cursor.close()
    db.close()
    print 'done!'


if __name__ == '__main__':
    # crawl_json()
    crawl_job_detail()
    #crawl_company_detail()
