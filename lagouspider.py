#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import proxies
import requests
import sys
import json
import MySQLdb
import logging
from bs4 import BeautifulSoup
import time

reload(sys)
sys.setdefaultencoding('utf-8')
date = time.strftime('%Y-%m-%d', time.localtime())
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s line:%(lineno)d [%(levelname)s]:%(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='logs/' + date + '.log',
                    filemode='a')
dbadd = 'localhost'
user = 'root'
password = '123123'
database = 'jobs'


# 读取职位列表
def readweb(p):
    print '获取职位类别列表.....'
    configmap = {}
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
    return configmap


# 根据职位名爬取招聘信息
def scrapy(jobname, p):
    req_url = 'http://www.lagou.com/jobs/positionAjax.json?'
    headers = {'content-type': 'application/json;charset=UTF-8'}
    while (True):
        proxies = p.getproxies()
        try:
            req = requests.post(req_url, params={'first': 'false', 'pn': 1, 'kd': jobname}, headers=headers, timeout=60,
                                proxies=proxies, allow_redirects=False)

            maxpagenum = req.json()['content']['positionResult']['totalCount'] / \
                         req.json()['content']['positionResult']['pageSize']
            break
        except Exception, e:
            p.nextip()
            logging.debug(str(e))
    flag = True
    num = 1
    print '包含 '+str(req.json()['content']['positionResult']['totalCount'])+' 条招聘信息'
    logging.info('maxpagenum:' + str(maxpagenum))
    while flag:
        payload = {'first': 'false', 'pn': num, 'kd': jobname}
        if num %100==0:
            print '已爬取 '+str(num)+' 条'
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
                job_json = response.json()['content']['positionResult']['result']
                write_db(job_json)
            except Exception, e:
                logging.debug(str(e))
        else:
            print('connect error! url = ' + req_url)
        num += 1
    return True

def crawl_json():
    p = proxies.Proxy()
    jobdict = readweb(p)
    for jobtype in jobdict:
        for jobname in jobdict[jobtype]:
            print 'crawling',jobtype,jobname,'.....'
            scrapy(jobname,p)


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
def write_db(job_json):
    db = MySQLdb.connect(dbadd, user, password, database, use_unicode=True, charset="utf8")
    cursor = db.cursor()
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
            db.commit()
        except Exception, e:
            if 'Duplicate' not in str(e):
                logging.debug(str(e))
            else:
                cursor.execute(
                    'update lagoujobs set formatCreateTime = %s where positionId = %s',
                    [each_job_info_obj['createTime'].encode('utf-8'), each_job_info_obj['positionId']])
                db.commit()
            continue
        values = []
        values.append(each_job_info_obj['companyId'])
        values.append(each_job_info_obj['companyName'].encode('utf-8'))
        values = values + ['', '', '', '', '', '', '', '', '', '']
        try:
            cursor.execute(
                'insert into company values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', values)
            db.commit()
        except Exception, e:
            if 'Duplicate' not in str(e):
                logging.debug(str(e))
            continue
    cursor.close()
    db.close()


# 分析每个职位页面详情,返回职位详细信息
def get_job_info_byid(job_id, p):
    req_url = 'http://www.lagou.com/jobs/' + str(job_id) + '.html'
    str_txt = ''
    while (True):
        try:
            proxies = p.getProxyies()
            req = requests.get(req_url, proxies=proxies, timeout=30)
            html = req.content
            if u'亲，你来晚了，该信息已经被删除鸟~' in html:
                return 'delete'
            html_soup = BeautifulSoup(html, 'html.parser')
            job_bt_soup = html_soup.select('.job_bt')
            str_txt = job_bt_soup[0].text
            break
        except Exception, e:
            print e
            p.nextip()
    return str_txt


# 获取description为空的职位的职位信息
def get_job_description():
    db = MySQLdb.connect(dbadd, user, password, database, use_unicode=True, charset="utf8")
    cursor = db.cursor()
    cursor.execute("select positionId from lagoujobs where description = \"\"")
    p = proxies.Proxy()
    f = open('finished.txt')
    finishlist = [j.strip() for j in f.readlines()]
    f.close()
    fi = open('finished.txt', 'a')
    for idi in cursor.fetchall():
        if str(idi[0]) in finishlist:
            continue
        details = get_job_info_byid(idi[0], p).replace('\n', '*#')
        print idi[0], details[:2]
        values = [details, idi[0]]
        cursor.execute('update lagoujobs set description = %s where positionId = %s', values)
        db.commit()
        fi.write(str(idi[0]) + '\n')
    fi.close()
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
        req = requests.get(req_url, proxies=proxies, timeout=60, headers=headers)
    except Exception, e:
        logging.debug(str(e))
        if 'passport.lagou.com' in str(e) or 'disappear.html' in str(e):
            return ['delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete']
        return
    #if req.status_code != 200:
    #    return ['delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete', 'delete']
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
def get_company_description():
    db = MySQLdb.connect(dbadd, user, password, database, use_unicode=True, charset="utf8")
    cursor = db.cursor()
    cursor.execute("select companyId,companyName from company where companyUrl = \"\" and fullName = \"\"")
    p = proxies.Proxy()
    for id in cursor.fetchall():
        while (True):
            try:
                values = get_company_info_byid(id[0], p)
                values.append("更新公司信息："+str(id[0]))
                cursor.execute(
                    'update company set companyUrl = %s,description = %s,fullName = %s,shortName = %s,detailPosition = %s,industryField = %s,companySize = %s,city = %s,financeStage = %s,profile = %s where companyId = %s',
                    values)
                db.commit()
                print id[0]
                break
            except Exception, e:
                logging.debug(str(e))
                p.nextip()
    cursor.close()
    db.close()


if __name__ == '__main__':
    crawl_json()
    #get_company_description()

