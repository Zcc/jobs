#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import MySQLdb
import sys
from proxies import Proxy
import time
import logging
import multiprocessing

reload(sys)
sys.setdefaultencoding('utf-8')
date = time.strftime('%Y-%m-%d-%X', time.localtime()).replace(':', '-')
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s line:%(lineno)d [%(levelname)s]:%(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='logs/' + date + '.log',
                    filemode='a')
dbadd = 'localhost'
user = 'root'
password = '123123'
database = 'jobs'


def createTables():
    db = MySQLdb.connect(dbadd, user, password, database)
    cursor = db.cursor()

    sql = "DROP TABLE IF EXISTS `zhilianjobs`;"
    cursor.execute(sql)
    db.commit()
    sql = """
		CREATE TABLE `zhilianjobs` (
	  `positionId` VARCHAR(255) NOT NULL,
	  `companyId` VARCHAR(255) NOT NULL,
	  `formatCreateTime` VARCHAR(255) NOT NULL,
	  `workYear` VARCHAR(255) NOT NULL,
	  `positionName` VARCHAR(255) NOT NULL,
	  `positionType` VARCHAR(255) NOT NULL,
	  `companyName` VARCHAR(255) NOT NULL,
	  `city` VARCHAR(255) NOT NULL,
	  `education` VARCHAR(255) NOT NULL,
	  `industryField` VARCHAR(255) NOT NULL,
	  `financeStage` VARCHAR(255) NOT NULL,
	  `companysize` VARCHAR(255) NOT NULL,
	  `salary` VARCHAR(255) NOT NULL,
	  `averagesalary` int(10) NOT NULL,
	  `description` VARCHAR(2000) NOT NULL,
	  PRIMARY KEY (`positionId`)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    cursor.execute(sql)
    db.commit()

    sql = "DROP TABLE IF EXISTS `zhiliancompany`;"
    cursor.execute(sql)
    db.commit()
    sql = """
			CREATE TABLE `zhiliancompany` (
		  `companyId` VARCHAR(255) NOT NULL,
		  `companyName` VARCHAR(255) NOT NULL,
		  `companyURL` VARCHAR(255) NOT NULL,
		  `industryField` VARCHAR(255) NOT NULL,
		  `companysize` VARCHAR(255) NOT NULL,
		   `detailPosition` VARCHAR(255) NOT NULL,
		  `description` VARCHAR(2000) NOT NULL,
		  PRIMARY KEY (`companyId`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    cursor.execute(sql)
    db.commit()
    cursor.close()
    db.close()
    return


def joblists():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36'}
    r = requests.get('http://sou.zhaopin.com/', headers=headers)
    html_soup = BeautifulSoup(r.content, 'html.parser')
    job_bt_soup = html_soup.select('.search_bottom_content')
    jobdict = {}
    for clearfixed in job_bt_soup[0].select('.clearfixed'):
        lists = []
        for job in clearfixed.h1.find_all("a"):
            lists.append((job.text, job['href']))
        jobdict[(clearfixed.p.text, clearfixed.p.a['href'])] = lists
    return jobdict


def crawl_byid(jobid):
    p = Proxy()
    db = MySQLdb.connect(dbadd, user, password, database)
    db.set_character_set('utf8')
    cursor = db.cursor()
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36'}
    host = 'http://sou.zhaopin.com'
    for i in xrange(10):
        url = host + jobid + u'&jl=全国&sb=2&p=' + str(i)
        while (True):
            try:
                ipadd = p.getip()
                proxies = {'http': ipadd, 'https': ipadd}
                r = requests.get(url, headers=headers,
                                 proxies=proxies, timeout=60)
                html_soup = BeautifulSoup(r.content, 'html.parser')
                job_bt_soup = html_soup.select(
                    '.newlist_list_content')[0]
                break
            except Exception, e:
                logging.debug(str(e))
                p.nextip()
        for table in job_bt_soup.find_all('table')[1:]:
            if len(table.select('.zwmc')) <= 0:
                continue
            zwmc = table.select('.zwmc')[0]
            joburl = zwmc.a['href']
            jobname = zwmc.a.text
            companyurl = ''
            companyname = ''
            salary = ''
            location = ''
            time = ''
            detail = ''
            jobdetail = ''
            companypolarity = ''
            companyscale = ''
            if len(table.select('.gsmc')) > 0:
                gsmc = table.select('.gsmc')[0]
                companyurl = gsmc.a['href']
                companyname = gsmc.a.text
            if len(table.select('.zwyx')) > 0:
                salary = table.select('.zwyx')[0].text
            if len(table.select('.gzdd')) > 0:
                location = table.select('.gzdd')[0].text
            if len(table.select('.gxsj')) > 0:
                time = table.select('.gxsj')[0].span.text
            if len(table.select('.newlist_deatil_last')) > 0:
                detail = table.select('.newlist_deatil_last')[0].text
            if len(table.select('.newlist_deatil_two')) > 0:
                jobdetail = table.select('.newlist_deatil_two')[
                    0].find_all('span')
                companypolarity = jobdetail[1].text[5:]
                companyscale = jobdetail[2].text[5:]
            education = ''
            experience = ''
            if len(jobdetail) > 3:
                if '经验' in jobdetail[3].encode('UTF-8'):
                    experience = jobdetail[3].text[3:]
                if '学历' in jobdetail[3].encode('UTF-8'):
                    education = jobdetail[3].text[3:]
                if len(jobdetail) > 4:
                    if '经验' in jobdetail[4].encode('UTF-8'):
                        experience = jobdetail[4].text[3:]
                    if '学历' in jobdetail[4].encode('UTF-8'):
                        education = jobdetail[4].text[3:]
            values = [joburl, companyurl, time, experience, jobname, job[
                0], companyname, location, education, jobtype[0], companypolarity, companyscale, salary, 0, detail]
            # print values
            try:
                cursor.execute(
                    'insert into zhilianjobs values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', values)
                db.commit()
            except Exception, e:
                if 'Duplicate' not in e[1]:
                    logging.debug(str(e))
            values = [companyurl, companyname,
                      '', '', companyscale, '', '']
            # print values
            try:
                cursor.execute(
                    'insert into zhiliancompany values (%s,%s,%s,%s,%s,%s,%s)', values)
                db.commit()
            except Exception, e:
                if 'Duplicate' not in e[1]:
                    logging.debug(str(e))


def spider():
    db = MySQLdb.connect(dbadd, user, password, database)
    db.set_character_set('utf8')
    cursor = db.cursor()
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36'}
    host = 'http://sou.zhaopin.com'
    lists = joblists()
    p = Proxy()
    tot = 0
    f = open('finished.txt')
    finished = [j.strip().decode("utf-8") for j in f.readlines()]
    f.close()
    finished[0] = finished[0][1:]
    for jobtype in lists.keys():
        print jobtype[0]
        for job in lists[jobtype]:
            tot += 1
            print job[0], tot
            if jobtype[0] + job[0] in finished:
                continue
            for i in xrange(10):
                url = host + job[1] + u'&jl=全国&sb=2&p=' + str(i)
                while (True):
                    try:
                        ipadd = p.getip()
                        proxies = {'http': ipadd, 'https': ipadd}
                        r = requests.get(url, headers=headers,
                                         proxies=proxies, timeout=60)
                        html_soup = BeautifulSoup(r.content, 'html.parser')
                        job_bt_soup = html_soup.select(
                            '.newlist_list_content')[0]
                        break
                    except Exception, e:
                        logging.debug(str(e))
                        p.nextip()
                for table in job_bt_soup.find_all('table')[1:]:
                    if len(table.select('.zwmc')) <= 0:
                        continue
                    zwmc = table.select('.zwmc')[0]
                    joburl = zwmc.a['href']
                    jobname = zwmc.a.text
                    companyurl = ''
                    companyname = ''
                    salary = ''
                    location = ''
                    time = ''
                    detail = ''
                    jobdetail = ''
                    companypolarity = ''
                    companyscale = ''
                    if len(table.select('.gsmc')) > 0:
                        gsmc = table.select('.gsmc')[0]
                        companyurl = gsmc.a['href']
                        companyname = gsmc.a.text
                    if len(table.select('.zwyx')) > 0:
                        salary = table.select('.zwyx')[0].text
                    if len(table.select('.gzdd')) > 0:
                        location = table.select('.gzdd')[0].text
                    if len(table.select('.gxsj')) > 0:
                        time = table.select('.gxsj')[0].span.text
                    if len(table.select('.newlist_deatil_last')) > 0:
                        detail = table.select('.newlist_deatil_last')[0].text
                    if len(table.select('.newlist_deatil_two')) > 0:
                        jobdetail = table.select('.newlist_deatil_two')[
                            0].find_all('span')
                        companypolarity = jobdetail[1].text[5:]
                        companyscale = jobdetail[2].text[5:]
                    education = ''
                    experience = ''
                    if len(jobdetail) > 3:
                        if '经验' in jobdetail[3].encode('UTF-8'):
                            experience = jobdetail[3].text[3:]
                        if '学历' in jobdetail[3].encode('UTF-8'):
                            education = jobdetail[3].text[3:]
                        if len(jobdetail) > 4:
                            if '经验' in jobdetail[4].encode('UTF-8'):
                                experience = jobdetail[4].text[3:]
                            if '学历' in jobdetail[4].encode('UTF-8'):
                                education = jobdetail[4].text[3:]
                    values = [joburl, companyurl, time, experience, jobname, job[
                        0], companyname, location, education, jobtype[0], companypolarity, companyscale, salary, 0,
                              detail]
                    # print values
                    try:
                        cursor.execute(
                            'insert into zhilianjobs values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', values)
                        db.commit()
                    except Exception, e:
                        if 'Duplicate' not in e[1]:
                            logging.debug(str(e))
                    values = [companyurl, companyname,
                              '', '', companyscale, '', '']
                    # print values
                    try:
                        cursor.execute(
                            'insert into zhiliancompany values (%s,%s,%s,%s,%s,%s,%s)', values)
                        db.commit()
                    except Exception, e:
                        if 'Duplicate' not in e[1]:
                            logging.debug(str(e))
            finishedf = open('finished.txt', 'a')
            finishedf.write(jobtype[0] + job[0] + '\n')
    cursor.close()
    db.close()


def avasalary(salary):
    if '-' in salary:
        bounds = salary.split('-')
        return (int(bounds[0]) + int(bounds[1])) / 2
    return 0


def getdetailbyid(positionid, p):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36'}
    while (True):
        try:
            ipadd = p.getip()
            proxies = {'http': ipadd, 'https': ipadd}
            r = requests.get(positionid, headers=headers,
                             timeout=60, proxies=proxies)
            break
        except Exception, e:
            logging.debug(str(e))
            p.nextip()
    html_soup = BeautifulSoup(r.content, 'html.parser')
    welfare_tab = html_soup.select('.welfare-tab-box')
    if len(welfare_tab) == 0:
        return []
    terminalpage_left = html_soup.select('.terminalpage-left')[0]
    detalili = terminalpage_left.find_all('li')
    inner_box = terminalpage_left.select('.tab-inner-cont')
    company_box = html_soup.select('.company-box')
    if len(company_box) == 0:
        return []
    labels = ''
    if len(welfare_tab) > 0:
        labels = '|'.join([l.text for l in welfare_tab[0].find_all('span')])
    polarity = ''
    workyear = ''
    number = ''
    jobdetails = ''
    companydetails = ''

    if len(detalili) >= 8:
        jproperty = detalili[3].text[5:]
        workyear = detalili[4].text[5:]
        number = detalili[6].text[5:]
    if len(inner_box) >= 2:
        jobdetails = inner_box[0].text.strip().replace(
            '\n', '*#').replace('  ', '')
        companydetails = inner_box[1].text.strip()

    detalili = company_box[0].find_all('li')
    companyURL = ''
    industryField = ''
    detailPosition = ''
    financeStage = ''
    companysize = ''
    companyName = ''
    companyId = ''
    for de in detalili:
        if u'规模' in de.span.text:
            companysize = de.strong.text
        if u'性质' in de.span.text:
            financeStage = de.strong.text
        if u'行业' in de.span.text:
            industryField = de.strong.text
        if u'地址' in de.span.text:
            detailPosition = de.strong.text.strip()
        if u'主页' in de.span.text:
            companyURL = de.strong.text
    companyinfo = company_box[0].select('.company-name-t')
    if len(companyinfo) > 0:
        companyName = companyinfo[0].a.text
        companyId = companyinfo[0].a['href']
    values = [companyId, labels, jproperty, workyear, number, jobdetails[:4000], companyId, companyName, companyURL,
              industryField, companysize, detailPosition, companydetails[:4000], financeStage]
    return values


def getdetail(fetchallist):
    db = MySQLdb.connect(dbadd, user, password, database)
    db.set_character_set('utf8')
    cursor = db.cursor()
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')
    p = Proxy()
    for i in fetchallist:
        if 'redirecturl' in i[0]:
            continue
        values = getdetailbyid(i[0], p)
        if len(values) == 0:

            continue
        val = values[:6]
        val.append(i[0])
        try:
            cursor.execute(
                'update zhilianjobs set companyId = %s,labels = %s,property = %s,workYear = %s,number = %s,detaildescription = %s where positionId = %s',
                val)
            db.commit()
        except Exception, e:
            logging.debug(str(e))
        val = values[6:]
        try:
            cursor.execute(
                'insert into zhiliancompany values (%s,%s,%s,%s,%s,%s,%s,%s)', val)
            db.commit()
        except Exception, e:
            try:
                cursor.execute(
                    'update zhiliancompany set companyURL = %s,industryField = %s, companysize = %s,detailPosition = %s,description = %s,financeStage = %s where companyId = %s',
                    val[2:] + val[:1])
                db.commit()
            except Exception, e:
                logging.debug(str(e))
        print i[0][25:]

    cursor.close()
    db.close()


def crawl_job_detail():
    db = MySQLdb.connect(dbadd, user, password, database,
                         use_unicode=True, charset="utf8")
    cursor = db.cursor()
    cursor.execute("select positionId,companyId from zhilianjobs WHERE number = \"\"")
    i = 0
    fetchallist = []
    pool = multiprocessing.Pool(processes=4)
    for idi in cursor.fetchall():
        fetchallist.append(idi)
        i += 1
        #if i == 400:
        #    break
        if i % 1000 == 0:
            pool.apply_async(getdetail, (fetchallist,))
            fetchallist = []
    print 'start.....'
    pool.close()
    pool.join()
    cursor.close()
    db.close()
    print 'done!'


if __name__ == '__main__':
    # createTables()
    # spider()
    # getdetail()
    crawl_job_detail()
    # db = MySQLdb.connect(dbadd, user, password, database,
    #                      use_unicode=True, charset="utf8")
    # cursor = db.cursor()
    # p = Proxy()
    # values = getdetailbyid('http://jobs.zhaopin.com/000001367251918.htm',p)
    # val = values[:6]
    # val.append('http://jobs.zhaopin.com/000001367251918.htm')
    # try:
    #     cursor.execute(
    #         'update zhilianjobs set companyId = %s,labels = %s,property = %s,workYear = %s,number = %s,detaildescription = %s where positionId = %s',
    #         val)
    #     db.commit()
    # except Exception, e:
    #     logging.debug(str(e))
    # cursor.close()
    # db.close()