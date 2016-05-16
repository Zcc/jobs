import requests
from bs4 import BeautifulSoup
import os

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36',
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection':'keep-alive',
        'Cookie':'_gat=1; Hm_lvt_7ed65b1cc4b810e9fd37959c9bb51b31=1462434057,1462434982,1462437840; Hm_lpvt_7ed65b1cc4b810e9fd37959c9bb51b31=1462438111; _ga=GA1.2.128553310.1462434056',
        'Host':'www.kuaidaili.com',
        'Upgrade-Insecure-Requests':1,
        'Accept-Language':'zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4,zh-TW;q=0.2',
        'Accept-Encoding':'gzip, deflate, sdch',
        'Cache-Control':'max-age=0'}

class Proxy:
    def __init__(self):
        if not os.path.isfile('proxies'):
            self.getwebip()
        self.proxies = [ip.strip() for ip in open('proxies').readlines()]
        self.tot = 0

    def getwebip(self):
        proxiesfile = open('proxies','w')
        for i in xrange(5):
            #r = requests.get('http://www.mimiip.com/gngao/'+str(i))
            r = requests.get('http://ip84.com/gn/' + str(i))
            #r = requests.get('http://www.xicidaili.com/nn/' + str(i),headers=headers)
            #r = requests.get('http://www.kuaidaili.com/free/inha/' + str(i)+'/',headers=headers)
            soup = BeautifulSoup(r.content, 'html.parser')
            for tr in soup.find_all('tr'):
                if tr!= None:
                    trlist = tr.find_all('td')
                    if len(trlist)>2:
                        ip = 'http://'+trlist[0].text+":"+trlist[1].text
                        proxiesfile.write(ip+'\n')
        proxiesfile.close()

    def getProxyies(self):
        return self.proxies

    def nextip(self):
        self.tot += 1
        if self.tot > len(self.proxies):
            self.getwebip()
            self.__init__()
            self.tot=0

    def getip(self):
        return self.proxies[self.tot%len(self.proxies)]

    def getproxies(self):
        ip = self.getip()
        return {'http': ip, 'https': ip}

if __name__ == '__main__':
    p = Proxy()
    print p.getip()
    p.nextip()
    print p.getproxies()