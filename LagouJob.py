# -*- coding: utf-8 -*-
import json,requests,pyodbc,re
from bs4 import BeautifulSoup

url1="http://www.lagou.com/gongsi/184-0-0.json"

headers = { "Accept":"text/html,application/xhtml+xml,application/xml;",
            "Accept-Encoding":"gzip",
            "Accept-Language":"zh-CN,zh;q=0.8",
            "Referer":"http://www.example.com/",
            "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36"
            }
def work(n):
    payload= {'first': 'true', 'pn': n,'sortField':'0','havemark':'0'}
    r = requests.get(url, params=payload)
    jsonstr=r.text
    jsonobj=json.loads(jsonstr)
    keys=jsonobj['result'][0].keys()
    keysstr=','.join(keys)
    keysstr=keysstr.rstrip(',')
    cnxn = pyodbc.connect('driver={SQL Server};server=192.168.1.208;database=zhangyilongtest;uid=sa;pwd=windmagic')
    cursor=cnxn.cursor()
    for company in jsonobj['result']:
        values=[]
        for  key in keys:
            if isinstance(company[key], int) or isinstance(company[key], float):
                company[key]=str(company[key])
            elif isinstance(company[key], list):
                company[key]="NULL"
            values.append(company[key])
        valuestr="','".join(values)
        valuestr=valuestr.rstrip(',')
        valuestr="'"+valuestr+"'"
        sql='insert into lagoujob('+keysstr+') values ('+valuestr+')'
        try:
            cursor.execute(sql)
            cnxn.commit()
            print company['companyId'],'success'
        except Exception as e:
            print company['companyId']
    cnxn.close()
        

##for n in range(1,46):
##    work(n)

def work2(n):
    print n
    url="http://www.lagou.com/gongsi/"+str(n)+".html"
    r = requests.get(url)
    html_doc=r.text.encode('utf8')
    ##soup = BeautifulSoup(html_doc,"html.parser",from_encoding="utf8")
    result=re.findall('companyInfo = (.*);',html_doc);
    if len(result)!=0:
        companyInfo=list(result)[0]
        jsonobj=json.loads(companyInfo)
        c0=['lastLoginTime','resumeProcessTime','positionNum','experienceCount','resumeProcessRate']+\
        ['companyShortName', 'isFirst',  'companyName',  'companyIntroduce','companyUrl' ,'approve']+\
        ['companySize','industryField','finaceStage','city','companyLabels','companyProfile','companyId']
        dataInfo=[jsonobj['dataInfo']['lastLoginTime'],jsonobj['dataInfo']['resumeProcessTime'],jsonobj['dataInfo']['positionCount'],jsonobj['dataInfo']['experienceCount'],jsonobj['dataInfo']['resumeProcessRate']]
        coreInfo=[jsonobj['coreInfo']['companyShortName'],jsonobj['coreInfo']['isFirst'],jsonobj['coreInfo']['companyName'],jsonobj['coreInfo']['companyIntroduce'],jsonobj['coreInfo']['companyUrl'],jsonobj['coreInfo']['approve']]
        baseInfo=[jsonobj['baseInfo']['companySize'],jsonobj['baseInfo']['industryField'],jsonobj['baseInfo']['financeStage'],jsonobj['baseInfo']['city']]
        labels=','.join(jsonobj['labels'])
        companyProfile=jsonobj['introduction'].has_key('companyProfile') and jsonobj['introduction']['companyProfile'].replace("'","''") or '';
        companyId=jsonobj['companyId']
        v0=dataInfo+coreInfo+baseInfo
        v0.append(labels)
        v0.append(companyProfile)
        v0.append(companyId)
        c0str=','.join(c0)
        v0str="','".join([isinstance(x, int)and str(x) or x for x in v0])
        v0str="'"+v0str.rstrip(',')+"'"
        sql='insert into lagoujob('+c0str+') values ('+v0str+')';
        try:
            cursor.execute(sql)
            cnxn.commit()
        except Exception as e:
            print str(companyId),' fail'
            f=open('log2.txt','ab')
            print>>f, str(companyId)
            f.close()
        for x in jsonobj['location']:
            companyId=str(x['companyId'])
            longitude= x.has_key('longitude') and   x['longitude'] or ''
            latitude= x.has_key('latitude') and   x['latitude'] or ''
            briefPosition= x.has_key('briefPosition') and   x['briefPosition'] or ''
            c1str='[companyId_locid],[companyId],[updateTime],[briefPosition],[longitude],[createTime],[latitude],[isdel],[detailPosition],[locid]'
            v1=[companyId+'_'+str(x['id']),companyId,x['updateTime'],briefPosition,longitude,x['createTime'],latitude,x['isdel'],x['detailPosition'],x['id']]
            v1str="','".join([(isinstance(x, int) or isinstance(x, float)) and str(x) or x for x in v1])
            v1str="'"+v1str.rstrip(',')+"'"            
            sql='insert into address('+c1str+') values ('+v1str+')';
             
            try:
                cursor.execute(sql)
                cnxn.commit() 
            except Exception as e:
                print str(companyId),' loc fail'
                f=open('loc.txt','ab')
                print>>f, str(companyId)
                f.close()
                
cnxn = pyodbc.connect('driver={SQL Server};server=192.168.1.208;database=db;uid=sa;pwd=pwd')
cursor=cnxn.cursor()
##f=open("loc.txt","rb")
##for x in f.readlines():
##    work2(x)
for n in range(105851,107418):
    work2(n)
cnxn.close()
