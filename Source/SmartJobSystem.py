# 项目名称：智慧JOB大数据平台
# 开发人员：lgc
# 开发时间：2022/1/13 - 2022/1/16

import os
import random
import time
from enum import Enum

import MySQLdb
import pandas
import pymongo
import requests
from bs4 import BeautifulSoup
from lxml import etree


class Website:
    """
    获取网站url 和 虚拟user-agent
    """
    def __init__(self):
        self.__url_list = {
            'zhiyou': 'https://www.jobui.com',
            'xjh': 'https://www.nowcoder.com/careertalk?q='
        }
        self.__user_agent_list = [
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
            "Opera/8.0 (Windows NT 5.1; U; en)",
            "Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50",
            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; en) Opera 9.50",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0",
            "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; SE 2.X MetaSr 1.0)",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Maxthon/4.4.3.4000 Chrome/30.0.1599.101 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36",
            "Mozilla/5.0 (Windows; U; Windows NT 5.2) AppleWebKit/525.13 (KHTML, like Gecko) Chrome/0.2.149.27 Safari/525.13",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
            "Mozilla/5.0 (Macintosh; U; IntelMac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1Safari/534.50",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:51.0) Gecko/20100101 Firefox/51.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
            "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
        ]

    # 随机获取一个user-agent作为headers
    def get_headers(self):
        return {'User-Agent': random.choice(self.__user_agent_list)}

    # 获取职友集url
    def get_zhiyou(self):
        return self.__url_list['zhiyou']

    # 获取宣讲会url
    def get_xjh(self):
        return self.__url_list['xjh']


class Job:
    """
    工作
    """
    def __init__(self):
        self.title = ''
        self.experience = ''
        self.education = ''
        self.salary = ''
        self.company = ''
        self.page_views = ''
        self.industry = ''
        self.company_size = ''
        self.city = ''
        self.responsibilities = ''
        self.source_website = ''
        self.update_time = ''
        self.logo_information = ''

    # 简单规格化爬虫信息 info 信息字符列表
    def format(self, info):
        self.title = info[0].strip()
        self.experience = info[1].strip()
        self.education = info[2].strip()
        self.salary = info[3].strip()
        self.company = info[4].strip()
        self.page_views = info[5].strip()
        self.industry = info[6].strip()
        self.company_size = info[7].strip()
        self.city = info[8].strip()
        self.responsibilities = info[9].strip()
        self.source_website = info[10].strip()
        self.update_time = info[11].strip()
        self.logo_information = info[12].strip()

    # 返回一行以逗号分割的字符串(7个主要信息)
    def get_main_str(self):
        s = r''
        s += str(self.title) + ','
        s += str(self.experience) + ','
        s += str(self.education) + ','
        s += str(self.salary) + ','
        s += str(self.company) + ','
        s += str(self.page_views) + ','
        s += str(self.city) + '\n'
        return s


class MyFile:
    """
    文件操作
    """
    def __init__(self):
        self.job_col = ['岗位名称', '经验', '学历', '薪资', '公司', '浏览量', '行业',
                        '公司规模', '城市', '岗位职责', '来源网站', '更新时间', 'logo信息']
        self.job_content = {'岗位名称': [], '经验': [], '学历': [], '薪资': [], '公司': [], '浏览量': [], '行业': [],
                            '公司规模': [], '城市': [], '岗位职责': [], '来源网站': [], '更新时间': [], 'logo信息': []}
        self.company_col = ['公司名称', '浏览量', '关注', '主旨', '性质', '规模', '成立', '行业', '全称', '简介']
        self.company_content = {'公司名称': [], '浏览量': [], '关注': [], '主旨': [],
                                '性质': [], '规模': [], '成立': [], '行业': [], '全称': [], '简介': []}
        self.seminar_col = ['标题', '高校', '时间', '位置']
        self.seminar_content = {'标题': [], '高校': [], '时间': [], '位置': []}

    # 添加一个job信息到txt
    def save_job_txt(self, filepath, job):
        content = [job.get_main_str()]
        MyTxt().append_file(filepath, content)

    # 从txt中读取jobs信息
    def load_jobs_txt(self, filepath):
        info_list = MyTxt().read_file(filepath)
        return info_list

    # 一次写入jobs信息到txt
    def save_jobs_txt(self, filepath, jobs):
        content = []
        for job in jobs:
            content.append(job.get_main_str())
        MyTxt().write_file(filepath, content)

    # 从excel中读取jobs信息
    def load_jobs_excel(self, filepath):
        jobs = []
        df = MyExcel().read_excel(filepath)
        for item in df.values:
            job = Job()
            job.title = item[0]
            job.experience = item[1]
            job.education = item[2]
            job.salary = item[3]
            job.company = item[4]
            job.page_views = item[5]
            job.industry = item[6]
            job.company_size = item[7]
            job.city = item[8]
            job.responsibilities = item[9]
            job.source_website = item[10]
            job.update_time = item[11]
            job.logo_information = item[12]
            jobs.append(job)
        return jobs

    # 一次写入jobs信息到excel
    def save_jobs_excel(self, filepath, jobs):
        col = self.job_col
        content = self.job_content
        for job in jobs:
            content['岗位名称'].append(job.title)
            content['经验'].append(job.experience)
            content['学历'].append(job.education)
            content['薪资'].append(job.salary)
            content['公司'].append(job.company)
            content['浏览量'].append(job.page_views)
            content['行业'].append(job.industry)
            content['公司规模'].append(job.company_size)
            content['城市'].append(job.city)
            content['岗位职责'].append(job.responsibilities)
            content['来源网站'].append(job.source_website)
            content['更新时间'].append(job.update_time)
            content['logo信息'].append(job.logo_information)
        MyExcel().create_excel(filepath, content, col)

    # 添加一个job信息到excel
    def save_job_excel(self, filepath, job):
        if not os.path.exists(filepath):
            MyExcel().create_excel(filepath, self.job_content, self.job_col)
        content = self.job_content
        content['岗位名称'] = job.title
        content['经验'] = job.experience
        content['学历'] = job.education
        content['薪资'] = job.salary
        content['公司'] = job.company
        content['浏览量'] = job.page_views
        content['行业'] = job.industry
        content['公司规模'] = job.company_size
        content['城市'] = job.city
        content['岗位职责'] = job.responsibilities
        content['来源网站'] = job.source_website
        content['更新时间'] = job.update_time
        content['logo信息'] = job.logo_information
        MyExcel().append_excel(filepath, content)

    # 添加一个company信息到excel
    def save_company_excel(self, filepath, company):
        if not os.path.exists(filepath):
            MyExcel().create_excel(filepath, self.company_content, self.company_col)
        content = self.company_content
        content['公司名称'] = company.name
        content['浏览量'] = company.page_views
        content['关注'] = company.attention
        content['主旨'] = company.subject
        content['性质'] = company.nature
        content['规模'] = company.scale
        content['成立'] = company.establishment
        content['行业'] = company.industry
        content['全称'] = company.full_name
        content['简介'] = company.introduction
        MyExcel().append_excel(filepath, content)

    # 一次写入companies信息到excel
    def save_companies_excel(self, filepath, companies):
        col = self.company_col
        content = self.company_content
        for company in companies:
            content['公司名称'].append(company.name)
            content['浏览量'].append(company.page_views)
            content['关注'].append(company.attention)
            content['主旨'].append(company.subject)
            content['性质'].append(company.nature)
            content['规模'].append(company.scale)
            content['成立'].append(company.establishment)
            content['行业'].append(company.industry)
            content['全称'].append(company.full_name)
            content['简介'].append(company.introduction)
        MyExcel().create_excel(filepath, content, col)

    # 添加一个seminar信息到excel
    def save_seminar_excel(self, filepath, seminar):
        if not os.path.exists(filepath):
            MyExcel().create_excel(filepath, self.seminar_content, self.seminar_col)
        content = self.seminar_content
        content['标题'] = seminar.title
        content['高校'] = seminar.college
        content['时间'] = seminar.time
        content['位置'] = seminar.location
        MyExcel().append_excel(filepath, content)

    # 一次写入seminars信息到excel
    def save_seminars_excel(self, filepath, seminars):
        col = self.seminar_col
        content = self.seminar_content
        for seminar in seminars:
            content['标题'].append(seminar.title)
            content['高校'].append(seminar.college)
            content['时间'].append(seminar.time)
            content['位置'].append(seminar.location)
        MyExcel().create_excel(filepath, content, col)


class MyTxt:
    """
    txt文件处理
    """
    def __init__(self):
        self.file_content = ''

    # 读取文件 filepath 文件路径
    def read_file(self, filepath):
        with open(filepath, 'r', encoding='utf8') as file:
            self.file_content = file.readlines()
        return self.file_content

    # 保存文件 filepath 文件路径 content内容列表
    def write_file(self, filepath, content):
        with open(filepath, 'w', encoding='utf8') as file:
            file.writelines(content)

    # 添加模式 filepath 文件路径 content内容列表
    def append_file(self, filepath, content):
        with open(filepath, 'a', encoding='utf8') as file:
            file.writelines(content)

    # 获取以逗号分割的信息列表
    def get_list_by_comma(self, filepath):
        self.read_file(filepath)
        info = ''
        for item in self.file_content:
            info += ''.join(item).strip()
        return info.replace('，', ',').strip().split(',')


class MyExcel:
    """
    excel表处理
    """
    # 创建excel表
    def create_excel(self, filepath, content, col):
        df = pandas.DataFrame(content, columns=col)
        df.to_excel(filepath, index=False)

    # 读取excel表
    def read_excel(self, filepath):
        df = pandas.read_excel(filepath)
        return df

    # 向Excel表中添加数据
    def append_excel(self, filepath, content):
        df = self.read_excel(filepath)
        df = df.append(content, ignore_index=True)
        df.to_excel(filepath, index=False)

    # 获取Excel某一列数据
    def get_list_by_column(self, filepath, col_idx):
        df = pandas.read_excel(filepath, usecols=[col_idx])
        content = []
        for item in df.values:
            content.append(item[0])
        return content


class Reptile:
    """
    爬虫
    """
    def __init__(self, url='', headers={'User-Agent': ''}, sleep_time=14):
        self.url = url
        self.headers = headers
        self.sleep_time = sleep_time

    # 获取lxml的etree
    def get_lxml_etree(self):
        while True:
            self.headers = Website().get_headers()
            resp = requests.get(self.url, headers=self.headers)
            time.sleep(self.sleep_time)
            if resp.status_code == 200:
                context = resp.text
                return etree.HTML(context)

    # 获取bs4的soup
    def get_bs4_soup(self):
        while True:
            self.headers = Website().get_headers()
            resp = requests.get(self.url, headers=self.headers)
            time.sleep(self.sleep_time)
            if resp.status_code == 200:
                context = resp.text
                return BeautifulSoup(context, 'html.parser')


class XZReptile(Reptile):
    """
    薪资数据 爬虫
    """
    def __init__(self, view_pages=1):
        Reptile.__init__(self)
        # 爬取页面总数
        self.view_pages = view_pages
        self.jobs = []
        self.try_count = 0
        self.error_info = []

    # 获取爬取的url列表
    def get_url_list(self):
        self.url = Website().get_zhiyou()
        city_list = MyTxt().get_list_by_comma(Filepath().get_filepath(Filepath().read_city_txt))
        title_list = MyTxt().get_list_by_comma(Filepath().get_filepath(Filepath().read_title_txt))
        pages_count = self.view_pages
        # www.jobui.com/jobKw=java开发工程师&cityKw=北京&n=1
        url_list = []
        for title in title_list:
            for city in city_list:
                for i in range(pages_count):
                    url = self.url + '/jobs?jobKw={}&cityKw={}&n={}'.format(title, city, i + 1)
                    url_list.append(url)
        return url_list

    # 使用lxml从职友集爬取信息
    def get_info_by_lxml(self):
        url_list = self.get_url_list()
        info_list = []
        for url in url_list:
            lists = []
            flag = 0
            city = [url[url.find('cityKw='):url.find('&n=')].lstrip('cityKw=')]
            while len(lists) == 0 and flag < 5:
                flag += 1
                reptile = Reptile(url)
                my_etree = reptile.get_lxml_etree()
                lists = my_etree.xpath('//div[@class="c-job-list"]')
            for item in lists:
                self.try_count += 1
                # 两个url可能会出错
                try:
                    info = [item.xpath('./div[@class="job-content-box"]/div[@class="job-content"]/div[1]/a/h3'),
                            item.xpath('./div[@class="job-content-box"]/div[@class="job-content"]/div[2]/div/span[1]'),
                            item.xpath('./div[@class="job-content-box"]/div[@class="job-content"]/div[2]/div/span[2]'),
                            item.xpath('./div[@class="job-content-box"]/div[@class="job-content"]/div[2]/div/span[3]'),
                            item.xpath('./div[@class="job-content-box"]/div[@class="job-content"]/div[3]/a'),
                            item.xpath('./div[@class="job-content-box"]/div[@class="job-content"]/div[4]/span')]
                    url1 = self.url + item.xpath('./div[@class="job-content-box"]/div[@class="job-content"]/div[1]/a/@href')[0]
                    self.get_info2_by_lxml(url1, info, city)

                    for i in range(len(info)):
                        if len(info[i]) == 0:
                            info[i] = ''
                        elif isinstance(info[i][0], str):
                            info[i] = info[i][0]
                        else:
                            info[i] = info[i][0].xpath('string(.)')
                    info_list.append(info)

                    job = Job()
                    job.format(info)
                    MyFile().save_job_txt(Filepath().get_filepath(Filepath().write_salary_copy_txt), job)
                    MyFile().save_job_excel(Filepath().get_filepath(Filepath().write_salary_copy_xlsx), job)

                except Exception as e:
                    self.error_info.append(e)
        return info_list

    # 爬取第2个网页的信息
    def get_info2_by_lxml(self, url, info, city):
        lists = []
        flag = 0
        my_etree = None
        while len(lists) == 0 and flag < 5:
            flag += 1
            reptile = Reptile(url)
            my_etree = reptile.get_lxml_etree()
            lists = my_etree.xpath('//div[@id="navTab"]')[0].xpath('./div[@class="cfix banner-nav-box"]/a[1]/@href')
        url1 = self.url + lists[0]
        self.get_info3_by_lxml(url1, info)

        info.append(city)
        info.append(my_etree.xpath('//div[@class="hasVist cfix sbox fs16"]'))
        info.append(my_etree.xpath('//dl[@class="JI-so fs16"]/dd/a'))
        info.append(my_etree.xpath('//span[@class="fs16 gray9"]'))
        info.append(my_etree.xpath('//img[@class="company-banner-logo"]/@src'))

    # 爬取第3个网页的信息
    def get_info3_by_lxml(self, url, info):
        reptile = Reptile(url)
        my_etree = reptile.get_lxml_etree()
        info.append(my_etree.xpath('//div[@class="m-container"]/div[@class="intro"]/div[@class="cfix fs16"]/'
                                   'div[@class="j-edit hasVist dlli mb10"]/div[3]/span'))
        info.append(my_etree.xpath('//div[@class="m-container"]/div[@class="intro"]/div[@class="cfix fs16"]/'
                                   'div[@class="j-edit hasVist dlli mb10"]/div[1]/div[@class="company-worker"]'))

    # 从爬下来的info中获取job列表
    def get_jobs(self):
        info_list = self.get_info_by_lxml()
        for info in info_list:
            job = Job()
            job.format(info)
            self.jobs.append(job)


class Company:
    """
    公司信息
    """
    def __init__(self):
        self.name = ''
        self.page_views = ''
        self.attention = ''
        self.subject = ''
        self.nature = ''
        self.scale = ''
        self.establishment = ''
        self.industry = ''
        self.full_name = ''
        self.introduction = ''

    # 简单规范化爬虫信息
    def format(self, info):
        self.name = info[0].strip()
        self.page_views = info[1].strip()
        self.attention = info[2].strip()
        self.subject = info[3].strip()
        self.nature = info[4].strip()
        self.scale = info[5].strip()
        self.establishment = info[6].strip()
        self.industry = info[7].strip()
        self.full_name = info[8].strip()
        self.introduction = info[9].strip()


class GSReptile(Reptile):
    """
    公司信息数据 爬虫
    """
    def __init__(self):
        Reptile().__init__()
        self.companies = []
        self.try_count = 0
        self.error_info = []

    # 获取爬取的url列表
    def get_url_list(self):
        self.url = Website().get_zhiyou()
        name_list = MyExcel().get_list_by_column(Filepath().get_filepath(Filepath().read_company_xlsx), 1)
        # https://www.jobui.com/cmp?keyword=河南八六三软件股份有限公司
        url_list = []
        for name in name_list:
            url = self.url + '/cmp?keyword={}'.format(name)
            url_list.append(url)
        return url_list

    # 通过bs4爬取信息
    def get_info_by_bs4(self):
        url_list = self.get_url_list()
        info_list = []
        for url in url_list:
            lists = []
            flag = 0
            while len(lists) == 0 and flag < 5:
                flag += 1
                reptile = Reptile(url)
                soup = reptile.get_bs4_soup()
                lists = soup.find_all('div', attrs={'class': 'c-company-list'})
            for item in lists:
                company_name = item.find('a', attrs={'class': 'company-name'})
                if company_name is None:
                    continue
                try:
                    self.try_count += 1
                    info = [company_name,
                            item.find('span', attrs={'class': 'company-desc'}),
                            item.find('span', attrs={'id': 'gradeFollowNum'}),
                            item.find('div', attrs={'class': 'company-short-content company-segmetation'})]
                    url1 = self.url + company_name.get('href')
                    self.get_info2_by_bs4(url1, info)
                    for i in range(len(info)):
                        if info[i] is None:
                            info[i] = ''
                        else:
                            info[i] = info[i].text
                    info_list.append(info)

                    company = Company()
                    company.format(info)
                    MyFile().save_company_excel(Filepath().get_filepath(Filepath().write_company_copy_xlsx), company)
                except Exception as e:
                    self.error_info.append(e)
        return info_list

    # 爬取第2页信息
    def get_info2_by_bs4(self, url, info):
        lists = []
        flag = 0
        while len(lists) == 0 and flag < 5:
            flag += 1
            reptile = Reptile(url)
            soup = reptile.get_bs4_soup()
            lists = soup.find_all('div', attrs={'class': 'm-container'})
        for item in lists:
            info_items = item.find_all('div', attrs={'class': 'company-info-item'})
            if len(info_items) == 0:
                continue
            info.append(item.find('div', attrs={'class': 'company-nature'}))
            info.append(item.find('div', attrs={'class': 'company-worker'}))
            info.append(item.find('span', attrs={'class': 'fs18 fwb'}))
            info.append(item.find('span', attrs={'class': 'comInd'}))
            info.append(info_items[len(info_items) - 1])
            info.append(item.find('p', attrs={'class': 'mb10 cmp-txtshow'}))

    # 爬取company信息
    def get_companies(self):
        info_list = self.get_info_by_bs4()
        for info in info_list:
            company = Company()
            company.format(info)
            self.companies.append(company)


class Seminar:
    """
    宣讲会
    """
    def __init__(self):
        self.title = ''
        self.college = ''
        self.time = ''
        self.location = ''

    # 简单规范化
    def format(self, info):
        self.title = info[0].strip()
        self.college = info[1].strip()
        self.time = info[2].strip()
        self.location = info[3].strip()


class XJHReptile(Reptile):
    def __init__(self, view_pages=1):
        Reptile().__init__()
        self.seminars = []
        # 爬取页面总数
        self.view_pages = view_pages

    # 获取url列表
    def get_url_list(self):
        # 原始 https://www.nowcoder.com/careertalk?q=
        # 翻页 https://www.nowcoder.com/careertalk?page=1
        self.url = Website().get_xjh()
        url_list = []
        for i in range(self.view_pages):
            url = self.url.rstrip('q=') + 'page={}'.format(i + 1)
            url_list.append(url)
        return url_list

    # 通过lxml爬取信息
    def get_info_by_lxml(self):
        url_list = self.get_url_list()
        info_list = []
        for url in url_list:
            lists = []
            flag = 0
            while len(lists) == 0 and flag < 5:
                flag += 1
                reptile = Reptile(url)
                my_tree = reptile.get_lxml_etree()
                lists = my_tree.xpath('//a[@class="career-talk-item js-talk-item"]')

            for item in lists:
                url1 = self.url.rstrip('/careertalk?q=') + item.xpath('./@href')[0]
                info = self.get_info2_by_lxml(url1)
                for i in range(len(info)):
                    if len(info[i]) == 0:
                        info[i] = ''
                    else:
                        info[i] = info[i][0].xpath('string(.)')
                info_list.append(info)

                seminar = Seminar()
                seminar.format(info)
                MyFile().save_seminar_excel(Filepath().get_filepath(Filepath().write_seminar_copy_xlsx), seminar)
        return info_list

    # 获取第2个页面的信息
    def get_info2_by_lxml(self, url):
        info = []
        lists = []
        flag = 0
        while len(lists) == 0 and flag < 5:
            flag += 1
            reptile = Reptile(url)
            my_tree = reptile.get_lxml_etree()
            lists = my_tree.xpath('//div[@class="career-com  js-talk-item"]')
        for item in lists:
            info.append(item.xpath('./div[@class="career-com-hd"]/h2'))
            info.append(item.xpath('./div[@class="career-com-bd"]/ul[@class="career-detail"]/li[1]'))
            info.append(item.xpath('./div[@class="career-com-bd"]/ul[@class="career-detail"]/li[2]'))
            info.append(item.xpath('./div[@class="career-com-bd"]/ul[@class="career-detail"]/li[3]'))
        return info

    # 获得宣讲会信息
    def get_seminars(self):
        info_list = self.get_info_by_lxml()
        for info in info_list:
            seminar = Seminar()
            seminar.format(info)
            self.seminars.append(seminar)


class DataClear:
    """
    数据清洗
    """
    # 经验枚举类
    class Experience(Enum):
        exp_1 = '一年以下'
        exp1_3 = '(1,3]'
        exp3_5 = '(3,5]'
        exp5_ = '五年以上'

    # 枚举区间的最大值
    __exp_dic = {Experience.exp_1.value: 1,
                 Experience.exp1_3.value: 3,
                 Experience.exp3_5.value: 5,
                 Experience.exp5_.value: 9999}

    def __init__(self, job_title_count=10):
        # 岗位名称：数量
        self.__title_dic = {}
        self.__title_list = []
        self.__title_count = job_title_count

    # 处理字符串中的一个/两个数字
    def solve_number(self, s):
        idx = 0
        while idx < len(s):
            if '0' <= s[idx] <= '9':
                break
            idx += 1

        num1 = ''
        flag1 = 1
        while idx < len(s):
            if '0' <= s[idx] <= '9' or s[idx] == '.':
                num1 += s[idx]
            else:
                if s[idx] == '万':
                    flag1 = 10000
                elif s[idx] == '千':
                    flag1 = 1000
                break
            idx += 1

        num2 = num1
        while idx < len(s):
            if '0' <= s[idx] <= '9':
                num2 = ''
                break
            idx += 1

        flag2 = 1
        while idx < len(s):
            if '0' <= s[idx] <= '9' or s[idx] == '.':
                num2 += s[idx]
            else:
                if s[idx] == '万':
                    flag2 = 10000
                elif s[idx] == '千':
                    flag2 = 1000
                break
            idx += 1

        if num1 == '':
            num1 = num2 = -1
        num1 = float(num1)
        num2 = float(num2)
        if flag1 != 1 or flag2 != 1:
            num1 *= max(flag1, flag2)
            num2 *= max(flag1, flag2)
        return [num1, num2]

    # 规范化岗位
    def get_title(self, title):
        for item in self.__title_list[:self.__title_count]:
            if title == item[0]:
                return title
        return '其他'

    # 规范化经验
    def get_experience(self, exp):
        num = self.solve_number(exp)
        result = ''
        start_year = num[0]
        end_year = num[1]
        # 没有捕获到数字，不限经验
        if start_year == -1:
            result += self.Experience.exp_1.value
            result += self.Experience.exp1_3.value
            result += self.Experience.exp3_5.value
            result += self.Experience.exp5_.value
            return result
        while start_year <= end_year:
            if start_year <= self.__exp_dic[self.Experience.exp_1.value]:
                result += self.Experience.exp_1.value
                start_year = min(end_year, self.__exp_dic[self.Experience.exp_1.value]) + 1
            elif start_year <= self.__exp_dic[self.Experience.exp1_3.value]:
                result += self.Experience.exp1_3.value
                start_year = min(end_year, self.__exp_dic[self.Experience.exp1_3.value]) + 1
            elif start_year <= self.__exp_dic[self.Experience.exp3_5.value]:
                result += self.Experience.exp3_5.value
                start_year = min(end_year, self.__exp_dic[self.Experience.exp3_5.value]) + 1
            elif start_year <= self.__exp_dic[self.Experience.exp5_.value]:
                result += self.Experience.exp5_.value
                start_year = min(end_year, self.__exp_dic[self.Experience.exp5_.value]) + 1
        return result

    # 规范化薪资
    def get_salary(self, sal):
        num = self.solve_number(sal)
        num[0] = max(0.0, num[0])
        num[1] = max(0.0, num[1])
        return (num[0] + num[1]) / 2.0

    # 规范化浏览量
    def get_page_views(self, views):
        num = self.solve_number(views)
        return int(num[0])

    # 判断是否是脏数据 脏数据return True
    def is_bad_data(self, info):
        if len(info) != 7:
            return True
        if '不限经验' in info[1]:
            return True
        if ('面谈' in info[3]) or ('面议' in info[3]):
            return True
        return False

    # 规范化
    def get_jobs(self, infos):
        data = []
        for item in infos:
            info = item.split(',')
            if self.is_bad_data(info):
                continue
            for i in range(len(info)):
                info[i] = info[i].strip()
            title = info[0]
            if title in self.__title_dic.keys():
                self.__title_dic[title] += 1
            else:
                self.__title_dic[title] = 1
            data.append(info)
        # 把字典按岗位数量降序排序
        self.__title_list = sorted(self.__title_dic.items(), key=lambda kv: kv[1], reverse=True)
        jobs = []
        for info in data:
            job = Job()
            job.title = self.get_title(info[0])
            job.experience = self.get_experience(info[1])
            job.education = info[2]
            job.salary = self.get_salary(info[3])
            job.company = info[4]
            job.page_views = self.get_page_views(info[5])
            job.city = info[6]
            jobs.append(job)
        return jobs


class MongoDB:
    """
    MongoDB数据库
    """
    def __init__(self, client_name, db_name):
        self.__client_name = client_name
        self.__db_name = db_name

    # 插入多条数据
    def insert_many(self, col_name, dict_list):
        client = pymongo.MongoClient(self.__client_name)
        db = client[self.__db_name]
        col = db[col_name]
        col.drop()
        x = col.insert_many(dict_list)


class MySQL:
    """
    MySQL数据库
    """
    def __init__(self, ip, user, pwd, db):
        self.__ip = ip
        self.__user = user
        self.__pwd = pwd
        self.__db = db

    # 插入一个列表 sql_list
    def insert_many(self, sql_list):
        db = MySQLdb.connect(self.__ip, self.__user, self.__pwd, self.__db, charset='utf8')
        cursor = db.cursor()
        try:
            for sql in sql_list:
                cursor.execute(sql)
            db.commit()
        except Exception as e:
            db.rollback()
        db.close()

    # 单独执行一条sql语句
    def insert(self, sql):
        db = MySQLdb.connect(self.__ip, self.__user, self.__pwd, self.__db, charset='utf8')
        cursor = db.cursor()
        try:
            cursor.execute(sql)
            db.commit()
        except Exception as e:
            db.rollback()
        db.close()


class DataBase:
    """
    数据库处理
    """
    def __init__(self):
        self.__mongodb_client_name = 'mongodb://localhost:27017/'
        self.__mongodb_db_name = 'soft863db'
        self.__mysql_ip = 'localhost'
        self.__mysql_user = 'root'
        self.__mysql_pwd = 'lgc2621690255'
        self.__mysql_db = 'soft863db'

    # 重置 mongodb 的 client 和 database
    def reset_mongodb(self, mongodb_client_name, mongodb_db_name):
        self.__mongodb_client_name = mongodb_client_name
        self.__mongodb_db_name = mongodb_db_name

    # 重置 mysql 的 ip,user,pwd 和 database
    def reset_mysql(self, ip, user, pwd, db):
        self.__mysql_ip = ip
        self.__mysql_user = user
        self.__mysql_pwd = pwd
        self.__mysql_db = db

    # 保存薪资数据到mongodb 表名默认为: jobs
    def save_jobs_mongodb(self, jobs):
        col_name = 'jobs'
        dict_list = []
        for job in jobs:
            dic = {
                'title': job.title,
                'experience': job.experience,
                'education': job.education,
                'salary': job.salary,
                'company': job.company,
                'page_views': job.page_views,
                'industry': job.industry,
                'company_size': job.company_size,
                'city': job.city,
                'responsibilities': job.responsibilities,
                'source_website': job.source_website,
                'update_time': job.update_time,
                'logo_information': job.logo_information
            }
            dict_list.append(dic)
        MongoDB(self.__mongodb_client_name, self.__mongodb_db_name).insert_many(col_name, dict_list)

    # 保存清洗过的薪资数据到mongodb 表名默认为: jobs_clear
    def save_jobs_clear_mongodb(self, jobs):
        col_name = 'jobs_clear'
        dict_list = []
        for job in jobs:
            dic = {
                'title': job.title,
                'experience': job.experience,
                'education': job.education,
                'salary': job.salary,
                'company': job.company,
                'page_views': job.page_views,
                'city': job.city
            }
            dict_list.append(dic)
        MongoDB(self.__mongodb_client_name, self.__mongodb_db_name).insert_many(col_name, dict_list)

    # 创建薪资数据表到mysql 表名默认为: tb_jobs
    def create_jobs_tb(self):
        sql_list = ['DROP TABLE IF EXISTS tb_jobs;']
        sql = 'CREATE TABLE tb_jobs(' \
              'id INT NOT NULL PRIMARY KEY,' \
              'title NVARCHAR(64),' \
              'experience NVARCHAR(32),' \
              'education NVARCHAR(16),' \
              'salary NVARCHAR(16),' \
              'company NVARCHAR(32),' \
              'page_views NVARCHAR(16),' \
              'industry NVARCHAR(64),' \
              'company_size NVARCHAR(16),' \
              'city NVARCHAR(16),' \
              'responsibilities NVARCHAR(8192),' \
              'source_website NVARCHAR(16),' \
              'update_time NVARCHAR(32),' \
              'logo_information NVARCHAR(128)' \
              ');'
        sql_list.append(sql)
        MySQL(self.__mysql_ip, self.__mysql_user, self.__mysql_pwd, self.__mysql_db).insert_many(sql_list)

    # 保存薪资数据到mysql 表名默认为: tb_jobs
    def save_jobs_mysql(self, jobs):
        self.create_jobs_tb()
        sql_list = []
        count_id = 0
        for job in jobs:
            count_id += 1
            sql = 'INSERT INTO tb_jobs VALUES('
            sql += str(count_id) + ','
            sql += '\'' + str(job.title) + '\','
            sql += '\'' + str(job.experience) + '\','
            sql += '\'' + str(job.education) + '\','
            sql += '\'' + str(job.salary) + '\','
            sql += '\'' + str(job.company) + '\','
            sql += '\'' + str(job.page_views) + '\','
            sql += '\'' + str(job.industry) + '\','
            sql += '\'' + str(job.company_size) + '\','
            sql += '\'' + str(job.city) + '\','
            sql += repr(str(job.responsibilities)) + ','
            sql += '\'' + str(job.source_website) + '\','
            sql += '\'' + str(job.update_time) + '\','
            sql += '\'' + str(job.logo_information) + '\''
            sql += ');'
            sql_list.append(sql)
            # MySQL(self.__mysql_ip, self.__mysql_user, self.__mysql_pwd, self.__mysql_db).insert(sql)
        MySQL(self.__mysql_ip, self.__mysql_user, self.__mysql_pwd, self.__mysql_db).insert_many(sql_list)

    # 创建清洗过的薪资数据表到mysql 表名默认为: tb_jobs_clear
    def create_jobs_clear_tb(self):
        sql_list = ['DROP TABLE IF EXISTS tb_jobs_clear;']
        sql = 'CREATE TABLE tb_jobs_clear(' \
              'id INT NOT NULL PRIMARY KEY,' \
              'title NVARCHAR(32),' \
              'experience NVARCHAR(32),' \
              'education NVARCHAR(16),' \
              'salary NVARCHAR(16),' \
              'company NVARCHAR(32),' \
              'page_views NVARCHAR(16),' \
              'city NVARCHAR(16)' \
              ');'
        sql_list.append(sql)
        MySQL(self.__mysql_ip, self.__mysql_user, self.__mysql_pwd, self.__mysql_db).insert_many(sql_list)

    # 保存清洗过的薪资数据到mysql 表名默认为: tb_jobs_clear
    def save_jobs_clear_mysql(self, jobs):
        self.create_jobs_clear_tb()
        sql_list = []
        count_id = 0
        for job in jobs:
            count_id += 1
            sql = 'INSERT INTO tb_jobs_clear VALUES('
            sql += str(count_id) + ','
            sql += '\'' + str(job.title) + '\','
            sql += '\'' + str(job.experience) + '\','
            sql += '\'' + str(job.education) + '\','
            sql += '\'' + str(job.salary) + '\','
            sql += '\'' + str(job.company) + '\','
            sql += '\'' + str(job.page_views) + '\','
            sql += '\'' + str(job.city) + '\''
            sql += ');'
            sql_list.append(sql)
        MySQL(self.__mysql_ip, self.__mysql_user, self.__mysql_pwd, self.__mysql_db).insert_many(sql_list)


class Filepath:
    """
    使用到的文件路径

    默认路径: os.getcwd 即py文件所在目录
    修改文件路径：
        xx = ('xx','xx') 变量名 = (文件名, 路径)
        可替换路径和文件名,修改变量名需同步修改代码

    文件分为原文件和副本，区别如下：
        爬取网页过多时，爬虫效果不稳定，每次运行结果均保存在“副本”中，最新一次的结果保存于原文件中
    """
    # 需要读取的文件，统一存放于read_file目录下
    read_city_txt = ('热门城市.txt', os.getcwd() + os.sep + 'read_file')
    read_title_txt = ('关注岗位.txt', os.getcwd() + os.sep + 'read_file')
    read_company_xlsx = ('企业信息.xlsx', os.getcwd() + os.sep + 'read_file')

    # 需要写入的文件，统一存放于write_file目录下
    write_salary_txt = ('薪资数据.txt', os.getcwd() + os.sep + 'write_file')
    write_salary_xlsx = ('薪资数据.xlsx', os.getcwd() + os.sep + 'write_file')
    write_company_xlsx = ('公司信息数据.xlsx', os.getcwd() + os.sep + 'write_file')
    write_seminar_xlsx = ('宣讲会信息.xlsx', os.getcwd() + os.sep + 'write_file')
    write_salary_cleared_txt = ('薪资数据(清洗后).txt', os.getcwd() + os.sep + 'write_file')

    # 副本 统一存放于write_file\history目录下
    write_salary_copy_txt = ('薪资数据(副本).txt', os.getcwd() + os.sep + 'write_file' + os.sep + 'history')
    write_salary_copy_xlsx = ('薪资数据(副本).xlsx', os.getcwd() + os.sep + 'write_file' + os.sep + 'history')
    write_company_copy_xlsx = ('公司信息数据(副本).xlsx', os.getcwd() + os.sep + 'write_file' + os.sep + 'history')
    write_seminar_copy_xlsx = ('宣讲会信息(副本).xlsx', os.getcwd() + os.sep + 'write_file' + os.sep + 'history')

    # 获取文件路径
    def get_filepath(self, file):
        if not os.path.exists(file[1]):
            os.makedirs(file[1])
        return file[1] + os.sep + file[0]


# ---------- start 薪资数据爬取到txt和excel (耗时，不建议运行) ----------
# xz_reptile = XZReptile()
# xz_reptile.get_jobs()
# MyFile().save_jobs_txt(Filepath().get_filepath(Filepath().write_salary_txt), xz_reptile.jobs)
# MyFile().save_jobs_excel(Filepath().get_filepath(Filepath().write_salary_xlsx), xz_reptile.jobs)
# ---------- end 薪资数据爬取到txt和excel (耗时，不建议运行) ----------

# ---------- start 将爬取到的薪资数据保存到mysql和mongodb (单独运行时，需爬取文件存在) ----------
# jobs_list = MyFile().load_jobs_excel(Filepath().get_filepath(Filepath().write_salary_xlsx))
# DataBase().save_jobs_mongodb(jobs_list)
# DataBase().save_jobs_mysql(jobs_list)
# ---------- end 将爬取到的薪资数据保存到mysql和mongodb (单独运行时，需爬取文件存在) ----------

# ---------- start 公司信息数据爬取到excel ----------
# gs_reptile = GSReptile()
# gs_reptile.get_companies()
# MyFile().save_companies_excel(Filepath().get_filepath(Filepath().write_company_xlsx), gs_reptile.companies)
# ---------- end 公司信息数据爬取到excel ----------

# ---------- start 宣讲会信息爬取到excel ----------
# xjh_reptile = XJHReptile()
# xjh_reptile.get_seminars()
# MyFile().save_seminars_excel(Filepath().get_filepath(Filepath().write_seminar_xlsx), xjh_reptile.seminars)
# ---------- end 宣讲会信息爬取到excel ----------

# ---------- start 读取薪资数据并规范化后存入mysql和mongodb ----------
# jobs_info = MyFile().load_jobs_txt(Filepath().get_filepath(Filepath().write_salary_txt))
# jobs_list = DataClear().get_jobs(jobs_info)
# MyFile().save_jobs_txt(Filepath().get_filepath(Filepath().write_salary_cleared_txt), jobs_list)
# DataBase().save_jobs_clear_mongodb(jobs_list)
# DataBase().save_jobs_clear_mysql(jobs_list)
# ---------- end 读取薪资数据并规范化后存入mysql和mongodb ----------
