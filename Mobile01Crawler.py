# coding = utf-8
import warnings
import codecs
import csv
import json
import pprint
import datetime
pp = pprint.PrettyPrinter(indent=4)
warnings.filterwarnings('ignore')

import re
import time
import sqlite3
import requests
from pandas import DataFrame
from bs4 import BeautifulSoup
from multiprocessing import Pool

##########################################
# get the html code with given url       #
# param: url -> url of the web page      #
##########################################
def GetPageContent(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36'}
    res = requests.get(url, headers=headers)
    content = BeautifulSoup(res.text)
    return content #把html爬下來

#########################################
# get all topics and save as a txt file #
# param: url -> url of the home page    #
#########################################
def GetAllTopic(url):
    content = GetPageContent(url)

    all_topic = content.select_one('#top-menu').select('li')
    all_topic = [each for each in all_topic if 'topiclist' in each.find('a')['href'] or 'waypointtopiclist' in each.find('a')['href']]
    
    topic_dict = dict()
    idx = 0
    with open('topic_list_.txt', 'w',encoding='utf-8') as file:
        for each in all_topic:
            topic_link = each.find('a')['href']
            topic_page = GetPageContent('https://www.mobile01.com/'+topic_link)
            nav = topic_page.select('p.nav')[0].text
            start = nav.find('»')
            topic_name = nav[start+1:].lstrip().rstrip()
            while ' » ' in topic_name:
                topic_name = topic_name.replace(' » ', '>')
            while ' ' in topic_name:
                topic_name = topic_name.replace(' ', '')

            topic_dict[str(idx)] = [topic_link, topic_name]
            file.write(f'{idx} {topic_link} {topic_name}\n')
            print(f'{idx} {topic_link} {topic_name}\n')
            idx += 1
    return topic_dict

#################################
# read topic list from txt file #
#################################
def ReadTopic():
    topic_dict = dict()
    file = 'topic_list'
    with codecs.open(file, 'r', encoding='utf-8', errors='ignore') as file:
        for line in file:
            topic = line.replace('\n', '').split(' ')
            topic_dict[topic[0]] = [topic[1], topic[2]]
    file.close()
    return topic_dict

# 取得該分類文章總頁數
def GetTotalPageNum(url):
    content = GetPageContent('https://www.mobile01.com/' + url)
    #pagination = content.select('div.pagination') # "[1][2][3]...下一頁"這行y
    page_link = content.select('.l-tabulate__action .l-pagination__page a')
    if True:
        if page_link:
            last_page = page_link[-1]['href']            # 該行的最後一個按鈕就是最後一頁的網址
            replace ='/'+ url + '&p='
            total_page = last_page.replace(replace, '') # 把前面的字串過濾掉，只要網址後面的數字 (頁數)
        else:
            total_page = 1
    else:
        total_page = -1
    return total_page #type = str


def dim(a):
    if not type(a) == list:
        return []
    return [len(a)] + dim(a[0])


# 取得從第 1 頁到第 page_num 頁的所有文章網址
def GetPosts(page_num, url): #回傳每一討論文的: 網址\標題\時間\作者\回應文數量
    posts = list()
    url = url.strip()
    for i in range(1, 1 + page_num): #汽車綜合討論區共20頁

        content = GetPageContent('https://www.mobile01.com/' + url + '&p=' + str(i))
        all_link = [link.get('href') for link in content.find_all('a',{'class':'c-link u-ellipsis'})] #文章網址
        all_title = [title.text for title in content.find_all('a',{'class':'c-link u-ellipsis'})] # 文章標題
        all_date = [date.string for date in content.find_all('div',{'class':'o-fNotes'})] # 文章發布日期
        all_author = [author.string for author in content.find_all('div',{'class':None})] #作者
        all_reply = [reply.string for reply in content.find_all('div',{'class':'o-fMini'})] # 回覆量
        all_author[:] = all_author[::2] # delete repeat author name
        all_date[:] = all_date[::2] # delete repeat date time

        links = content.find_all('a',{'class':'c-link u-ellipsis'}) #每一頁文章網址合集
        link_list = []
        app = {}
        app.setdefault('message',[])
        for link in links:
            link_list.append(link.get('href'))
        

        for j in range(len(link_list)): #去一頁頁地抓每一討論文的回應(存到message)
            msg, tmplist = [], []
            message_link = 'https://www.mobile01.com/'+link_list[j] #每一個討論文的url,只有第一頁，要再加&p=2~pagination
            push_content = GetPageContent(message_link) #獲取每一個討論文的第一頁url 的 html --> 為了拿pagination 
            pagination_list = [page.text for page in push_content.find_all('a',{'class':'c-pagination'})] 
            pagination = pagination_list[-1] if pagination_list else 0 #挑出pagination
            push_ipdatetime, push_userid, all_push_content = [], [], []
            for p in range(1,int(pagination)+1): #每一討論文的回應文的頁數(=pagination)，這邊會去一頁頁地抓回應
                message_link = 'https://www.mobile01.com/'+link_list[j]+'&p='+str(p) #更改message_link, 應該要從1~pagination
                push_content = GetPageContent(message_link) #更改push_content
                all_push_content.extend([push.text.strip().strip("\n") for push in push_content.find_all('article',{'class':'u-gapBottom--max c-articleLimit'})])
                tmp_ipdatetime = [push_datetime.text.strip().strip("\n") for push_datetime in push_content.find_all('span',{'class':'o-fNotes o-fSubMini'})]
                if p == 1:
                    tmp_ipdatetime = tmp_ipdatetime[4:] # 拿掉發文者時間和發文者id
                else:
                    tmp_ipdatetime = tmp_ipdatetime[2:] # 第二頁之後的回應也會把發文者ipdatetime紀錄進去，所以要pop掉
                push_ipdatetime.extend([tmp_ipdatetime[i] for i in range(len(tmp_ipdatetime)) if i%2 ==0 ]) # remove #1, #2, #3 ...
                tmp_userid = [userid.text.strip().strip("\n") for userid in push_content.find_all('a',{'class':'c-link c-link--gn u-ellipsis'})]
                if p==1 and j==0:
                    tmp_userid.pop(0) #把發文者id拿掉，第二頁之後就沒有這個問題
                elif p==1 and j!=0:
                    tmp_userid.pop(0)
                    tmp_userid.pop(0)
                push_userid.extend([tmp_userid[i] for i in range(len(tmp_userid)) if i%2 == 0]) #拿掉重複的回文者id
                for k in range(len(push_userid)-1): #把資訊放進msg_dict
                    msg_dict ={}
                    msg_dict['push_content'] = all_push_content[k]
                    msg_dict['push_ipdatetime'] = push_ipdatetime[k]
                    msg_dict['push_userid'] = push_userid[k]
                    msg.append(msg_dict)
            tmplist.append(msg)
            app['message'].append(tmplist[0])
        app['article_url'] = all_link
        app['article_title'] = all_title
        app['date'] = all_date
        app['author'] = all_author
        app['reply'] = all_reply
        posts.append(app)
        print('Has scraped ',len(posts),' pages')

    
    return posts

#################################
# get the article of the post   #
# param: url -> url of the post #
#################################
def ParseGetArticle(url):
    soup = GetPageContent('https://www.mobile01.com/' + url)
    origin = soup.find('article', {'class':'l-publishArea topic_article'}) # 文章內文在<'article', {'class':'l-publishArea topic_article'> 底下
    if origin:
        content = str(origin)
        # replace <br>, <br\> and '\n' with a whitespace########
        content = re.sub("<br\s*>", " ", content, flags=re.I)  #
        content = re.sub("<br\s*/>", " ", content, flags=re.I) #
        content = re.sub("\n+", " ", content, flags=re.I)      #
        ########################################################
        # remove hyperlink
        content = re.sub("<a\s+[^<>]+>(?P<aContent>[^<>]+?)</a>", "\g<aContent>", content, flags=re.I)
        content = BeautifulSoup(content)
        content = ' '.join(content.text.lstrip().rstrip().split())
    else:
        content = 'None'

    return content

###################################################
# get the clean content of the post               #
# param: list(str) -> list(str) of the post       #
###################################################

def clean_regex(org): # convert org_content to content 
    org_contents = []
    for i in range(len(org)):
        for j in range(len(org[i])):
            org_contents.append(str(org[i][j]['org_content']))
    pat = "[\s+\d+\W+]" 
    for i in range(len(org_contents)):
        org_contents[i] = re.sub(pat,"",org_contents[i], flags=re.I)
    return org_contents

 

##############################################
# get the articles of each post              #
# param: post_list -> mata data of all posts #
##############################################
def GetArticles(post_list):
    articles = list()
    for i in range(len(post_list)):
        tmp = []
        for j in range(len(post_list[i]['article_title'])):
            tmp.append({
                'article_title': post_list[i]['article_title'], 
                'article_url': post_list[i]['article_url'], 
                'author': post_list[i]['author'], 
                'message_count': {'all':post_list[i]['reply']}, 
                'org_content': ParseGetArticle(post_list[i]['article_url'][j]),
                'ner_content':(post_list[i]['article_title'][j]+ParseGetArticle(post_list[i]['article_url'][j])),
                'messages': post_list[i]['message'],
                'date':post_list[i]['date']
            })
        articles.append(tmp)
    return articles

##########################################
# save data into SQLite database         #
# param: db_name -> name of the database #
#        posts -> posts data             #
##########################################

# def Save2DB(db_name, posts):
#     conn = sqlite3.connect(db_name)
#     cur = conn.cursor()
#     create_table = """ CREATE TABLE IF NOT EXISTS table1(
#                         ID integer PRIMARY KEY,
#                         title text NOT NULL,
#                         link text NOT NULL,
#                         date text NOT NULL,
#                         author text NOT NULL,
#                         reply text NOT NULL,
#                         content text NOT NULL
#                         ); """
#     cur.execute(create_table)
#     for i in posts:
#         cur.execute("insert into table1 (title, link, date, author, reply, content) values (?, ?, ?, ?, ?, ?)",
#             (i['title'], i['link'], i['date'], i['author'], i['reply'], i['content']))
#     conn.commit()
#     conn.close()

##############################
# save data into excel       #
# param: posts -> posts data #
##############################
# def Save2Excel(posts):
#     titles = [entry['title'] for entry in posts]
#     links = [entry['link'] for entry in posts]
#     dates = [entry['date'] for entry in posts]
#     authors = [entry['author'] for entry in posts]
#     replies = [entry['reply'] for entry in posts]
#     contents = [entry['content'] for entry in posts]
#     df = DataFrame({
#         'title':titles,
#         'link':links,
#         'date': dates,
#         'author':authors,
#         'reply': replies,
#         'content': contents
#         })
#     df.to_excel('data.xlsx', sheet_name='sheet1', index=False, columns=['title', 'link', 'date', 'author', 'reply', 'content'])


##############################
# save data into csv       #
# param: posts -> posts data #
##############################
def Save2Csv(posts):
    titles, links, authors, replies, dates = [], [], [], [], []
    for i in range(len(posts)):   #將article_title, article_url, author, message_count, date 分別存成list
        titles.extend(posts[i][0]['article_title'])
        links.extend(posts[i][0]['article_url'])
        authors.extend(posts[i][0]['author'])
        replies.extend(posts[i][0]['message_count']['all'])
        dates.extend(posts[i][0]['date'])

    org_contents, ner_contents, messages = [], [], []
    for i in range(len(posts)):  #將org_content, ner_content, content, messages 分別存成list
        for j in range(len(posts[i])):
            org_contents.append(str(posts[i][j]['org_content']))
            ner_contents.append(str(posts[i][j]['ner_content']))
            content = clean_regex(posts)
            messages.append(posts[i][0]['messages'][j]) 

    with open('data.csv', 'w', newline='',encoding='utf-8') as csvfile:
        fieldnames = ['article_title','board','article_source','mdy_date','crt_date','article_url','author','message_count','org_content','ner_content','content','messages','date']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(len(titles)):           
            row = {
                'article_title':titles[i],
                'board':'car',
                'article_source':'mobile01',
                'mdy_date': datetime.datetime.utcnow(),
                'crt_date': datetime.datetime.utcnow(),
                'article_url':'https://www.mobile01.com/'+links[i],
                'author':authors[i],
                'message_count': replies[i],
                'org_content': org_contents[i],
                'ner_content':ner_contents[i],
                'content':content[i],
                'messages':messages[i],
                'date':dates[i]
                }
            writer.writerow(row)

    csvfile.close()



##############################
# save data into json       #
# param: posts -> posts data #
##############################

def Save2json(posts):
    titles, links, authors, replies, dates = [], [], [], [], []
    for i in range(len(posts)): #將article_title, article_url, author, message_count, date 分別存成list
        titles.extend(posts[i][0]['article_title'])
        links.extend(posts[i][0]['article_url'])
        authors.extend(posts[i][0]['author'])
        replies.extend(posts[i][0]['message_count']['all'])
        dates.extend(posts[i][0]['date'])
    org_contents, ner_contents, messages = [], [], [] #將org_content, ner_content, content, messages 分別存成list
    for i in range(len(posts)):
        for j in range(len(posts[i])):
            org_contents.append(str(posts[i][j]['org_content']))
            ner_contents.append(str(posts[i][j]['ner_content']))
            content = clean_regex(posts)
            messages.append(posts[i][0]['messages'][j]) 
    with open('data.json', 'w',encoding='utf-8') as json_file:
        for i in range(len(titles)):           
            row = {
                'article_title':titles[i],
                'board':'car',
                'article_source':'mobile01',
                'mdy_date': str(datetime.datetime.utcnow()), #datatime is not JSONSerializable, convert it to string
                'crt_date': str(datetime.datetime.utcnow()),
                'article_url':'https://www.mobile01.com/'+links[i],
                'author':authors[i],
                'message_count': replies[i],
                'org_content': org_contents[i],
                'ner_content':ner_contents[i],
                'content':content[i],
                'messages':messages[i],
                'date':dates[i]
                }
            json.dump(row, json_file,indent=4, sort_keys=True,ensure_ascii=False)
    json_file.close()