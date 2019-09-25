# coding = utf-8
import warnings
import codecs
import csv
import json
import pprint
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
    #pagination = content.select('div.pagination') # "[1][2][3]...下一頁"這行
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

# 取得從第 1 頁到第 page_num 頁的所有文章網址
def GetPosts(page_num, url):
    posts = list()
    url = url.strip()
    msg = []
    for i in range(1, 1 + page_num):

        content = GetPageContent('https://www.mobile01.com/' + url + '&p=' + str(i))
        all_link = [link.get('href') for link in content.find_all('a',{'class':'c-link u-ellipsis'})] #文章網址
        all_title = [title.text for title in content.find_all('a',{'class':'c-link u-ellipsis'})] # 文章標題
        all_reply = [reply.string for reply in content.find_all('div',{'class':'o-fMini'})] # 回覆量
        all_date = [date.string for date in content.find_all('div',{'class':'o-fNotes'})] # 文章發布日期
        all_author = [author.string for author in content.find_all('div',{'class':None})] #作者
        

        links = content.find_all('a',{'class':'c-link u-ellipsis'}) #文章網址合集
        link_list = []
        for link in links:
            link_list.append(link.get('href'))
        for j in range(len(link_list)):
            message_link = 'https://www.mobile01.com/'+link_list[j] #每一個討論文的url
            push_content = GetPageContent(message_link) #獲取每一個討論文的url 的 html 
            all_push_content = [push.text.strip().strip("\n") for push in push_content.find_all('article',{'class':'u-gapBottom--max c-articleLimit'})]
            tmp_ipdatetime = [push_datetime.text.strip().strip("\n") for push_datetime in push_content.find_all('span',{'class':'o-fNotes o-fSubMini'})]
            push_ipdatetime = [tmp_ipdatetime[i] for i in range(len(tmp_ipdatetime)) if i%2 ==0 ] # remove #1, #2, #3 ...
            push_ipdatetime.pop(0) # 這是發文者的發文時間，我們現在要的只有回文者的回文時間
            push_ipdatetime.pop(0) # #1是發文者，#2才是一樓

            tmp_userid = [userid.text.strip().strip("\n") for userid in push_content.find_all('a',{'class':'c-link c-link--gn u-ellipsis'})]
            tmp_userid.pop(0) #把發文者id拿掉
            push_userid = [tmp_userid[i] for i in range(len(tmp_userid)) if i%2 == 0] #拿掉重複的發文者id

            for k in range(len(push_userid)-1):
                msg_dict ={}
                msg_dict['push_content'] = all_push_content[k]
                msg_dict['push_ipdatetime'] = push_ipdatetime[k]
                msg_dict['push_userid'] = push_userid[k]
                msg.append(msg_dict)


            posts.append({
                'link': all_link,
                'title': all_title,
                'date': all_date,
                'author': all_author,
                'reply': all_reply,
                'message': msg
            })

        # 單次存取
        # title = content.find('a',{'class':'c-link u-ellipsis'}).text       # 文章標題
        # reply = content.find('div',{'class':'o-fMini'}).string  # 回覆量
        # date = content.find('div',{'class':'o-fNotes'}).string   # 文章發布日期
        # author = content.find('div',{'class':None}).string #select div without class attribute
        # message = push_content.find('article',{'class':'u-gapBottom--max c-articleLimit'}).text #select reply message


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

##############################################
# get the articles of each post              #
# param: post_list -> mata data of all posts #
##############################################
def GetArticles(post_list):

    articles = list()
    for i in range(len(post_list)):
        for j in range(len(post_list[i]['title'])):
            articles.append({
                'title': post_list[i]['title'][j], 
                'link': post_list[i]['link'][j], 
                'date': post_list[i]['date'][j],
                'author': post_list[i]['author'][j], 
                'reply': post_list[i]['reply'][j], 
                'content': ParseGetArticle(post_list[i]['link'][j]),
                'messages': post_list[i]['message']
            })
            
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
    titles = [entry['title'] for entry in posts]
    links = [entry['link'] for entry in posts]
    dates = [entry['date'] for entry in posts]
    authors = [entry['author'] for entry in posts]
    replies = [entry['reply'] for entry in posts]
    contents = [entry['content'] for entry in posts]
    messages = [entry['messages'] for entry in posts]
    #一個entry['messages'] ＝ [{'push_content': 'dun3375 wrote:isaac0611大...(恕刪)哈～謝謝你，你沒說我都不知道我分數有增加呢而且這篇加10分耶，
    # 應該還有另外一位善心人士加分，一併說謝謝', 'push_ipdatetime': '2010-01-17 2:25', 'push_userid': 'A79002529'}]


    with open('data.csv', 'w', newline='') as csvfile:
        fieldnames = ['title','link','date','author','reply','content','messages']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(len(titles)):            
            row = {
                'title':[titles[i]][0],
                'link':['https://www.mobile01.com/'+links[i]][0],
                'date': [dates[i]][0],
                'author':[authors[i]][0],
                'reply': [replies[i]][0],
                'content': [contents[i]][0],
                'messages':messages[i]
                }
            writer.writerow(row)


##############################
# save data into json       #
# param: posts -> posts data #
##############################

def Save2json(posts):
    titles = [entry['title'] for entry in posts]
    links = [entry['link'] for entry in posts]
    dates = [entry['date'] for entry in posts]
    authors = [entry['author'] for entry in posts]
    replies = [entry['reply'] for entry in posts]
    contents = [entry['content'] for entry in posts]
    messages = [entry['messages'] for entry in posts]
    with open('data.json', 'w') as json_file:
        for i in range(len(titles)):            
            row = {
                'title':[titles[i]][0],
                'link':['https://www.mobile01.com/'+links[i]][0],
                'date': [dates[i]][0],
                'author':[authors[i]][0],
                'reply': [replies[i]][0],
                'content': [contents[i]][0],
                'messages':messages[i]
            }
            json.dump(row, json_file,indent=4, sort_keys=True,ensure_ascii=False)
