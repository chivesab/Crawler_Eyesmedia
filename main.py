import Mobile01Crawler as MCrawler
import time
import schedule

def main():
    topic_dict = MCrawler.ReadTopic()
    #print(u'Whyich topic would you want to crawl?')
    #idx = input('Input the number in front of the topic (see topic_list.txt): ').strip()
    idx = '0'
    total_page_num = int(MCrawler.GetTotalPageNum(topic_dict[idx][0]))

    print(u'Topic {{ {} }} has {} pages in total.'.format(topic_dict[idx][1], total_page_num))
    page_want_to_crawl = input(u'How many pages do you want to crawl? ').lstrip().rstrip()
    if page_want_to_crawl == '' or not page_want_to_crawl.isdigit() or int(page_want_to_crawl) <= 0:
            print(u'EXIT')
    else:
        page_want_to_crawl = min(int(page_want_to_crawl), total_page_num)

        start = time.time()

        posts = MCrawler.GetPosts(page_want_to_crawl, topic_dict[idx][0])
        print(u'{} posts in total.'.format(len(posts)))
        posts_data = MCrawler.GetArticles(posts)
        print(u'Finish. Spend {} seconds on crawling.'.format(time.time()-start))



        # ans = input('Save to database? [yes/no]:')
        # if ans.lower() == 'yes':
        #     MCrawler.Save2DB('data.db', posts_data)
        
        # ans = input('Save to excel? [yes/no]:')
        # if ans.lower() == 'yes':
        #     MCrawler.Save2Excel(posts_data)
        #ans = input('Save to csv? [yes/no]:')
        
        #if ans.lower() =='yes' or ans.lower()=='y': #目前會直接存csv
        if True:
            print("Start saving data.csv")
            start = time.time()
            MCrawler.Save2Csv(posts_data)
            print(u'Finish. Spend {} seconds on saving data.csv'.format(time.time()-start))

        #ans = input('Save to json? [yes/no]:')
        #if ans.lower() =='yes' or ans.lower()=='y': #目前會直接存json
        if True:
            print("Start saving data.json")
            start = time.time()
            MCrawler.Save2json(posts_data)
            print(u'Finish. Spend {} seconds on saving data.json'.format(time.time()-start))
        

if __name__ == '__main__':
    main()
    