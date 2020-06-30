import urllib.request
from bs4 import BeautifulSoup
import ibm_db_dbi as db
import re
import requests
from urllib.request import Request, urlopen
import odnoklassniki

def telegram_bot_sendtext(bot_message):

    bot_token = '822684671:AAHwtyvu4DD0zcsVDO6CihiJaVjENaLzNJw'
    bot_chatID = '534789055'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)
    return response.json()
text_sms1 = "успешно ok1"
text_sms2 = 'сломан ok1'
text_sms3 = 'старт ok1'
telegram_bot_sendtext(text_sms3)
try:
    url_gr = []
    url_gr.append('https://ok.ru/salemstandup')#Salem Stand Up
    url_gr.append('https://ok.ru/qpopsalem')#Q-POP Salem
    url_gr.append('https://ok.ru/salembeauty')#Salem Beauty
    url_gr.append("https://ok.ru/salemsayahat")
    url_gr.append('https://ok.ru/salemsocialkaz')#Salem Social Media KAZ
    url_gr.append('https://ok.ru/salemsocial')#Salem Social Media 

    channel_id=['55883777704060','55890148065293','55890149113869','55917600178189','55890134499341','55890118115341']
    for n in range(len(url_gr)):
        print(url_gr[n])
        page = requests.get(url_gr[n])
        soup = BeautifulSoup(page.text)
        q = soup.find('div',{'class':'feed-list'})
        a=q.find_all('div',{'class':'feed js-video-scope h-mod'})
        for g in a:
            b = g.find('div',{'class':'feed_cnt'}).get('data-l')
            if b!=None:
                topicId = (re.split(r',', b)[1])

                bd_object_id = []
                connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
                sql_1_test = "SELECT object_id FROM TL_MEDIA_DATA_ODNOKLASSNIKI where channel_id="+channel_id[n]
                con = db.connect(connection_text, "", "")
                cursor = con.cursor()
                cursor.execute(sql_1_test)
                bd_object_id = list(sum(list(cursor), ()))


                ok = odnoklassniki.Odnoklassniki('CBAQGQENEBABABABA', 'C68BE66D5752DE88A72655E0', 'tkn1MNj03HNDcn2Nv7ZKaI53qN63NeLdUSwIkLFhkHgFQMClwck8XVRpvf6YgnBmZt6f9')
                #topicId = re.split(r',', el.get_attribute("data-l"))[1]
                ss = ok.discussions.get(discussionId=topicId, discussionType='group_topic')              
                if ss!={}:
                    object_id = ss['discussion']['object_id']
                    if not object_id in bd_object_id:
                        print(topicId)
                        creation_date = ss['discussion']['creation_date']
                        if len(ss['discussion'])<=13:
                            text = None
                        else:
                            text = ss['discussion']['title'].encode(encoding='UTF-8', errors='strict')
                        total_comments_count = ss['discussion']['total_comments_count']
                        like_count = ss['discussion']['like_count']
                        #channel_id = ss['discussion']['ref_objects'][0]['id']
                        url_channel = url_gr[n] + '/topic/' + topicId
                        #print(object_id,creation_date,text,total_comments_count,like_count,url_channel)
                        #print()


                        connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
                        #connection_text = "DATABASE=sample;HOSTNAME=192.168.2.23;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=1q2w3e4R;"
                        con = db.connect(connection_text, "", "") 
                        cursor = con.cursor() 
                        sql_1_test = "insert into  TL_MEDIA_DATA_ODNOKLASSNIKI (object_id , published_date , channel_id , likes , comments , views , reposts , caption , text , url_attachment , url_channel , source_id ) values (?,?,?,?,?,?,?,?,?,?,?,?)" 
                        #cursor.execute(sql_1_test, (object_id, creation_date, channel_id[n], like_count, total_comments_count , None, None, None, text, None, url_channel, 6)) 
                        cursor.execute(sql_1_test, (object_id, creation_date, channel_id[n], like_count, total_comments_count, None, None, None, text, None, url_channel, 6)) 
                        con.commit() 


                        sql = 'SELECT comment_id FROM TL_MEDIA_COMMENTS_ODNOKLASSNIKI where object_id='+object_id
                        con = db.connect(connection_text, "", "")
                        cursor = con.cursor()
                        cursor.execute(sql)
                        bd_comment_id = list(sum(list(cursor), ()))
                        con.commit()

                        for i in range(int(total_comments_count/100)+1):
                            com = ok.discussions.getDiscussionComments(entityId=topicId,entityType='GROUP_TOPIC',count=100,offset=100*i)
                            for j in range(len(com['commentss'])):
                                com_comment_id = com['commentss'][j]['id']
                                com_object_id = object_id
                                com_published_date=com['commentss'][j]['date']
                                if com['commentss'][j]['text']=='':
                                    com_comment_text = None
                                else:
                                    com_comment_text = com['commentss'][j]['text'].encode(encoding='UTF-8', errors='strict')                                
                                com_author_id = com['commentss'][j]['author_id']
                                if ok.users.getInfo(uids=com_author_id, fields='first_name,last_name')!=[]:
                                    a = ok.users.getInfo(uids=com_author_id, fields='first_name,last_name')[0]
                                    com_author_name=a['first_name']+' '+a['last_name']
                                else:
                                    com_author_name = None
                                com_author_url = 'https://ok.ru/profile/'+com_author_id
                                #print(com_comment_id,com_object_id,com_published_date,com_comment_text,com_author_id,com_author_url)

                                if not com_comment_id in bd_comment_id:
                                    con = db.connect(connection_text, "", "") 
                                    cursor = con.cursor() 
                                    sql_1_test = "insert into TL_MEDIA_COMMENTS_ODNOKLASSNIKI (comment_id, object_id, published_date, comment_likes, comment_text, author_id, author_name, author_url, parent_id, SOURCE_ID) values (?,?,?,?,?,?,?,?,?,?)" 
                                    #cursor.execute(sql_1_test, (com_comment_id, com_object_id, com_published_date, None, com_comment_text, com_author_id, com_author_name, com_author_url, None, '6')) 
                                    cursor.execute(sql_1_test, (com_comment_id, com_object_id, com_published_date, None, com_comment_text, com_author_id, com_author_name.encode(encoding='UTF-8', errors='strict'), com_author_url, None, '6'))
                                    con.commit() 
                                    cursor.close() 
                                    con.close() 
    telegram_bot_sendtext(text_sms1)
except:
    telegram_bot_sendtext(text_sms2)
