import os
import googleapiclient.discovery
import threading
import ibm_db_dbi as db
import datetime
import requests

connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
con = db.connect(connection_text, "", "")
cursor = con.cursor()
sql_1_test = "SELECT * from tl_media_comments_yt  where published_date>'2019-10-01 00:00:00' and published_date<'2019-11-01 00:00:00'"  # +str(channel)+""
cursor.execute(sql_1_test)  # id_, date_time_new[0].replace(","," "),channel, None, None, int(views),None,None, str(abc), str(lohc),kanal[channel],5))
list1 = cursor.fetchall()
con.commit()
cursor.close()
con.close()
ids=[]

for i in range(len(list1)):
    if   "https://yt3" in list1[i][-3]:
          ids.append(i)



ListOfApi_key =[
                "AIzaSyDVi-xtiWoBuuoSzm-0i1tZSJVz0PPlhRo",
                "AIzaSyBNAu2AqeMXRn-RcJYdrw6fWCNVCA9Q5uY",
                "AIzaSyD2rWPIT5AqnIdwCeiyFqB4VUHWKkNnzYo",
                "AIzaSyDwS49YXnjng7FKQzlSF-d6Ve0ZfQhlEtM",
                "AIzaSyBVwqWaFy1sNEkhOk_aZuGUm2LeW7x-lJI",
                "AIzaSyBEDgudxwt6KLyROarBDwAa7wxpDy6GbKw"
               ]
count=1
for i in range(39967,len(ids)):
    comment_id=list1[ids[i]][1]
    api_key = ListOfApi_key[1]
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = api_key
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=DEVELOPER_KEY)
    request = youtube.comments().list(
            part="snippet",
            id=comment_id
        )
    response = request.execute()
    try:
        channel_url=response['items'][0]['snippet']['authorChannelUrl'].split('/channel/')[1]
        connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
        con = db.connect(connection_text, "", "")
        cursor = con.cursor()
        sql_1_test = "update tl_media_comments_yt set author_url='"+channel_url+ "' where COMMENT_ID='" + comment_id + "'"
        cursor.execute(sql_1_test)
        con.commit()
        cursor.close()
        con.close()
        print(comment_id+" "+str(count) + " "+ str(i))
        count+=1
    except IndexError:
        print("Error:"+comment_id)