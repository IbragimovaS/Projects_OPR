import os
import googleapiclient.discovery
import threading
import ibm_db_dbi as db
import datetime
import requests
def telegram_bot_sendtext(bot_message):
    bot_token = '941100121:AAEvfKUcdC5UBuOUx4Dg1RpsRjKuQgOt5Uw'
    bot_chatID = '579446756'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()
globalCount = 1
# ListOfChannel_id = [
#     'UCWFzKtHroFDAZNuVcJqQy_g',  # 1
#     'UCh1q7QQgxxpFqA837SIRYVg',  # 2
#     'UCgKkLbGudgVvz2hmZzjGwHw',  # 3
#     'UCtBvGNMVYZIoDJDQkRglpLQ',  # 4
#     'UCaWq7HX4HuHwttTrDXqqZuw',  # 5
#     'UCHD1GXdFq7jifjSQZUMsyaw',  # 6
#     'UCQHwGars89v0myZCFGOnT_Q',  # 7
#     'UCbICMg4VCIeQUHGw0Tolfug',  # 8
#     'UCpkc5yI-T6o69XKqTXiIIQg',  # 9
#     'UChs90eeFy865Q6bEVlyCo-w',  # 10
#     'UCxYuvvNubMpeY2SQE1MYnZw',  # 11
#     'UCjVGRx-focg86UePIRBH1iA',  # 12
#     'UCIlLaUQtcaabbw9-uQysRjw',  # 13
#     'UCNsmiVP_84KPtswCMNgz75A',  # 14
#     'UC6wZggQydceHuEwIT5GUgBA',  # 15
#     'UCG7gueoHzq06Lxn998Ib6GQ',  # 16
#     'UCiJ6RBthQvEPI4UsSNtM8jw',  # 17
#     'UCcsOAJgwReu4SFGKqdlDufg',  # 18
#     'UCUlcTEcMt1hVBZCzWemS-kw',  # 19
#     'UCIf2roNEhnjiT5etYrW5EOw',  # 20
#     'UCiknsLQvfySJZ0DSDo1W5jA',  # 21
#     'UCeCf8i63K_zrjdFM5IiW98Q',  # 22
#     'UCKg0oy4-scG8jYBwFbMaq5w',  # 23
#     'UCfkdQX-rX_fgXuqmp_oR6fQ',  # 24
#     'UCmmPjPXkctLf6m3W_UufHTQ',  # 25
#     'UCNBEmg0TKgGSNvN_7qqj6sw',  # 26
#     'UC1LqcwpbWhw79gTsYrA_mzA',  # 27
#     'UCpAR7WmDi9d1yRAJm7JQhDQ',  # 28
#     'UCaxLFsyo424jgM01zYLw3vA',  # 29
#     'UCf0jMvh9QJZrR3sKkyHDjNQ',  # 30
#     'UCc4_E46M_mZQkFSKASbBj7w',  # 31
#     'UC9vNs8VExCZSU5Etgvv7hnQ',  # 32
#     'UC_i-3xcn1yjwj7YHnF7qj1A',  # 33
#     'UCIj2ibh69CzUdvvfVN4o8NA',  # 34
#     'UChg155Cq5_IClm_Z43LEH-w',  # 35
#     'UCBgucog0WTa_rzPkDDjIuSw',  # 36
#     'UCR6jvyfLDG-T0JsFkqxvQdg',  # 37
#     'UC4c7PTqK2maTQNOezL5A0Wg'  # 38
# ]
#try:



ListOfApi_key = [
    'AIzaSyCyGnTZyHssr7_lFeHmm6ZrijRhPIAvZrM',  # 1
    'AIzaSyA5ZFCnncf31CNXTxuG1rx2nYZx0DdHXBg',  # 2
    'AIzaSyAudhbhzNBT9HO5rdEOtW4ADK6GphJSX2A',  # 3
    'AIzaSyCMvCtvPSVfmEtR2xaphDYUtsEJSGXIUwM',  # 4
    'AIzaSyBHLmkYpZJWgI7yKgy6QJtObCmflRTQO0U',  # 5
    'AIzaSyCz6y_t95i6-NPoLBkqdDfoTurpH3h1PcI',  # 6
    'AIzaSyDtAkGyeyMhEgacUCUpWd6Y-XuN3y1JSJQ',  # 7
    'AIzaSyCICtC2Y43pyL6YlrHiKsnrQiB5ibvn_XE',  # 8
    'AIzaSyDT0DXaz02PQBe6QQp5IBCg_2s2i0Mpsxs',  # 9
    'AIzaSyBd8H1U500j8CvjBV2q6cV6yZT1fMazRoY',  # 10
    'AIzaSyALZYADI7V0tx4pkOPOOeHtdsnPkx_L1Ug',  # 11
    'AIzaSyDEgTOkCKkMKN4VtZrvq4J7Gs_eJmzuG68',  # 12
    'AIzaSyAuFvBZFx6Ff7utDjnXPUn_KCrAKo9cBak',  # 13
    'AIzaSyB1UFPPJPfVkXP056YLesqxhBCZxdjzoxI',  # 14
    'AIzaSyDG4vc-BtEfeRjJZO2pO7fZY7uX_6sBQYU',  # 15
    'AIzaSyAcpf7gObejaQWM9o090craseViUV0SbtU',  # 16
    'AIzaSyCIf9xM0zi2FS6RMhOW30Mi75fhJQrdBlE',  # 17
    'AIzaSyBoaTvD1zOfYWNIYNR3Lrf2XOBKNHHmDrc',  # 18
    'AIzaSyDT1vxsrsOtSM51MwSKqahjDBGWgSpNcPM',  # 19
    'AIzaSyDlamNguiQsBH8WQ5bNTVcE9vYHBVPj_ro',  # 20
    'AIzaSyDy4OxGmZue7VZyo-tYearyU7Vj_TWUL7o',  # 21
    'AIzaSyBnDCsqZVEIe56MFnaSN85YoQMWnd_fEBo',  # 22
    'AIzaSyBVzVfpkDhIPxT6WUmYnZgNgPVKIFSu-5k',  # 23
    'AIzaSyDS9z1Ry3oypMf7sNywsnePbKiiLvKw_6A',  # 24
    'AIzaSyB5r0_bejIlnKJO6YObcewPj4ABkukc5-E',  # 25
    'AIzaSyBIdfva7sY9smYoa3fG9lWn8yuyvnSgsS8',  # 26
    'AIzaSyAMeUIvaBRJuEQGZpzE85G5m8tHXxKuGg4',  # 27
    'AIzaSyAEI8XIpUqo-0yyN0qwoJo7gpfFkn5ioOs',  # 28
    'AIzaSyBlrdp_fXA8WYdJ7hOLOOJ1VEe73rCWNuY',  # 29
    'AIzaSyAjuPJLkzPtwmsxjQYzQR8oE6BDULQtiYY',  # 30
    'AIzaSyDMqC-Z6LTSLNJC_9YJevYf4ZwNARJvfyk',  # 31
    'AIzaSyALyCYzqpDp-yNEWyeH7Ikec2Xc3FYs9co',  # 32
    'AIzaSyCLgB-BNUipx6Ha2bk1PULHHBZ6TgD8B04',  # 33
    'AIzaSyA4GO1paSIz94z3ivwQj_PSNgPSjVUP2rQ',  # 34
    'AIzaSyBn7zCAI0g1mTuj40P3H2Qfw3pjGl7gYzM',  # 35
    'AIzaSyAL3q8p87sa-9ApYqz8qV1yvzDXoPHdcyo',  # 36
    'AIzaSyA0eGWU5aOEt5l_na80teC2OpV5xE4S1ao',  # 37
    'AIzaSyBQgOPwWrpZjXK2Iv-l1_HSulTwP_sbbOE',  # 38
    'AIzaSyD44DpT3fvlgaW7p7UUKr4U1MSm8kCVbfI',  # 39
    'AIzaSyCFUCJcx2yzt6qBQwVrHODbIFpg1ZX6WPo',  # 40
    'AIzaSyB5Vq4d9birPT3AC4Mu7JyBFJ4cGUM0W9k',  # 41
    'AIzaSyAIP-9Sn8yZw7jW8Q5Hqj_KdNk9Q9DuwUE',  # 42
    'AIzaSyDe5ffF8Zx30MTdO6LB41LVD0v4b-ZdPLQ',  # 43
    'AIzaSyDj92VUuhacKatJ3NOd3CpsVWb8YgnQQ_0',  # 44
    'AIzaSyCGQeCUeyeq1QbhWIS8QiEmOd9Oa3KPEOE',  # 45
    'AIzaSyCoZIaKRzVtlOzR-kexKNdGv6bNgN0fOj4',  # 46
    'AIzaSyDSmBDUSTNQHE7h7jECMyfEtfrTldkRcOQ',  # 47
    'AIzaSyDb6Ttjz09cKJbnGyDjE-5WUBgwTP4X_3Y',  # 48
    'AIzaSyBGbOklb35AZHkkyiPaGuUWOBS_cAeAsuI',  # 49
    'AIzaSyBIdPdRkzFL6GS5sDG1ptW63SizJDzereo',  # 50
    'AIzaSyBH-HSacz6DwRJO7QfqnuQceYGgon3QHK4',  # 51
    'AIzaSyCu4tbFI1hx0om-8Q59-vz3GQpg6IqADHs',  # 52
    'AIzaSyBwUP5o_qfDUswQnhxbdCVeaxSG-TrFyQk',  # 53
    'AIzaSyB_kC99iXGG6CvdlNfd4PFmRpuzVHmxBew',  # 54
    'AIzaSyD1M4CKTrV7VFN64ernplHo23SAIsZITFA',  # 55
    'AIzaSyAZQpvm_o7bEAS3i1ij9JOhLD4-tpoVMJ8',  # 56
    'AIzaSyD7rNGrwSaetO_Lg89C_RSQGzA-jmtGxcU'  # 57
]
new_list_chanel = [
    'UC__llOdR9FL15eP79tn7ohA',

    'UCDZ1slP9LRYWKGnVACKlNOQ',

    'UCrAKtg2hRjjAEvSKJ0h2VUg',

    'UClmwE5HsBUxYwRSfRQgNr3Q',

    'UCNvi6cgHzpUBUIhKAUwZSug',

    'UCKUnyoRQCbElZpqgXWVhdKQ',

    'UCdZU0ZphvtIxu6yEmvIugOw',

    'UCV-Dw6HbAx1OVRtBV2uJgNw',

    'UCSIdlHWNFKLNOQ_dsXkgGJg',

    'UCvrkZkCnrNQgINNRByO6R-A',
    'UC__llOdR9FL15eP79tn7ohA',
    'UCNvi6cgHzpUBUIhKAUwZSug'
]
while True:
    #bot_message = "posts start"
    #telegram_bot_sendtext(bot_message)
    connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
    con = db.connect(connection_text, "", "")
    cursor = con.cursor()
    sql_1_test = "SELECT channel_id from tl_media_channels where source_id = 3"  # +str(channel)+""
    cursor.execute(
        sql_1_test)  # id_, date_time_new[0].replace(","," "),channel, None, None, int(views),None,None, str(abc), str(lohc),kanal[channel],5))
    list1 = cursor.fetchall()
    con.commit()
    cursor.close()
    con.close()
    ListOfChannel_id = []
    for i in range(len(list1)):
        ListOfChannel_id.append(list1[i][0])
    for i in range(len(new_list_chanel)):
         ListOfChannel_id.append(new_list_chanel[i])
    for q in range(len(ListOfChannel_id)):
        listIDVideos = []

        connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
        con = db.connect(connection_text, "", "")
        cursor = con.cursor()
        sql_1_test = "SELECT OBJECT_ID from tl_media_data_yt"  # +str(channel)+""
        cursor.execute(
            sql_1_test)  # id_, date_time_new[0].replace(","," "),channel, None, None, int(views),None,None, str(abc), str(lohc),kanal[channel],5))
        fromdbVid = cursor.fetchall()
        con.commit()
        cursor.close()
        con.close()
        fromdbVideoId = []
        for e in range(len(fromdbVid)):
            fromdbVideoId.append(fromdbVid[e][0])

        print("Start channel: " + str(q))
        for w in range(len(ListOfApi_key)):
            try:
                api_key = ListOfApi_key[w]
                channel_id = ListOfChannel_id[q]
                coun = 1
                os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
                api_service_name = "youtube"
                api_version = "v3"
                DEVELOPER_KEY = api_key

                youtube = googleapiclient.discovery.build(
                    api_service_name, api_version, developerKey=DEVELOPER_KEY)

                request = youtube.search().list(
                    part="snippet,id",
                    channelId=channel_id,
                    maxResults=50,
                    order="date"
                )
                response = request.execute()
                break
            except:
                print("Limit: " + str(w))
        for item in response['items']:
            try:
                videoID = item["id"]["videoId"]
                print(str(coun) + ": " + videoID)
                coun += 1
                listIDVideos.append(videoID)
                print("_______________")
            except KeyError:
                print("")
        booll = 3
        try:
            nextPageToken = response['nextPageToken']
            bolll = 1
        except:
            booll = 0
        if booll == 1:
            while True:
                for w in range(len(ListOfApi_key)):
                    try:
                        api_key = ListOfApi_key[w]
                        channel_id = ListOfChannel_id[q]
                        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
                        api_service_name = "youtube"
                        api_version = "v3"
                        DEVELOPER_KEY = api_key

                        youtube = googleapiclient.discovery.build(
                            api_service_name, api_version, developerKey=DEVELOPER_KEY)

                        request = youtube.search().list(
                            part="snippet,id",
                            channelId=channel_id,
                            maxResults=50,
                            order="date",
                            pageToken=nextPageToken
                        )
                        response = request.execute()
                        break
                    except:
                        print("Limit n : " + str(w))
                for item in response['items']:
                    try:
                        videoID = item["id"]["videoId"]
                        print(videoID)
                        print(coun)
                        coun += 1
                        listIDVideos.append(videoID)
                        print("_______________")
                    except KeyError:
                        print("")
                try:
                    nextPageToken = response['nextPageToken']
                except:
                    break
        for r in range(len(listIDVideos)):
            if listIDVideos[r] in fromdbVideoId:
                print('start video ' + listIDVideos[r] + " " + str(r))
                for w in range(len(ListOfApi_key)):
                    try:
                        api_key = ListOfApi_key[w]
                        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
                        api_service_name = "youtube"
                        api_version = "v3"
                        DEVELOPER_KEY = api_key
                        youtube = googleapiclient.discovery.build(
                            api_service_name, api_version, developerKey=DEVELOPER_KEY)
                        request = youtube.videos().list(
                            part="statistics",
                            id=listIDVideos[r]
                        )
                        response = request.execute()
                        break
                    except:
                        print("Limit update: " + listIDVideos[r] + " " + str(w))

                viewCount = response["items"][0]['statistics']['viewCount']
                try:
                    likeCount = response["items"][0]['statistics']['likeCount']
                except:
                    likeCount='0'
                try:
                    commentCount = response["items"][0]['statistics']['commentCount']
                except:
                    commentCount='0'
                print("viewCount: " + viewCount)
                print("likeCount: " + likeCount)
                print('commentCount: ' + commentCount)
                connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
                con = db.connect(connection_text, "", "")
                cursor = con.cursor()
                sql_1_test = "update tl_media_data_yt set COMMENTS=" + commentCount + ", LIKES=" + likeCount + ", VIEWS=" + viewCount + " where OBJECT_ID='" + listIDVideos[r] + "'"
                cursor.execute(sql_1_test)
                con.commit()
                cursor.close()
                con.close()
                print(
                    "update: " + listIDVideos[r] + " -" + str(r) + "/" + str(len(listIDVideos)) + " and " + ListOfChannel_id[
                        q] + str(q) + '/' + str(len(ListOfChannel_id)))
            if not listIDVideos[r] in fromdbVideoId:
                print(
                    "insert: " + listIDVideos[r] + " -" + str(r) + "/" + str(len(listIDVideos)) + " and " + ListOfChannel_id[
                        q] + str(q) + '/' + str(len(ListOfChannel_id)))
                for w in range(len(ListOfApi_key)):
                    try:
                        api_key = ListOfApi_key[w]
                        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
                        api_service_name = "youtube"
                        api_version = "v3"
                        DEVELOPER_KEY = api_key
                        youtube = googleapiclient.discovery.build(
                            api_service_name, api_version, developerKey=DEVELOPER_KEY)
                        request = youtube.videos().list(
                            part="statistics,snippet",
                            id=listIDVideos[r]
                        )
                        response = request.execute()
                    except:
                        print("Limit insert: " + listIDVideos[r] + " " + str(w))

                OBJECT_ID = listIDVideos[r]  # 1
                # 2
                dat = response["items"][0]['snippet']['publishedAt']
                h = int(dat.split("T")[1].split(".")[0].split(":")[0]) + 6
                if h >= 24:
                    ho = h - 24
                else:
                    ho = h
                hou = str(ho)
                PUBLISHED_DATE = dat.split("T")[0] + " " + hou + ":" + dat.split("T")[1].split(".")[0].split(":")[1] + ":" + \
                                 dat.split("T")[1].split(".")[0].split(":")[2]
                CHANNEL_ID = response["items"][0]['snippet']['channelId']  # 3
                LIKES = response["items"][0]['statistics']['likeCount']  # 4
                try:
                    COMMENTS = response["items"][0]['statistics']['commentCount']  # 5
                except:
                    COMMENTS='0'

                VIEWS = response["items"][0]['statistics']['viewCount']  # 6
                TEXT = response["items"][0]['snippet']['title'] + " " + "\n" + response["items"][0]['snippet'][
                    'description']  # 7
                URL_CHANNEL = 'https://www.youtube.com/channel/' + CHANNEL_ID  # 8
                SOURCE_ID = '3'  # 9
                connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
                con = db.connect(connection_text, "", "")
                cursor = con.cursor()
                sql_1_test = "insert into tl_media_data_yt (OBJECT_ID, PUBLISHED_DATE, CHANNEL_ID, LIKES, COMMENTS, VIEWS, TEXT,URL_CHANNEL,SOURCE_ID) values (?,?,?,?,?,?,?,?,?)"
                cursor.execute(sql_1_test, (
                    OBJECT_ID, PUBLISHED_DATE, CHANNEL_ID, LIKES, COMMENTS, VIEWS, TEXT, URL_CHANNEL, SOURCE_ID))
                con.commit()
                cursor.close()
                con.close()
                print("OBJECT_ID: " + OBJECT_ID)  # 1
                print("PUBLISHED_DATE: " + PUBLISHED_DATE)  # 2
                print("CHANNEL_ID: " + CHANNEL_ID)  # 3
                print("LIKES: " + LIKES)  # 4
                print("COMMENTS: " + COMMENTS)  # 5
                print("VIEWS: " + VIEWS)# 6
                print("TEXT: \n" + TEXT)# 7
                print('URL_CHANNEL: ' + URL_CHANNEL)# 8
                print('SOURCE_ID: ' + SOURCE_ID)# 9
# except:
#     bot_message = "The youtube data was broken"
#     telegram_bot_sendtext(bot_message)