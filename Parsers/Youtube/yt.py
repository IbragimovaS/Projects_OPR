# -*- coding: utf-8 -*-
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

    'UCvrkZkCnrNQgINNRByO6R-A'

]

while True:

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

    a = datetime.datetime.today().strftime("%M")
    if int(a) % 1 == 0:
        connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
        con = db.connect(connection_text, "", "")
        cursor = con.cursor()
        sql_1_test = "SELECT COMMENT_ID from tl_media_comments_yt"  # +str(channel)+""
        cursor.execute(
            sql_1_test)  # id_, date_time_new[0].replace(","," "),channel, None, None, int(views),None,None, str(abc), str(lohc),kanal[channel],5))
        commID = cursor.fetchall()
        con.commit()
        cursor.close()
        con.close()
        commID1 = []
        for t in range(len(commID)):
            commID1.append(commID[t][0])
        print("GET ID")
        for q in range(len(ListOfChannel_id)):
            print("start channel " + str(q))
            for w in range(len(ListOfApi_key)):
                try:
                    print("try 1")
                    api_key = ListOfApi_key[w]
                    channel_Id = ListOfChannel_id[-q]
                    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
                    api_service_name = "youtube"
                    api_version = "v3"
                    DEVELOPER_KEY = api_key

                    youtube = googleapiclient.discovery.build(
                        api_service_name, api_version, developerKey=DEVELOPER_KEY)
                    request = youtube.commentThreads().list(
                        part="snippet,replies",
                        allThreadsRelatedToChannelId=channel_Id,
                        maxResults=100,
                        order="time"
                    )
                    response = request.execute()
                    break
                except:  # HttpError:
                    print('except 1')
                    print("Limit " + ListOfApi_key[w] + " " + str(w))
            print("GET response")
            for item in response['items']:
                COMMENT_ID = item['snippet']['topLevelComment']['id']  #
                try:
                    print("try 2")
                    OBJECT_ID = item['snippet']['videoId']  # 2
                except:
                    print('except 2')
                    OBJECT_ID = Catch
                Catch = OBJECT_ID

                dat = item["snippet"]['topLevelComment']['snippet']['updatedAt'].split('.')[0].replace("T", " ") + ".00"
                h = int(dat.split(" ")[1].split(":")[0]) + 6
                if h >= 24:
                    h = h - 24
                if h < 24:
                    ho = str(h)
                if int(ho) < 10:
                    ho = "0" + ho
                # 3
                PUBLISHED_DATE = dat.split(" ")[0] + " " + str(ho) + ":" + dat.split(" ")[1].split(":")[1] + ":" + \
                                 dat.split(" ")[1].split(":")[2]
                COMMENT_LIKES = item["snippet"]['topLevelComment']["snippet"]['likeCount']  # 4
                COMMENT_TEXT = item["snippet"]['topLevelComment']['snippet']['textOriginal']  # 5
                AUTHOR_ID = ""  # 6
                AUTHOR_NAME = item['snippet']['topLevelComment']['snippet']['authorDisplayName']  # 7
                try:
                    print("try 3")
                    AUTHOR_URL = item["snippet"]['topLevelComment']['snippet']['authorChannelUrl'].split("/channel/")[
                        1]  # 8
                except IndexError:
                    print('except 3')
                    AUTHOR_URL = item["snippet"]['topLevelComment']['snippet']['authorChannelUrl']
                PARENT_ID = ""  # 9
                SOURCE_ID = "3"  # 10
                channelId = item['snippet']['channelId']

                print("PUBLISHED_DATE: " + PUBLISHED_DATE)  # 1
                if not COMMENT_ID in commID1:
                    try:
                        print("try 4")
                        connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
                        con = db.connect(connection_text, "", "")
                        cursor = con.cursor()
                        sql_1_test = "insert into tl_media_comments_yt (comment_id, object_id, published_date, comment_likes, comment_text, author_id,author_name,author_url, parent_id,SOURCE_ID) values (?,?,?,?,?,?,?,?,?,?)"
                        cursor.execute(sql_1_test, (
                        COMMENT_ID, OBJECT_ID, PUBLISHED_DATE, COMMENT_LIKES, COMMENT_TEXT, AUTHOR_ID, AUTHOR_NAME,
                        AUTHOR_URL, PARENT_ID, SOURCE_ID))
                        con.commit()
                        cursor.close()
                    except:
                        print('except 4')
                        connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
                        con = db.connect(connection_text, "", "")
                        cursor = con.cursor()
                        sql_1_test = "insert into tl_media_comments_yt (comment_id, object_id, published_date, comment_likes, comment_text, author_id,author_name,author_url, parent_id,SOURCE_ID) values (?,?,?,?,?,?,?,?,?,?)"
                        cursor.execute(sql_1_test, (
                            COMMENT_ID, OBJECT_ID, PUBLISHED_DATE, COMMENT_LIKES, COMMENT_TEXT, AUTHOR_ID, AUTHOR_NAME,
                            AUTHOR_URL[0],
                            PARENT_ID, SOURCE_ID))
                        con.commit()
                        cursor.close()
                        con.close()
                    print("COMMENT_ID: " + COMMENT_ID)  # 1
                    print("OBJECT_ID: " + OBJECT_ID)  # 2
                    print("PUBLISHED_DATE: " + PUBLISHED_DATE)  # 3
                    print("COMMENT_LIKES: " + str(COMMENT_LIKES))  # 4
                    print("COMMENT_TEXT:" + COMMENT_TEXT)  # 5
                    print("AUTHOR_ID: " + "")  # 6
                    print("AUTHOR_NAME: " + AUTHOR_NAME)  # 7

                    print("PARENT_ID: " + PARENT_ID)  # 9
                    print("SOURCE_ID:" + SOURCE_ID)  # 10
                try:
                    print("try 5")
                    print("REPLiES:" + str(len(item['replies']['comments'])))
                    for i in range(len(item['replies']['comments'])):
                        print("- - - - - - - - - - - - - - - - - - - -")
                        COMMENT_ID = item['replies']['comments'][i]['id']  # 1
                        OBJECT_ID = item['replies']['comments'][i]['snippet']['videoId']  # 2
                        r_dat = item['replies']['comments'][i]['snippet']['publishedAt'].split('.')[0].replace("T",
                                                                                                               " ") + ".00"
                        r_h = int(r_dat.split(" ")[1].split(":")[0]) + 6
                        if r_h >= 24:
                            r_h = r_h - 24
                        if r_h < 24:
                            r_ho = str(r_h)
                        if int(r_ho) < 10:
                            r_ho = "0" + r_ho
                        # 3
                        PUBLISHED_DATE = r_dat.split(" ")[0] + " " + str(r_ho) + ":" + r_dat.split(" ")[1].split(":")[
                            1] + ":" + r_dat.split(" ")[1].split(":")[2]
                        COMMENT_LIKES = item['replies']['comments'][i]['snippet']['likeCount']  # 4
                        COMMENT_TEXT = item['replies']['comments'][i]['snippet']['textOriginal']  # 5
                        AUTHOR_ID = ""  # 6
                        AUTHOR_NAME = item['replies']['comments'][i]['snippet']['authorDisplayName']  # 7
                        AUTHOR_URL = item['replies']['comments'][i]['snippet']['authorChannelUrl'].split("/channel/")[
                            1]  # 8
                        PARENT_ID = item['replies']['comments'][i]['snippet']['parentId']  # 9
                        SOURCE_ID = "3"  # 10

                        print("R.PUBLISHED_DATE " + str(i) + ": " + PUBLISHED_DATE)  # 1
                        if not COMMENT_ID in commID1:
                            connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
                            con = db.connect(connection_text, "", "")
                            cursor = con.cursor()
                            sql_1_test = "insert into tl_media_comments_yt (comment_id, object_id, published_date, comment_likes, comment_text, author_id,author_name,author_url, parent_id,SOURCE_ID) values (?,?,?,?,?,?,?,?,?,?)"
                            cursor.execute(sql_1_test, (
                                COMMENT_ID, OBJECT_ID, PUBLISHED_DATE, COMMENT_LIKES, COMMENT_TEXT, AUTHOR_ID,
                                AUTHOR_NAME,
                                AUTHOR_URL, PARENT_ID, SOURCE_ID))
                            con.commit()
                            cursor.close()
                            con.close()
                            print("R.COMMENT_ID " + str(i) + ": " + COMMENT_ID)  # 1
                            print("R.OBJECT_ID " + str(i) + ": " + OBJECT_ID)  # 2
                            print("R.PUBLISHED_DATE " + str(i) + ": " + PUBLISHED_DATE)  # 3
                            print("R.COMMENT_LIKES " + str(i) + ": " + str(COMMENT_LIKES))  # 4
                            print("R.COMMENT_TEXT " + str(i) + ": " + COMMENT_TEXT)  # 5
                            print("R.AUTHOR_ID " + str(i) + ": " + "None")  # 6
                            print("R.AUTHOR_NAME " + str(i) + ": " + AUTHOR_NAME)  # 7
                            print("R.PARENT_ID " + str(i) + ": " + PARENT_ID)  # 9
                            print("R.SOURCE_ID " + str(i) + ": " + SOURCE_ID)  # 10
                except KeyError:
                    print('except 5')
                    pass
                print("__________________________________")
            # ------------------------------------------------------------------------------------------------------------------------------
            for y in range(10):
                try:
                    print("try 6")
                    nextPageToken = response['nextPageToken']
                except:
                    print('except 6')
                    break
                for w in range(len(ListOfApi_key)):
                    try:
                        print("try 7")
                        api_key = ListOfApi_key[w]
                        channel_Id = ListOfChannel_id[-q]
                        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
                        api_service_name = "youtube"
                        api_version = "v3"
                        DEVELOPER_KEY = api_key

                        youtube = googleapiclient.discovery.build(
                            api_service_name, api_version, developerKey=DEVELOPER_KEY)

                        request = youtube.commentThreads().list(
                            part="snippet,replies",
                            allThreadsRelatedToChannelId=channel_Id,
                            maxResults=100,
                            order="time",
                            pageToken=nextPageToken
                        )
                        response = request.execute()
                        break
                    except:
                        print('except 7')
                        print("Limit " + ListOfApi_key[w] + " " + str(w))
                for item in response['items']:
                    COMMENT_ID = item['snippet']['topLevelComment']['id']  # 1
                    try:
                        print("try 8")
                        OBJECT_ID = item['snippet']['videoId']  # 2
                    except KeyError:
                        try:
                            print("try 9")
                            OBJECT_ID = item['replies']['comments'][0]['snippet']['videoId']
                        except:
                            print('except 9')
                            OBJECT_ID = OBJECT_ID1
                    dat = item["snippet"]['topLevelComment']['snippet']['updatedAt'].split('.')[0].replace("T",
                                                                                                           " ") + ".00"
                    h = int(dat.split(" ")[1].split(":")[0]) + 6
                    if h >= 24:
                        h = h - 24
                    if h < 24:
                        ho = str(h)
                    if int(ho) < 10:
                        ho = "0" + ho
                    # 3
                    PUBLISHED_DATE = dat.split(" ")[0] + " " + str(ho) + ":" + dat.split(" ")[1].split(":")[1] + ":" + \
                                     dat.split(" ")[1].split(":")[2]
                    COMMENT_LIKES = item["snippet"]['topLevelComment']["snippet"]['likeCount']  # 4
                    COMMENT_TEXT = item["snippet"]['topLevelComment']['snippet']['textOriginal']  # 5
                    AUTHOR_ID = ""  # 6
                    AUTHOR_NAME = item['snippet']['topLevelComment']['snippet']['authorDisplayName']  # 7
                    try:
                        print("try 10")
                        AUTHOR_URL = \
                        item["snippet"]['topLevelComment']['snippet']['authorChannelUrl'].split("/channel/")[1]  # 8
                    except IndexError:
                        print('except 10')
                        AUTHOR_URL = item["snippet"]['topLevelComment']['snippet']['authorChannelUrl']
                    PARENT_ID = ""  # 9
                    SOURCE_ID = "3"  # 10
                    print("PUBLISHED_DATE: " + PUBLISHED_DATE)
                    if not COMMENT_ID in commID1:
                        try:
                            print("try 11")
                            connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
                            con = db.connect(connection_text, "", "")
                            cursor = con.cursor()
                            sql_1_test = "insert into tl_media_comments_yt (comment_id, object_id, published_date, comment_likes, comment_text, author_id,author_name,author_url, parent_id,SOURCE_ID) values (?,?,?,?,?,?,?,?,?,?)"
                            cursor.execute(sql_1_test, (
                                COMMENT_ID, OBJECT_ID, PUBLISHED_DATE, COMMENT_LIKES, COMMENT_TEXT, AUTHOR_ID,
                                AUTHOR_NAME,
                                AUTHOR_URL,
                                PARENT_ID, SOURCE_ID))
                            con.commit()
                            cursor.close()
                            con.close()
                        except:
                            connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
                            con = db.connect(connection_text, "", "")
                            cursor = con.cursor()
                            sql_1_test = "insert into tl_media_comments_yt (comment_id, object_id, published_date, comment_likes, comment_text, author_id,author_name,author_url, parent_id,SOURCE_ID) values (?,?,?,?,?,?,?,?,?,?)"
                            cursor.execute(sql_1_test, (
                                COMMENT_ID, OBJECT_ID, PUBLISHED_DATE, COMMENT_LIKES, COMMENT_TEXT, AUTHOR_ID,
                                AUTHOR_NAME,
                                AUTHOR_URL[0],
                                PARENT_ID, SOURCE_ID))
                            con.commit()
                            cursor.close()
                        print("COMMENT_ID: " + COMMENT_ID)  # 1
                        print("OBJECT_ID: " + OBJECT_ID)  # 2
                        print("PUBLISHED_DATE: " + PUBLISHED_DATE)  # 3
                        print("COMMENT_LIKES: " + str(COMMENT_LIKES))  # 4
                        print("COMMENT_TEXT:" + COMMENT_TEXT)  # 5
                        print("AUTHOR_ID: " + "")  # 6
                        print("AUTHOR_NAME: " + AUTHOR_NAME)  # 7
                        print("PARENT_ID: " + PARENT_ID)  # 9
                        print("SOURCE_ID:" + SOURCE_ID)  # 10

                try:
                    print("REPLiES:" + str(len(item['replies']['comments'])))
                    for i in range(len(item['replies']['comments'])):
                        print("- - - - - - - - - - - - - - - - - - - -")
                        COMMENT_ID = item['replies']['comments'][i]['id']  # 1
                        OBJECT_ID = item['replies']['comments'][i]['snippet']['videoId']  # 2
                        r_dat = item['replies']['comments'][i]['snippet']['publishedAt'].split('.')[0].replace("T",
                                                                                                               " ") + ".00"
                        r_h = int(r_dat.split(" ")[1].split(":")[0]) + 6
                        if r_h >= 24:
                            r_h = r_h - 24
                        if r_h < 24:
                            r_ho = str(r_h)
                        if int(r_ho) < 10:
                            r_ho = "0" + r_ho
                        # 3
                        PUBLISHED_DATE = r_dat.split(" ")[0] + " " + str(r_ho) + ":" + r_dat.split(" ")[1].split(":")[
                            1] + ":" + r_dat.split(" ")[1].split(":")[2]
                        COMMENT_LIKES = item['replies']['comments'][i]['snippet']['likeCount']  # 4
                        COMMENT_TEXT = item['replies']['comments'][i]['snippet']['textOriginal']  # 5
                        AUTHOR_ID = ""  # 6
                        AUTHOR_NAME = item['replies']['comments'][i]['snippet']['authorDisplayName']  # 7
                        AUTHOR_URL = item['replies']['comments'][i]['snippet']['authorChannelUrl'].split("/channel/")[
                            1]  # 8
                        PARENT_ID = item['replies']['comments'][i]['snippet']['parentId']  # 9
                        SOURCE_ID = "3"  # 10
                        print("R.PUBLISHED_DATE " + str(i) + ": " + PUBLISHED_DATE)
                        if not COMMENT_ID in commID1:
                            connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
                            con = db.connect(connection_text, "", "")
                            cursor = con.cursor()
                            sql_1_test = "insert into tl_media_comments_yt (comment_id, object_id, published_date, comment_likes, comment_text, author_id,author_name,author_url, parent_id,SOURCE_ID) values (?,?,?,?,?,?,?,?,?,?)"
                            cursor.execute(sql_1_test, (
                                COMMENT_ID, OBJECT_ID, PUBLISHED_DATE, COMMENT_LIKES, COMMENT_TEXT, AUTHOR_ID,
                                AUTHOR_NAME,
                                AUTHOR_URL, PARENT_ID, SOURCE_ID))
                            con.commit()
                            cursor.close()
                            con.close()
                            print("R.COMMENT_ID " + str(i) + ": " + COMMENT_ID)  # 1
                            print("R.OBJECT_ID " + str(i) + ": " + OBJECT_ID)  # 2
                            print("R.PUBLISHED_DATE " + str(i) + ": " + PUBLISHED_DATE)  # 3
                            print("R.COMMENT_LIKES " + str(i) + ": " + str(COMMENT_LIKES))  # 4
                            print("R.COMMENT_TEXT " + str(i) + ": " + COMMENT_TEXT)  # 5
                            print("R.AUTHOR_ID " + str(i) + ": " + "None")  # 6
                            print("R.AUTHOR_NAME " + str(i) + ": " + AUTHOR_NAME)  # 7
                            print("R.AUTHOR_URL " + str(i) + ": " + AUTHOR_URL)  # 8
                            print("R.PARENT_ID " + str(i) + ": " + PARENT_ID)  # 9
                            print("R.SOURCE_ID " + str(i) + ": " + SOURCE_ID)  # 10
                        OBJECT_ID1 = OBJECT_ID
                except KeyError:
                    pass
                print("__________________________________")

                print("nextPageToken:" + str(y) + "--" + nextPageToken)
            print("End channel " + str(q))
        print("Iter end")
    else:
        pass