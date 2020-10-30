# -*- coding: utf-8 -*-
import requests
import json
import time
import datetime
import csv
import ibm_db_dbi as db

tag = 'ny'
url = 'http://www.instagram.com/explore/tags/' + tag + '/?__a=1'
url_for_each_post = 'https://www.instagram.com/p/'
headers = {
    'Host': 'www.instagram.com',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Proxy-Authorization': 'Basic YWNjZXNzX3Rva2VuOjFhMGRsaGhsOGpscmsydm9xMTE5OThsaGczcXVuZmY1a3A5cGpvNXRpdWM1bXNvN2UzN2g=',
    'Connection': 'keep-alive',
    'Cookie': 'mid=XK3zaAAEAAEI6CmrmHhJ9RY2nQw0; shbid=10649; shbts=1557340710.4067957; csrftoken=UTk15npDOCSdWXW80JMTmCUHn6D2jd2O; ds_user_id=13127050833; sessionid=13127050833%3AsTNSSL99Kn5G7u%3A5; rur=ASH; urlgen="{\"207.189.31.197\": 53889\054 \"72.35.247.135\": 53889\054 \"138.197.139.130\": 14061}:1hOf2W:ZiTBtBG2atuUvTv_5n6zwaMs8oE"',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0',
    'TE': 'Trailers'
}

keys = ['id', 'owner', 'display_url', 'edge_liked_by', 'edge_media_to_comment', 'shortcode', 'edge_media_to_caption',
        'accessibility_caption', 'is_video', 'text']

def telegram_bot_sendtext(bot_message):
    bot_token = '873733574:AAFmyXY_cvkErr9YpYNw7Qf3Y87d1Sjpgg4'
    bot_chatID = '91344390'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()

def jprint(data_dict):
    print(json.dumps(data_dict, indent=4))


def get_id_page(url, headers, session=None):
    print(url)
    session = session or requests.Session()

    r = session.get(url, headers=headers)
    r_code = r.status_code
    # print(r)
    print(r_code)
    if r_code == requests.codes.ok:
        # the code is 200 or valid
        return r
    else:
        return None


def get_id_for_blog(url, headers, session=None):
    print(url)
    session = session or requests.Session()

    r = session.get(url, headers=headers)
    r_code = r.status_code
    # print(r)
    print(r_code)
    if r_code == requests.codes.ok:
        # the code is 200 or valid
        return r.json()
    else:
        return None

def get_blog_data(data, type):
    connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
    con = db.connect(connection_text, "", "")
    cursor = con.cursor()
    try:
        if data is not None:
            time.sleep(0.7)
            inner_data = data.get('graphql', None)
            inner_data = inner_data.get('user', None)

            biography = inner_data['biography']
            number_of_followers = inner_data['edge_followed_by']['count']
            full_name = inner_data['full_name']
            channel_id = inner_data['id']
            channel_name = inner_data['username']
            posts_count = inner_data['edge_owner_to_timeline_media']['count']
            inserted_date = datetime.datetime.now()
            general_info = {
                'biography': biography,
                'number_of_followers': number_of_followers,
                'full_name': full_name,
                'channel_id': channel_id,
                'channel_name': channel_name,
                'posts_count': posts_count
            }



            #connection_text = "DATABASE=PRODDB;HOSTNAME=192.168.252.11;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Qjuehnghj1;"
            #con = db.connect(connection_text, "", "")
            #cursor = con.cursor()

            cursor.execute('select channel_id from tl_media_channels where channel_id = \'{channel_id}\''.format(channel_id=channel_id))

            one_row = cursor.fetchone()
            print(one_row)
            if one_row is not None:
                print('Update {channel_id} channel.'.format(channel_id=channel_id))

                sql_update = "update tl_media_channels set posts_count={posts_count}, number_of_followers={number_of_followers} where channel_id = \'{channel_id}\'".format(
                    posts_count=posts_count, number_of_followers=number_of_followers, channel_id=channel_id)

                sql_history = "insert into tl_media_channels_history (inserted_date, channel_id, channel_name, biography, full_name, number_of_followers, posts_count, source_id) values (?,?,?,?,?,?,?,?)"

                cursor.execute(sql_history, (inserted_date, channel_id, channel_name, biography, full_name, number_of_followers, posts_count, type))

                cursor.execute(sql_update)

            else:

                print('Insert ', channel_id, 'object.')

                sql = "insert into tl_media_channels (channel_id, channel_name, biography, full_name, number_of_followers, posts_count, source_id) values (?,?,?,?,?,?,?)"

                cursor.execute(sql, (channel_id, channel_name, biography, full_name, number_of_followers, posts_count, type))


            con.commit()
    except:
        print("User not found")

    cursor.close()
    con.close()

blogs_positive = {
'rud_news',
'aksu.ermak',
'hromtau_akimat',

'alibekovkz',
'yelikbayev',
'kudaibergen_kairat',
'yrashev',
'kyran_talapbek',
'larion_lyan',
'dinarasatzhan',
'maxim_sarsenov',
'ztb_kz',
'ztb_video',
'kris.p.almaty',
'dudoser',
'cars_kz',
'kris_p_brothers_official',
'yedilovonline',
'orkeni',
'margulan_seissembai',
'kuantr',
'beisekeyev',
'maricherie',
'sultankyzy',
'kaztwitter',
'qazaqstan_eli',
#positive
    'salemvines',
    'salemvines_kaz',
    'salem_standup',
    'salem.beauty',
    'showsalem',
    'salemsocial',
    'qpopsalem',
    'salem_park',
    # 'salem_shabyt',
    'salem_cyberdogs',
    # 'aitysg',
    'dop_kz',
    'instavideo_kz',
    'today_kz',
    'kris_p_almaty',
    '100baksoff',
    'autolife_almaty',
    'v.shymkente',
    'kaz_youtube',
    'kazakh_videoo',
    '_kyzyk_times',
    'kazakh__stars',
    'stars_kazak',
    'kazakhtoy',
    'what_semey',
    'search_aktobe',
    'po_dorogam_ukg',
    'vse_obo_vsem_kostanay',
    'what.uralsk',
    'what.aktobe',
    'kostanay_.kz',
    'semey_live',
    'nazaraudarsagyz',
    'playlist.kz',
    'shymkent_dudoser',
    'dudoser_a',
    'dudoser.kz',
    'v.shymkente.ok',
    'shymkent_news_',
    'shymkent.insta',
    'oskemen.today',
    'taraz_today',
    'tartylsai',
    'salem_sayahat',
    'aitys4g',
    'talklike_amantasygan',
    'dastarkhan_kaz',
    'prikol.kzz',
    'qjeri.kz',



    'kaztwitter',
    'insta_azil',
    'zello.aktobe',
    'eki_ezu',
    'instavideokz',
    'mampasy_live',
    'zhiza_kz',
    'qazaq_tv',
    'zhest_video',
    'qazaq_hype',
    'ayta_bersyn',
    'almaty___dubsmash',
    '100baksoff_',
    'instavideo_kaz',
    'krutye_agashki',
    'vseobovsem.pavlodar',
    'vseobovsem_karaganda',
    'vseobovsem_astana',
    'ztb_kz',
    'zhest.kz',
    'ztb_video',
    'spirit_of_tengri',
    'urankz',
    'ilyas_dzh',
    'lialina2004',
    'amantasygan8',
    'menenbarisuraid1',
    'millermariya',
    'eldana_foureyes',
    'khizmetov',
    'urazimanovkb',
    'azizvekimov',
    'aubakiremilbay',
    'brzh_',
    'veronkm',
    'damir_nurseitov',
    'nomadiyar',
    'kalibekov_medet',
    'zhass8',
    'nursultanaidarov',
    'nurmahype',
    'taras.kosimbarov',
    'alymkulova_nazgul',
    'anzor.borzaev',
    'kyran_tokesh',
    'nurbolkhan',
    'sanzhgally',
    'shakentii',
    'isekaroketa',
    'marlenkapbar',
    'sanjarlive',
    'ermek_kenensarov',
    'meirzhach_tv',
    'anarabatyrkhan',
    'kartop.tv',
    'sabirkin_',
    'ratbek',
    't_jokers',
    'hakimmukaram',
    'zheka_fatbelly',
    'jokeasses',
    'ahmad__helmi',
    'k_beksultan',
    'zhandos_t',
    'yuframe',
    'archibaaalt',
    'yussupov21',
    'aiymsm',
    'oksukpaevak',
    'zhanar_aizhanova_official',
    'vipzzal',
    'aizhuldyz_adaibekova',
    'qjeri.kz',
    'tulepbergen_baisakalov',
    'mihonbrat',
    'ernarkyrykbaev',
    'projectx.kz',
    'bdsa1em',
    '39metrov',
    'nestevatr',
    'humor.park',



    'salem_sayahat',
    'aitys4g',
    'talklike_amantasygan',
    'dastarkhan_kaz',
    'prikol.kzz',
    'vseobovsem.pavlodar',
    'vseobovsem_karaganda',
    'vseobovsem_astana',



}
blogs_negative = {
    'azattyq',
    'zanamiviehali',
    'lia3928',
    'dvk_volonter',
    '1612_tv',
    '1612kanal',
    'mukhtarablyazov',
    'aktivist.kaz',
    'antinazik',
    'lolsultan_kz',
    'alibekovkz',
    'kazakh_inform',
    'zhanat_anetova',
    'zhest_kz',
    'rukh2k19',
    'freekazakhs',
    'almaty.times',


#new sites (10.05.19 15.33)
    'lii3928',
    'dvk_oral',
    'program_dvk',
    'aktivist_dvk',
    'dvk_2018',
    'pozorbaev.kz',
    'dvk_pavlodar',
    'oyan__kazak',
    'opposition.kz',
    'dsd2480',
    #new sites (10.05.19 16.51)
    'mr.grazhdanin',
    'almaz_karaseri',
    'dsatpayev',
    'aika.tleukz',
    'diana_kz.13',
    'botazhanna555',
    'medinabayssar',
    'tokayev_online',
    'jan.abuov',
    'anuar_nurpeisov',
    'garik8820',
    'democratic_vkz',
    'factcheck_kz',
    'zhanibek83_14',
    'gos.dep.usa',
    'first_president_kz',
    'turekulov_vainameinen19830530',
    'dvk_ustkamenogorsk',
    'dvk_krg',
    'dvk_shym',
    'dvk_alga_kazakhstan',
    'l_love_you_dvk',
    'dvkvolonter',
    'dvk_taldykorgan',
    'oizon',
    'dvk_06',
    'currenttimeasia',
    #new sites (13.05.19)
    'oyan_kazakh_alga_kazakhstan',
    'svobodnaya_strana',
    'astanovka98',
    'dvk.kz00',
    'dvk_zh',
    'dvk_taraz',
    'dvk_aktobe',
    'dvk_oskemen',
    'dvk_petropavl',
    'dvk_aktau7292',
    'anty_dvk_shymkent'

    'isenova',
'free.kazakhstan',
    'almaty__dubsmash',
    'strangeice',
    'suinbike',
    'ladykolbasa',
    'temujin_duisen',
    'marklenov',
    'miroiu',
    'asianflashh',
    'ainur_niyazova',
    'ittin.balasy',
    'qwaypunk',
    'dvk_choice',
    'karlygashablyazova',
    'freedom_kz2019',
    'miting.kz',
    '777avangard777',
    'prodvijenie_cmm',
    'soz_bostandygy',
    'erbolat_ali',
    'maya_mayflower',
    'zello_kaz',
    'press.kit',
    'alash_ulandary',
    'uralsk_avto_sauda',
    'bobbie_the_brave',
    'the_mister_rav',
    'oybay.kz',
    'democraticandfreedom.kz',
    'muratdilmanov',
    'cap7su',
    '1mukhtarmurat',
    'dimash_alzhanov',
    'zhanel.k',
    'benkafemka',
    'suinbike',
    'yamateh0106',
    'relax.life.kz',
    'temujin_duisen',
    'miroiu',
    'anduman',
    'rayhanfw',
    'juno_lunaris',
    'erbol.seidazimov',
    'violaluiji',
    'yrysty__a',
    'leyla_zuleikha',
    'arach_01',
    'arina_chilladze',
    'arty.ninja',
    'ainikosha',
    'adalim420',
    'timurnusimbekov',
    'birzhan_tba',
    'rinat_zaitov_resmi',

}


for blog in blogs_positive:
   data_blog = get_id_for_blog(url='https://www.instagram.com/' + blog + '/?__a=1', headers=headers)
   get_blog_data(data_blog, 2)
time.sleep(1800)

for blog1 in blogs_negative:
    data_blog = get_id_for_blog(url='https://www.instagram.com/' + blog1 + '/?__a=1', headers=headers)
    get_blog_data(data_blog, 1002)
time.sleep(1800)

