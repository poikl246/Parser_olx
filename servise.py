import re
import sqlite3
import requests
import random
import time


def func_chunks_generators(seq, size):
    return (seq[i::size] for i in range(size))

def request_no_error2(url, s, type = 0, retry=3, token=''):
    response = ''
    # global headers_token



    headers = {
        'Host': 'www.olx.uz',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        # 'Accept-Encoding': 'gzip, deflate',
        'Dnt': '1',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        # Requests doesn't support trailers
        # 'Te': 'trailers',
        'Connection': 'close',
        # Requests sorts cookies= alphabetically
        # 'Cookie': 'laquesis=buy-1923@b#buy-2717@b#edu2r-3360@b#edu2r-3361@b#edu2r-3601@a#er-1493@b#er-1596@b#er-1598@b#er-1609@b#er-1658@b#er-1697@a#euonb-524@b#jobs-2491@a#jobs-3722@b; laquesisff=aut-716#euonb-114#euonb-48#grw-124#oesx-1437#oesx-1643#oesx-645#oesx-867#olxeu-29763#srt-1289#srt-1346#srt-1434#srt-1593#srt-1758#srt-663#srt-669#srt-671#srt-899; lqstatus=1657646492|181f35c640ax6f626e9b|er-1493#buy-2717||; deviceGUID=e740de19-c12f-4a31-b4f7-bb60998e5b8b; a_access_token=f024daf0e2ecd9b2e7173d28c6db533d9e3569a9; a_refresh_token=91f7aa1cd43dbd2f9e0da67e980fbb5a3fbe0cd1; a_grant_type=device; session_start_date=1657648070652; observed_aui=439c63ca93a84929b20c03c7ad3a10d1; user_id=102937859; user_business_status=private; onap=181e84c96a3x74b9937d-3-181f35c640ax6f626e9b-9-1657648070; _gcl_au=1.1.1630396598.1657459774; laquesissu=296@favourite_search_save|1#296@favourite_search_save|1#296@save_search_banner_clicked|1#296@favourite_search_save|1#296@favourite_ad_click|1#298@reply_chat_sent|1#300@my_messages_sent|1#303@jobs_preferences_click|0#303@jobs_save_preferred_position|0#303@jobs_select_preferred_time|0#303@jobs_select_preferred_contract|0#303@jobs_save_preferred_salary|0; ldTd=true; cookieBarSeenV2=true; cookieBarSeen=true; mobile_default=desktop; sbjs_migrations=1418474375998%3D1; sbjs_current_add=fd%3D2022-07-12%2022%3A01%3A56%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.olx.uz%2Fd%2Fnedvizhimost%2Fkvartiry%2Farenda-dolgosrochnaya%2F%7C%7C%7Crf%3Dhttps%3A%2F%2Fwww.olx.uz%2Fd%2Fnedvizhimost%2Fkvartiry%2F; sbjs_first_add=fd%3D2022-07-12%2022%3A01%3A56%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.olx.uz%2Fd%2Fnedvizhimost%2Fkvartiry%2Farenda-dolgosrochnaya%2F%7C%7C%7Crf%3Dhttps%3A%2F%2Fwww.olx.uz%2Fd%2Fnedvizhimost%2Fkvartiry%2F; sbjs_current=typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29; sbjs_first=typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29; sbjs_udata=vst%3D1%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28X11%3B%20Linux%20x86_64%3B%20rv%3A102.0%29%20Gecko%2F20100101%20Firefox%2F102.0; sbjs_session=pgs%3D11%7C%7C%7Ccpg%3Dhttps%3A%2F%2Fwww.olx.uz%2Fd%2Fnedvizhimost%2Fkvartiry%2Farenda-dolgosrochnaya%2F%3Fpage%3D2; newrelic_cdn_name=CF; PHPSESSID=i9opg7q36c7bv956adjk53j8ii',
        # 'Authorization': 'Bearer 86b73cd814c95bb760923635bf05dae4d27ab7ff'
        }


    if type == 1:
        headers = {
            'Host': 'www.olx.uz',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0',
            'Accept': '*/*',
            'Accept-Language': 'ru',
            # 'Accept-Encoding': 'gzip, deflate',
            'Referer': 'https://www.olx.uz/d/obyavlenie/2-honali-uy-izharaga-beriladi-ID2W4kQ.html?reason=ip%7Cmaria',
            'Authorization': f'Bearer {token}',
            'X-Client': 'DESKTOP',
            'X-Device-Id': 'b350e50a-72b2-4eb5-a57a-a327d7a713ad',
            'X-Platform-Type': 'mobile-html5',
            'Version': 'v1.19',
            'Dnt': '1',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            # Requests doesn't support trailers
            # 'Te': 'trailers',
            'Connection': 'close',
            # Requests sorts cookies= alphabetically
            # 'Cookie': 'sbjs_migrations=1418474375998%3D1; sbjs_current_add=fd%3D2022-07-20%2019%3A12%3A04%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.olx.uz%2Fd%2Fobyavlenie%2F2-honali-uy-izharaga-beriladi-ID2W4kQ.html%3Freason%3Dip%257Cmaria%7C%7C%7Crf%3Dhttps%3A%2F%2Fwww.olx.uz%2Fd%2Fobyavlenie%2Fizharaga-fargona-kirgili-2-honali-kv-ID2Wngl.html; sbjs_first_add=fd%3D2022-07-20%2019%3A12%3A04%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.olx.uz%2Fd%2Fobyavlenie%2F2-honali-uy-izharaga-beriladi-ID2W4kQ.html%3Freason%3Dip%257Cmaria%7C%7C%7Crf%3Dhttps%3A%2F%2Fwww.olx.uz%2Fd%2Fobyavlenie%2Fizharaga-fargona-kirgili-2-honali-kv-ID2Wngl.html; sbjs_current=typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29; sbjs_first=typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29; sbjs_udata=vst%3D1%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28X11%3B%20Linux%20x86_64%3B%20rv%3A102.0%29%20Gecko%2F20100101%20Firefox%2F102.0; sbjs_session=pgs%3D1%7C%7C%7Ccpg%3Dhttps%3A%2F%2Fwww.olx.uz%2Fd%2Fobyavlenie%2F2-honali-uy-izharaga-beriladi-ID2W4kQ.html%3Freason%3Dip%257Cmaria; lqonap=1821bf3a235x60ae4dc6; laquesis=buy-2717@b#decision-377@a#edu2r-3360@b#edu2r-3361@b#edu2r-3601@b#er-1598@b#er-1697@b#euonb-524@b#jobs-3722@a; laquesisff=aut-716#euonb-114#euonb-48#grw-124#oesx-1437#oesx-1643#oesx-645#oesx-867#olxeu-29763#srt-1289#srt-1346#srt-1434#srt-1593#srt-1758#srt-663#srt-669#srt-671#srt-899; lqstatus=1658327528; deviceGUID=b350e50a-72b2-4eb5-a57a-a327d7a713ad; newrelic_cdn_name=CF; PHPSESSID=fli5v5jeujcovt3ibf6ibec3ie; a_access_token=16378da582f773cc65ee84feb8bff94d6c182c8f; a_refresh_token=ae9b7a79699f269be6ddaf9849d478b91ab3cf42; a_grant_type=device',
        }

    try:
        print('мы в запросе')
        response = s.get(url=url, headers=headers, verify=False)
        # response = requests.get(url=url, headers=headers, proxies=pr())
        print(f"[+] {url} {response.status_code}")

        if response.status_code == 401:
            print("Ошибка status_code", retry)
            if retry > 0:
                return request_no_error2(url, s, retry=(retry - 1))
            else:
                print('Ну типа все', url)
                return 0
        if response.status_code != 200:
            print('спим 2 секунды')
            time.sleep(2)
            s.proxies = {"http": f"socks5://localhost:{random.randint(9000, 9300)}", "https": f"socks5://localhost:{random.randint(9000, 9300)}"}
            print("Ошибка status_code", retry)
            if retry > 0:
                return request_no_error2(url, s, retry=(retry - 1))
            else:
                print('Ну типа все', url)
                return 0
    except Exception as ex:
        print('была ошибка в запросе, так что жду 3 секунды')
        time.sleep(3)
        if retry > 0:
            s.proxies = {"http": f"socks5://localhost:{random.randint(9000, 9300)}", "https": f"socks5://localhost:{random.randint(9000, 9300)}"}
            print(f"[INFO] retry={retry} => {url}", retry, '\n', ex)
            return request_no_error2(url, s, retry=(retry - 1))
        else:
            print('Ну типа все', url)
            return 0
    else:
        return response


def token_get(table_name, s):
    headers = {
        'Host': 'www.olx.uz',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0',
        'Accept': '*/*',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        # 'Accept-Encoding': 'gzip, deflate',
        'Referer': 'https://www.olx.uz/d/obyavlenie/2-honali-uy-izharaga-beriladi-ID2W4kQ.html?reason=ip%7Cmaria',
        # Already added when you pass json=
        # 'Content-Type': 'application/json',
        'X-Client': 'DESKTOP',
        'Origin': 'https://www.olx.uz',
        # 'Content-Length': '308',
        'Dnt': '1',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        # Requests doesn't support trailers
        # 'Te': 'trailers',
        'Connection': 'close',
        # Requests sorts cookies= alphabetically
        # 'Cookie': 'sbjs_migrations=1418474375998%3D1; sbjs_current_add=fd%3D2022-07-20%2019%3A12%3A04%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.olx.uz%2Fd%2Fobyavlenie%2F2-honali-uy-izharaga-beriladi-ID2W4kQ.html%3Freason%3Dip%257Cmaria%7C%7C%7Crf%3Dhttps%3A%2F%2Fwww.olx.uz%2Fd%2Fobyavlenie%2Fizharaga-fargona-kirgili-2-honali-kv-ID2Wngl.html; sbjs_first_add=fd%3D2022-07-20%2019%3A12%3A04%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.olx.uz%2Fd%2Fobyavlenie%2F2-honali-uy-izharaga-beriladi-ID2W4kQ.html%3Freason%3Dip%257Cmaria%7C%7C%7Crf%3Dhttps%3A%2F%2Fwww.olx.uz%2Fd%2Fobyavlenie%2Fizharaga-fargona-kirgili-2-honali-kv-ID2Wngl.html; sbjs_current=typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29; sbjs_first=typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29; sbjs_udata=vst%3D1%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28X11%3B%20Linux%20x86_64%3B%20rv%3A102.0%29%20Gecko%2F20100101%20Firefox%2F102.0; sbjs_session=pgs%3D1%7C%7C%7Ccpg%3Dhttps%3A%2F%2Fwww.olx.uz%2Fd%2Fobyavlenie%2F2-honali-uy-izharaga-beriladi-ID2W4kQ.html%3Freason%3Dip%257Cmaria; lqonap=1821bf3a235x60ae4dc6; laquesis=buy-2717@b#decision-377@a#edu2r-3360@b#edu2r-3361@b#edu2r-3601@b#er-1598@b#er-1697@b#euonb-524@b#jobs-3722@a; laquesisff=aut-716#euonb-114#euonb-48#grw-124#oesx-1437#oesx-1643#oesx-645#oesx-867#olxeu-29763#srt-1289#srt-1346#srt-1434#srt-1593#srt-1758#srt-663#srt-669#srt-671#srt-899; lqstatus=1658327528; deviceGUID=b350e50a-72b2-4eb5-a57a-a327d7a713ad',
    }

    with sqlite3.connect('info.db') as db:
        cursor = db.cursor()
        cursor.execute(
            f"SELECT url FROM {table_name} WHERE id is not NULL and created_time is NULL ORDER BY RANDOM() LIMIT 1;")


    response = request_no_error2(cursor.fetchone()[0], s)

    client_id = int(
        re.findall(r'"client_id\\":\\"\d{4,7}\\"', response.text)[0].replace('"client_id\\":\\"', '').replace('\\"',
                                                                                                              ''))

    client_secret = (re.findall(r'client_secret\\":\\".{40,55}\\"', response.text))[0].replace('client_secret\\":\\"',
                                                                                               '').replace(r'\",\"', '')

    json_data = {
        'device_id': 'b350e50a-72b2-4eb5-a57a-a327d7a713ad',
        'device_token': 'eyJpZCI6ImIzNTBlNTBhLTcyYjItNGViNS1hNTdhLWEzMjdkN2E3MTNhZCJ9.74b7c85b679a8b6f0e4f0dbbc7d02b479b417d2b',
        'grant_type': 'device',
        'scope': 'i2 read write v2',
        'client_id': client_id,
        'client_secret': client_secret,
    }

    cookies = {
        'deviceGUID': 'b350e50a-72b2-4eb5-a57a-a327d7a713ad', }

    try:
        response = s.post('https://www.olx.uz/api/open/oauth/token/', cookies=cookies, headers=headers,
                          json=json_data, verify=False)

        token = response.json()['access_token']
    except:
        print('\n\n\n\n\nТОКЕН ERROR\n\n\n')
        return token_get(table_name, s)

    print(f"{token=}")
    return token



if __name__ == '__main__':
    s = requests.Session()
    token_get('kvartiry_arenda_dolgosrochnaya__temp', s)