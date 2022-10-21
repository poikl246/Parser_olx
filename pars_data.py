import random
import sqlite3
import time
import requests
from threading import Thread
from servise import request_no_error2, func_chunks_generators, token_get
import categori_write


def Collecting_data_pars(url_list, token):
    s = requests.Session()
    s.proxies = {"http": f"socks5://localhost:{random.randint(9000, 9300)}", "https": f"socks5://localhost:{random.randint(9000, 9300)}"}
    print('мы в потоке')
    # s.verify = '/path/to/certfile'
    with sqlite3.connect('info.db') as db:
        cursor = db.cursor()
        for id_, id_2, table_name in url_list:
            print(id_, id_2, table_name)
            url = f'https://www.olx.uz/api/v1/offers/{id_}/suggested/'
            response = request_no_error2(url, s, type=1, token=token)
            resp = response.json()
            print(resp)


            if 'kvartiry' in table_name:
                categori_write.apartments(resp, table_name=table_name, db=db, cursor=cursor, id_2=id_2)
            elif 'zemlja' in table_name:
                print('мы тута')
                categori_write.land(resp, table_name=table_name, db=db, cursor=cursor, id_2=id_2)

            elif 'kommercheskie_pomeshcheniya' in table_name:
                print('мы тута')
                categori_write.Commercial_premises(resp, table_name=table_name, db=db, cursor=cursor, id_2=id_2)

            elif 'doma' in table_name:
                print('мы тута')
                categori_write.doma(resp, table_name=table_name, db=db, cursor=cursor, id_2=id_2)

            elif 'garazhi_stoyanki' in table_name:
                print('мы тута')
                categori_write.garazhi_stoyanki(resp, table_name=table_name, db=db, cursor=cursor, id_2=id_2)

            db.commit()
        db.commit()
        return 'good'

def Collecting_data_start(table_name, process_count_input):
    len_data = 99999
    flag = 0
    s = requests.Session()
    s.proxies = {"http": f"socks5://localhost:{random.randint(9000, 9300)}", "https": f"socks5://localhost:{random.randint(9000, 9300)}"}
    # global headers_token
    print('Начинаю сбор данных')

    token = token_get(table_name=table_name, s=s)

    time.sleep(2)

    for skjgkjr in range(10):
        
        if random.randint(0, 3) == 1:
            token = token_get(table_name=table_name, s=s)

        with sqlite3.connect('info.db') as db:
            cursor = db.cursor()
            cursor.execute(f"SELECT id, id_2 FROM {table_name} WHERE id is not NULL and created_time is NULL ORDER BY RANDOM();")

            dataset = list(map(lambda x:[x[0], x[1], table_name], cursor.fetchall()))


            # cursor.execute(
            #     f"SELECT url FROM {table_name} WHERE ORDER BY RANDOM() LIMIT 1;")
            #
            # url_token = cursor.fetchone()[0]

        len_dataset = len(dataset)
        print(dataset, len_dataset)

        if len_dataset == len_data:
            flag += 1
        else:
            flag = 0

        if len_dataset == 0 or flag == 5:
            break

        len_data = len_dataset

        process_count = min(int(round(len_dataset / 3, 0)) + 1, process_count_input)


        print('запускаю')
        time.sleep(2)

        urls_list = list(func_chunks_generators(dataset, process_count))
        threads = []
        for i in range(process_count):
            th = Thread(target=Collecting_data_pars, args=(urls_list[i], token))
            th.start()
            threads.append(th)
        #for t in threads:
           # t.join()



        for sleep in range(10, 0, -1):
            print(f"{sleep=}")
            time.sleep(1)

if __name__ == '__main__':
    Collecting_data_start('kvartiry_arenda_dolgosrochnaya__temp', 20)
