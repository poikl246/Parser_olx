import random
import re
import sqlite3
import time
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from servise import request_no_error2, func_chunks_generators
from threading import Thread



def Collecting_id_and_id_2_pars(url_list, retry=5):
    s = requests.Session()
    s.proxies = {"http": f"socks5://localhost:{random.randint(9000, 10000)}", "https": f"socks5://localhost:{random.randint(9000, 10000)}"}
    # s.verify = '/path/to/certfile'
    with sqlite3.connect('info.db') as db:
        for url, table_name in url_list:
            try:
                response = request_no_error2(url, s)
                response.encoding = 'cp-1251'

                resp = response.text


                soup = BeautifulSoup(resp, 'html.parser')

                id_page = int(soup.find(class_="css-9xy3gn-Text eu5v0x0").text.replace('ID:', '').strip())
                print(id_page)

                data = str(soup.find(id="olx-init-config"))

                out_clov = [i.split(":")[1] for i in re.findall(r'"id.":\d{5,12}', data)]

                if len(out_clov) == 1:
                    out_clov.append('')

                out_clov.append(url)
                print(out_clov)
                db.execute(f"UPDATE {table_name} SET id=?, id_2=? WHERE url = ?", out_clov)
                db.commit()

            except Exception as e:
                print(e)



def Collecting_id_and_id2_start(table_name, process_count_input):
    len_data = 99999
    flag = 0
    for ibvvjb in range(3):
        with sqlite3.connect('info.db') as db:
            cursor = db.cursor()
            cursor.execute(f"SELECT url FROM {table_name} WHERE id is NULL ORDER BY RANDOM();")

            dataset = list(map(lambda x:[x[0], table_name], cursor.fetchall()))

        len_dataset = len(dataset)
        # print(dataset, len_dataset)

        print('\n\n\n')

        if len_dataset == len_data:
            flag += 1
        else:
            flag = 0

        if len_dataset == 0 or flag == 3:
            break

        len_data = len_dataset

        process_count = min(len_dataset, process_count_input)


        urls_list = list(func_chunks_generators(dataset, process_count))
        #p = Pool(processes=process_count)
        #p.map(Collecting_id_and_id_2_pars, urls_list)

        for sleep in range(10, 0, -1):
            print(f"{sleep=}")
            time.sleep(1)

        threads = []
        for i in range(process_count):
            th = Thread(target=Collecting_id_and_id_2_pars, args=(urls_list[i],))
            th.start()
            threads.append(th)


    len_data = 99999
    flag = 0


if __name__ == "__main__":
    Collecting_id_and_id2_start('doma_arenda_posutochno__temp', 10)
