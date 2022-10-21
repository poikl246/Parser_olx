import random
import sqlite3
import time
from bs4 import BeautifulSoup
import requests
import config
# from servise import request_no_error2
import servise
import pars_id
import pars_data
import db_start

def parsing(url_input_f, tableName):
    s = requests.Session()
    s.proxies = {"http": f"socks5://localhost:{random.randint(9000, 9300)}",
                 "https": f"socks5://localhost:{random.randint(9000, 9300)}"}

    with sqlite3.connect('info.db') as db:

        list_url_obl = ['toshkent-oblast', 'samarkandskaya-oblast', 'buharskaya-oblast', 'navoijskaya-oblast',
         'horezmskaya-oblast', 'ferganskaya-oblast', 'kashkadarinskaya-oblast', 'karakalpakstan',
         'syrdarinskaya-oblast', 'surhandarinskaya-oblast']

        for obl in list_url_obl:
            url_input = url_input_f + obl
            print(url_input)

            for i in range(1, 100):

                print(url_input + f'?page={i}')
                response = servise.request_no_error2(url_input + f'?page={i}', s)
                print('response', i)
                # response.encoding = 'utf-8'
                src = response.text
                print('src', i)
                soup = BeautifulSoup(src, 'html.parser')
                print('soup', i)
                # print(src)


                # with open('index.html', 'w', encoding='utf-8') as file_1234564:
                #     file_1234564.write(src)

                for data in soup.find_all(class_="css-1bbgabe"):
                    try:
                        print('https://www.olx.uz' + data.get('href'))
                        db.execute(f'INSERT INTO {tableName} (url) VALUES (?);', [str('https://www.olx.uz' + data.get('href'))])
                    except Exception as e:
                        print(e, 'https://www.olx.uz' + data.get('href'), i)
                    # time.sleep(0.1)
                db.commit()

                print('for', i)


                print(i, '/', soup.find_all(attrs={"data-testid":"pagination-list-item"})[-1].text)

                if int(soup.find_all(attrs={"data-testid":"pagination-list-item"})[-1].text) == i:
                    print('break', i)
                    break

                for timeer in range(1, 0, -1):
                    time.sleep(1)
                    print(timeer)
                #if i == 2:
                    #break



def main(process_count):
    url_global_list = config.url_global_list

    for url_global in url_global_list:
        
        with open('info.txt', 'w', encoding='utf-8') as file_info:
            file_info.write(f'{url_global}\n{time.ctime(time.time())}\n')

        table_name_history = str(
            '_'.join(url_global.replace('https://www.olx.uz/d/nedvizhimost/', '').replace('?', '').split('/')).replace(
                '-', '_')) + time.strftime("%m_%Y", time.gmtime(time.time()))

        table_name = str(
            '_'.join(url_global.replace('https://www.olx.uz/d/nedvizhimost/', '').replace('?', '').split('/')).replace(
                '-', '_')) + '_temp'

        print(f"{table_name=}")
        # continue

        db_start.db_start_table(table_name)

        db_start.creating_tables_in_the_database(table_name, table_name_history)

        #### # # for i in range(10):
        try:
            parsing(url_global, tableName=table_name)
            pass
        except:
            pass


        for i in range(2):
            pars_id.Collecting_id_and_id2_start(table_name, process_count_input=7)
            pass


        for i in range(2):
            try:
                pars_data.Collecting_data_start(table_name, process_count_input=4)
                pass
            except:
                print('перезапуск')
        

        for i in range(30):
            print("Новая таблица")
            time.sleep(1)

    db_start.db_sqlite_to_PostgreSQL()
    db_start.PostgreSQL_history()
    db_start.Delete_info_db()
    
    print('The code has finished its work and is waiting for a new parsing to start')
if __name__ == '__main__':
    try:
        main(5)
    except Exception as e:
        print(e)

    # main(process_count=30)
    print('main завершен, спим')

    while True:
        if time.localtime(time.time())[3] > 19:
            time.sleep(10*60)
            print('спим', time.time())
        elif time.localtime(time.time())[3] < 20:
            time.sleep(10*60)
            print('спим', time.time())

        else:
            try:
                main(5)
                print('main завершен, спим')
            except Exception as e:
                print(e)
