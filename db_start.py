import sqlite3
import time
import config
import psycopg2
import datetime
from datetime import datetime


def db_start_table(table_name, table_name_history=''):
    with sqlite3.connect('info.db') as db:
        cursor = db.cursor()

        if 'kvartiry' in table_name:
            cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                                                           id int UNIQUE,
                                                           id_2 int,
                                                           url text UNIQUE,
                                                           created_time DATETIME,
                                                           valid_to_time DATETIME,
                                                           type_of_market,
                                                           number_of_rooms,
                                                           urgent,
                                                           floor,
                                                           total_floors,
                                                           house_type,
                                                           layout,
                                                           wc,
                                                           furnished,
                                                           more,
                                                           near_is,
                                                           repairs,
                                                           comission,
                                                           price float,
                                                           currency,
                                                           user_id,
                                                           user_name,
                                                           lat float,
                                                           lon float,
                                                           zoom float,
                                                           radius float, 
                                                           city,
                                                           district,
                                                           region text,
                                                           year_of_construction_sale,
                                                           ceiling_height,
                                                           total_area,
                                                           b2c_business_page,
                                                           status,
                                                           premium_ad_page
                                                           );""")



        elif 'zemlja' in table_name:
            cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
            id int UNIQUE,
            id_2 int,
            url text UNIQUE,
            created_time DATETIME,
            valid_to_time DATETIME,
            comission,
            price,
            currency,
            user_id,
            user_name,
            b2c_ad_page,
            status,
            premium_ad_page,
            land_area,
            purpose,
            location,
            type_of_plot,
            communications,
            commission,
            city,
            district,
            region,
            busines,
            zoom float,
            lat float,
            lon float,
            radius float,
            urgent,
            b2c_business_page
            );""")



        elif 'kommercheskie_pomeshcheniya' in table_name:
            cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
            id int UNIQUE,
            id_2 int,
            url text UNIQUE,
            created_time DATETIME,
            valid_to_time DATETIME,
            urgent,
            b2c_ad_page,
            b2c_business_page,
            premium_ad_page,
            price,
            currency,
            premise_type,
            user_id,
            user_name,
            total_area,
            effective_area,
            premises_location,
            floor,
            total_floor,
            ceiling_height,
            repairs,
            more_premise,
            parking_lot,
            comission,
            business,
            status,
            zoom float,
            lat float,
            lon float,
            radius float,
            city,
            region,
            land,
            district
            );""")



        elif 'doma' in table_name:
            cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
            id int UNIQUE,
            id_2 int,
            url text UNIQUE,
            created_time DATETIME,
            valid_to_time DATETIME,
            number_of_rooms, 
            urgent,
            total_floors,
            house_type,
            wc_house,
            furnished_house, 
            more_house,
            near_is,
            house_repairs,
            comission,
            price,
            currency,
            user_id,
            user_name,
            lat float,
            lon float,
            zoom float,
            radius float,
            city,
            district,
            region,
            year_of_construction_rent,
            year_of_construction_sale,
            ceiling_height,
            total_area,
            b2c_business_page,
            status,
            premium_ad_page,
            business,
            plot,
            private_house_type,
            water,
            heating,
            gas,
            total_living_area,
            electricity, 
            wall_type, 
            b2c_ad_page, 
            Repairs,
            rent_from
            );""")




        elif 'garazhi_stoyanki' in table_name:
            cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
            id int UNIQUE,
            id_2 int,
            url text UNIQUE,
            created_time DATETIME,
            valid_to_time DATETIME,
            comission,
            price,
            currency,
            user_id,
            user_name,
            b2c_business_page,
            status,
            premium_ad_page,
            garage_type,
            purpose_of_garage, 
            area, 
            lat float,
            lon float,
            zoom float,
            radius float,
            city,
            district,
            region,
            quantity_of_spaces, 
            business,
            b2c_ad_page
            );""")





def creating_tables_in_the_database(table_name, table_name_history):
    with psycopg2.connect(
            host=config.host,
            user=config.user,
            password=config.password,
            database=config.database
    ) as db:

        with db.cursor() as cursor:

            if 'kvartiry' in table_name:
                cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                                                                   id int UNIQUE,
                                                                   id_2 int,
                                                                   url text UNIQUE,
                                                                   created_time timestamp,
                                                                   valid_to_time timestamp,
                                                                   type_of_market text,
                                                                   number_of_rooms int,
                                                                   urgent int,
                                                                   floor int,
                                                                   total_floors int,
                                                                   house_type text,
                                                                   layout text,
                                                                   wc text,
                                                                   furnished text,
                                                                   more text,
                                                                   near_is text,
                                                                   repairs text,
                                                                   comission text,
                                                                   price float,
                                                                   currency text,
                                                                   user_id int,
                                                                   user_name text,
                                                                   lat float,
                                                                   lon float,
                                                                   zoom float,
                                                                   radius float, 
                                                                   city text,
                                                                   district text,
                                                                   region text,
                                                                   year_of_construction_sale int,
                                                                   ceiling_height int,
                                                                   total_area int,
                                                                   b2c_business_page int,
                                                                   status text,
                                                                   premium_ad_page int
                                                                   );""")

                cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name_history} (
                                                                   id int UNIQUE,
                                                                   id_2 int,
                                                                   url text UNIQUE,
                                                                   created_time timestamp,
                                                                   valid_to_time timestamp,
                                                                   type_of_market text,
                                                                   number_of_rooms int,
                                                                   urgent int,
                                                                   floor int,
                                                                   total_floors int,
                                                                   house_type text,
                                                                   layout text,
                                                                   wc text,
                                                                   furnished text,
                                                                   more text,
                                                                   near_is text,
                                                                   repairs text,
                                                                   comission text,
                                                                   price float,
                                                                   currency text,
                                                                   user_id int,
                                                                   user_name text,
                                                                   lat float,
                                                                   lon float,
                                                                   zoom float,
                                                                   radius float, 
                                                                   city text,
                                                                   district text,
                                                                   region text,
                                                                   year_of_construction_sale int,
                                                                   ceiling_height int,
                                                                   total_area int,
                                                                   b2c_business_page int,
                                                                   status text,
                                                                   premium_ad_page int
                                                                   );""")


            elif 'zemlja' in table_name:
                cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                    id int UNIQUE,
                    id_2 int,
                    url text UNIQUE,
                    created_time timestamp,
                    valid_to_time timestamp,
                    comission text,
                    price float,
                    currency text,
                    user_id int,
                    user_name text,
                    b2c_ad_page boolean,
                    status text,
                    premium_ad_page boolean,
                    land_area int,
                    purpose text,
                    location text,
                    type_of_plot text,
                    communications text,
                    commission text,
                    city text,
                    district text,
                    region text,
                    busines text,
                    zoom float,
                    lat float,
                    lon float,
                    radius float,
                    urgent text,
                    b2c_business_page int
                    );""")

                cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name_history} (
                                            id int UNIQUE,
                    id_2 int,
                    url text UNIQUE,
                    created_time timestamp,
                    valid_to_time timestamp,
                    comission text,
                    price float,
                    currency text,
                    user_id int,
                    user_name text,
                    b2c_ad_page boolean,
                    status text,
                    premium_ad_page boolean,
                    land_area int,
                    purpose text,
                    location text,
                    type_of_plot text,
                    communications text,
                    commission text,
                    city text,
                    district text,
                    region text,
                    busines text,
                    zoom float,
                    lat float,
                    lon float,
                    radius float,
                    urgent text,
                    b2c_business_page int
                                    );""")

            elif 'kommercheskie_pomeshcheniya' in table_name:
                cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                    id int UNIQUE,
                    id_2 int,
                    url text UNIQUE,
                    created_time timestamp,
                    valid_to_time timestamp,
                    urgent int,
                    b2c_ad_page int,
                    b2c_business_page int,
                    premium_ad_page int,
                    price float,
                    currency text,
                    premise_type text,
                    user_id int,
                    user_name text,
                    total_area float,
                    effective_area float,
                    premises_location text,
                    floor int,
                    total_floor int,
                    ceiling_height float,
                    repairs text,
                    more_premise text,
                    parking_lot text,
                    comission text,
                    business int,
                    status text,
                    zoom float,
                    lat float,
                    lon float,
                    radius float,
                    city text,
                    region text,
                    land float,
                    district text
                    );""")

                cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name_history} (
                                        id int UNIQUE,
                                        id_2 int,
                                        url text UNIQUE,
                                        created_time timestamp,
                                        valid_to_time timestamp,
                                        urgent int,
                                        b2c_ad_page int,
                                        b2c_business_page int,
                                        premium_ad_page int,
                                        price float,
                                        currency text,
                                        premise_type text,
                                        user_id int,
                                        user_name text,
                                        total_area float,
                                        effective_area float,
                                        premises_location text,
                                        floor int,
                                        total_floor int,
                                        ceiling_height float,
                                        repairs text,
                                        more_premise text,
                                        parking_lot text,
                                        comission text,
                                        business int,
                                        status text,
                                        zoom float,
                                        lat float,
                                        lon float,
                                        radius float,
                                        city text,
                                        region text,
                                        land float,
                                        district text
                                    );""")

            elif 'doma' in table_name:
                cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                    id int UNIQUE,
                    id_2 int,
                    url text UNIQUE,
                    created_time timestamp,
                    valid_to_time timestamp,
                    number_of_rooms int, 
                    urgent int,
                    total_floors int,
                    house_type text,
                    wc_house text,
                    furnished_house text, 
                    more_house text,
                    near_is text,
                    house_repairs text,
                    comission text,
                    price float,
                    currency text,
                    user_id int,
                    user_name text,
                    lat float,
                    lon float,
                    zoom float,
                    radius float,
                    city text,
                    district text,
                    region text,
                    year_of_construction_rent int,
                    year_of_construction_sale int,
                    ceiling_height float,
                    total_area float,
                    b2c_business_page int,
                    status text,
                    premium_ad_page int,
                    business int,
                    plot float,
                    private_house_type text,
                    water text,
                    heating text,
                    gas text,
                    total_living_area float,
                    electricity text, 
                    wall_type text, 
                    b2c_ad_page int, 
                    Repairs text,
                    rent_from text
                    );""")

                cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name_history} (
                    id int UNIQUE,
                    id_2 int,
                    url text UNIQUE,
                    created_time timestamp,
                    valid_to_time timestamp,
                    number_of_rooms int, 
                    urgent int,
                    total_floors int,
                    house_type text,
                    wc_house text,
                    furnished_house text, 
                    more_house text,
                    near_is text,
                    house_repairs text,
                    comission text,
                    price float,
                    currency text,
                    user_id int,
                    user_name text,
                    lat float,
                    lon float,
                    zoom float,
                    radius float,
                    city text,
                    district text,
                    region text,
                    year_of_construction_rent int,
                    year_of_construction_sale int,
                    ceiling_height float,
                    total_area float,
                    b2c_business_page int,
                    status text,
                    premium_ad_page int,
                    business int,
                    plot float,
                    private_house_type text,
                    water text,
                    heating text,
                    gas text,
                    total_living_area float,
                    electricity text, 
                    wall_type text, 
                    b2c_ad_page int, 
                    Repairs text,
                    rent_from text
                    );""")


            elif 'garazhi_stoyanki' in table_name:
                cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                    id int UNIQUE,
                    id_2 int,
                    url text UNIQUE,
                    created_time timestamp,
                    valid_to_time timestamp,
                    comission text,
                    price float,
                    currency text,
                    user_id int,
                    user_name text,
                    b2c_business_page int,
                    status text,
                    premium_ad_page int,
                    garage_type text,
                    purpose_of_garage text, 
                    area text,
                    lat float,
                    lon float,
                    zoom float,
                    radius float,
                    city text,
                    district text,
                    region text,
                    quantity_of_spaces float, 
                    business int,
                    b2c_ad_page int
                    );""")

                cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name_history} (
                    id int UNIQUE,
                    id_2 int,
                    url text UNIQUE,
                    created_time timestamp,
                    valid_to_time timestamp,
                    comission text,
                    price float,
                    currency text,
                    user_id int,
                    user_name text,
                    b2c_business_page int,
                    status text,
                    premium_ad_page int,
                    garage_type text,
                    purpose_of_garage text, 
                    area text,
                    lat float,
                    lon float,
                    zoom float,
                    radius float,
                    city text,
                    district text,
                    region text,
                    quantity_of_spaces float, 
                    business int,
                    b2c_ad_page int
                                    );""")





def kvartiry(data_in):
    glob_out_clov = []

    for data in data_in:
        keys = [['id', 'int'], ['id_2', 'int'], ['url', 'text'], ['created_time', 'text'],
                ['valid_to_time', 'text'], ['type_of_market', 'text'], ['number_of_rooms', 'int'],
                ['urgent', 'int'], ['floor', 'int'], ['total_floors', 'int'],
                ['house_type', 'text'], ['layout', 'text'], ['wc', 'text'], ['furnished', 'text'],
                ['more', 'text'], ['near_is', 'text'], ['repairs', 'text'], ['comission', 'text'],
                ['price', 'float'], ['currency', 'text'], ['user_id', 'int'], ['user_name', 'text'],
                ['lat', 'float'], ['lon', 'float'], ['zoom', 'float'], ['radius', 'float'],
                ['city', 'text'], ['district', 'text'], ['region', 'text'], ['year_of_construction_sale', 'int'],
                ['ceiling_height', 'int'], ['total_area', 'int'], ['b2c_business_page', 'int'],
                ['status', 'text'], ['premium_ad_page', 'int']]


        out_clov = []
        key = 0
        for key_, type_ in keys:
            if type_ == 'int':
                try:
                    if data[key] == None or data[key] == 'None' or data[key] == 'none':
                        out_clov.append(None)
                    else:
                        out_clov.append(int(data[key]))
                except:
                    out_clov.append(None)
            elif type_ == 'float':
                try:
                    if data[key] == None or data[key] == 'None' or data[key] == 'none':
                        out_clov.append(None)
                    else:
                        out_clov.append(float(data[key]))
                except:
                    out_clov.append(None)
            else:
                try:
                    if data[key] == None or data[key] == 'None' or data[key] == 'none':
                        out_clov.append(None)
                    else:
                        out_clov.append(str(data[key]))
                except:
                    out_clov.append('')

            key += 1

        glob_out_clov.append(out_clov)

    return glob_out_clov


def zemlja(data_in):
    glob_out_clov = []

    for data in data_in:
        keys = [['id', 'int'], ['id_2', 'int'], ['url', 'text'], ['created_time', 'text'],
                ['valid_to_time', 'text'], ['comission', 'text'], ['price', 'float'],
                ['currency', 'text'], ['user_id', 'int'], ['user_name', 'text'],
                ['b2c_ad_page', 'boolean'], ['status', 'text'], ['premium_ad_page', 'boolean'],
                ['land_area', 'int'], ['purpose', 'text'], ['location', 'text'],
                ['type_of_plot', 'text'], ['communications', 'text'], ['commission', 'text'],
                ['city', 'text'], ['district', 'text'], ['region', 'text'], ['busines', 'text'],
                ['zoom', 'float'], ['lat', 'float'], ['lon', 'float'], ['radius', 'float'],
                ['urgent', 'text'], ['b2c_business_page', 'int']]


        out_clov = []
        key = 0
        for key_, type_ in keys:
            if type_ == 'int':
                try:
                    out_clov.append(int(data[key]))
                except:
                    out_clov.append(None)
            elif type_ == 'float':
                try:
                    out_clov.append(float(data[key]))
                except:
                    out_clov.append(None)
            else:
                try:
                    out_clov.append(str(data[key]))
                except:
                    out_clov.append('')

            key += 1

        glob_out_clov.append(out_clov)

    return glob_out_clov


def kommercheskie_pomeshcheniya(data_in):
    glob_out_clov = []

    for data in data_in:
        keys = [['id', 'int'], ['id_2', 'int'], ['url', 'text'],
                ['created_time', 'text'], ['valid_to_time', 'text'],
                ['urgent', 'int'], ['b2c_ad_page', 'int'],
                ['b2c_business_page', 'int'], ['premium_ad_page', 'int'],
                ['price', 'float'], ['currency', 'text'], ['premise_type', 'text'],
                ['user_id', 'int'], ['user_name', 'text'], ['total_area', 'float'],
                ['effective_area', 'float'], ['premises_location', 'text'], ['floor', 'int'],
                ['total_floor', 'int'], ['ceiling_height', 'float'], ['repairs', 'text'],
                ['more_premise', 'text'], ['parking_lot', 'text'], ['comission', 'text'],
                ['business', 'int'], ['status', 'text'], ['zoom', 'float'], ['lat', 'float'],
                ['lon', 'float'], ['radius', 'float'], ['city', 'text'], ['region', 'text'],
                ['land', 'float'], ['district', 'text']]


        out_clov = []
        key = 0
        for key_, type_ in keys:
            if type_ == 'int':
                try:
                    out_clov.append(int(data[key]))
                except:
                    out_clov.append(None)
            elif type_ == 'float':
                try:
                    out_clov.append(float(data[key]))
                except:
                    out_clov.append(None)
            else:
                try:
                    out_clov.append(str(data[key]))
                except:
                    out_clov.append('')

            key += 1

        glob_out_clov.append(out_clov)

    return glob_out_clov


def doma(data_in):
    glob_out_clov = []

    for data in data_in:
        keys = [['id', 'int'], ['id_2', 'int'], ['url', 'text'], ['created_time', 'text'],
                ['valid_to_time', 'text'], ['number_of_rooms', 'int'], ['urgent', 'int'],
                ['total_floors', 'int'], ['house_type', 'text'], ['wc_house', 'text'],
                ['furnished_house', 'text'], ['more_house', 'text'], ['near_is', 'text'],
                ['house_repairs', 'text'], ['comission', 'text'], ['price', 'float'],
                ['currency', 'text'], ['user_id', 'int'], ['user_name', 'text'],
                ['lat', 'float'], ['lon', 'float'], ['zoom', 'float'], ['radius', 'float'],
                ['city', 'text'], ['district', 'text'], ['region', 'text'],
                ['year_of_construction_rent', 'int'], ['year_of_construction_sale', 'int'],
                ['ceiling_height', 'float'], ['total_area', 'float'], ['b2c_business_page', 'int'],
                ['status', 'text'], ['premium_ad_page', 'int'], ['business', 'int'],
                ['plot', 'float'], ['private_house_type', 'text'], ['water', 'text'],
                ['heating', 'text'], ['gas', 'text'], ['total_living_area', 'float'],
                ['electricity', 'text'], ['wall_type', 'text'], ['b2c_ad_page', 'int'],
                ['Repairs', 'text'], ['rent_from', 'text']]


        out_clov = []
        key = 0
        for key_, type_ in keys:
            if type_ == 'int':
                try:
                    out_clov.append(int(data[key]))
                except:
                    out_clov.append(None)
            elif type_ == 'float':
                try:
                    out_clov.append(float(data[key]))
                except:
                    out_clov.append(None)
            else:
                try:
                    out_clov.append(str(data[key]))
                except:
                    out_clov.append('')

            key += 1

        glob_out_clov.append(out_clov)

    return glob_out_clov


def garazhi_stoyanki(data_in):
    glob_out_clov = []

    for data in data_in:
        keys = [['id', 'int'], ['id_2', 'int'], ['url', 'text'], ['created_time', 'text'],
                ['valid_to_time', 'text'], ['comission', 'text'], ['price', 'float'],
                ['currency', 'text'], ['user_id', 'int'], ['user_name', 'text'],
                ['b2c_business_page', 'int'], ['status', 'text'], ['premium_ad_page', 'int'],
                ['garage_type', 'text'], ['purpose_of_garage', 'text'], ['area', 'text'],
                ['lat', 'float'], ['lon', 'float'], ['zoom', 'float'], ['radius', 'float'],
                ['city', 'text'], ['district', 'text'], ['region', 'text'],
                ['quantity_of_spaces', 'float'], ['business', 'int'], ['b2c_ad_page', 'int']]


        out_clov = []
        key = 0
        for key_, type_ in keys:
            if type_ == 'int':
                try:
                    out_clov.append(int(data[key]))
                except:
                    out_clov.append(None)
            elif type_ == 'float':
                try:
                    out_clov.append(float(data[key]))
                except:
                    out_clov.append(None)
            else:
                try:
                    out_clov.append(str(data[key]))
                except:
                    out_clov.append('')

            key += 1

        glob_out_clov.append(out_clov)

    return glob_out_clov





def db_sqlite_to_PostgreSQL():
    with psycopg2.connect(
            host=config.host,
            user=config.user,
            password=config.password,
            database=config.database
                                ) as db_postgres:
        db_postgres.autocommit = True
        cursor_post = db_postgres.cursor()
        with sqlite3.connect('info.db') as db_sql:
            cursor_lite = db_sql.cursor()

            table_list = ['kvartiry_arenda_dolgosrochnaya__temp', 'zemlja_prodazha__temp', 'kommercheskie_pomeshcheniya_arenda__temp', 'doma_prodazha__temp', 'garazhi_stoyanki_prodazha__temp', 'kvartiry_arenda_posutochno__temp', 'kvartiry_prodazha__temp', 'kvartiry_obmen__temp', 'doma_arenda_dolgosrochnaya__temp', 'doma_arenda_posutochno__temp', 'doma_obmen__temp', 'zemlja_arenda__temp', 'garazhi_stoyanki_arenda__temp', 'kommercheskie_pomeshcheniya_prodazha__temp']



            time_start = time.time()

            for table_name in table_list:
                print(table_name)
                cursor_lite.execute(f'SELECT id from {table_name} WHERE not created_time is NULL and not valid_to_time is NULL;')
                id_list_lite = set(map(lambda x:x[0], cursor_lite.fetchall()))

                cursor_post.execute(f'SELECT id from {table_name};')
                # id_list_post = set(cursor_post.fetchall())
                id_list_post = set(map(lambda x:x[0], cursor_post.fetchall()))

                # print(id_list_post, table_name)

                record_id = list(id_list_lite - id_list_post)
                # record_id = [0, 43148055, 43198451, 43205568]

                # print(record_id)

                print(len(id_list_lite), len(id_list_post), len(record_id))
                # print(str(record_id)[1:-1])
                if len(record_id) != 0:
                    cursor_lite.execute(f'SELECT * from {table_name} WHERE id in ({str(record_id)[1:-1]});')
                    data = cursor_lite.fetchall()
                    # print(data)
                    print(len(id_list_lite), len(id_list_post), len(record_id), len(data))
                    # break
                    # print(data)

                    if 'kvartiry' in table_name:
                        out = kvartiry(data)
                        print('Write DB')
                        cursor_post.executemany(f"INSERT INTO {table_name} VALUES ({str('%s,' * 35)[:-1]});", out)

                    elif 'zemlja' in table_name:
                        out = zemlja(data)
                        print('Write DB')
                        cursor_post.executemany(f"INSERT INTO {table_name} VALUES ({str('%s,' * 29)[:-1]});", out)

                    elif 'kommercheskie_pomeshcheniya' in table_name:
                        out = kommercheskie_pomeshcheniya(data)
                        print('Write DB')
                        cursor_post.executemany(f"INSERT INTO {table_name} VALUES ({str('%s,' * 34)[:-1]});", out)

                    elif 'doma' in table_name:
                        out = doma(data)
                        print('Write DB')
                        cursor_post.executemany(f"INSERT INTO {table_name} VALUES ({str('%s,' * 45)[:-1]});", out)

                    elif 'garazhi_stoyanki' in table_name:
                        out = garazhi_stoyanki(data)
                        print('Write DB')
                        cursor_post.executemany(f"INSERT INTO {table_name} VALUES ({str('%s,' * 26)[:-1]});", out)

                else:
                    print("It's already full.")

                print(f"{time.time() - time_start=}")
                time_start = time.time()
                # break










                # SELECT *
                # from test WHERE
                # id in (SELECT id FROM test
                # EXCEPT
                # SELECT id FROM test_2);




def PostgreSQL_history():
    with psycopg2.connect(
            host=config.host,
            user=config.user,
            password=config.password,
            database=config.database
                                ) as db_postgres:
        db_postgres.autocommit = True
        cursor_post = db_postgres.cursor()



        for url_global in config.url_global_list:

            table_name_history = str(
                '_'.join(
                    url_global.replace('https://www.olx.uz/d/nedvizhimost/', '').replace('?', '').split('/')).replace(
                    '-', '_')) + time.strftime("%m_%Y", time.gmtime(time.time()))

            table_name = str(
                '_'.join(
                    url_global.replace('https://www.olx.uz/d/nedvizhimost/', '').replace('?', '').split('/')).replace(
                    '-', '_')) + '_temp'

            print('Writing data to historical tables', table_name)
            cursor_post.execute(f"""
            INSERT INTO {table_name_history} SELECT * FROM {table_name} WHERE id in (
            SELECT id FROM {table_name} WHERE not id is NULL
            EXCEPT
            SELECT id FROM {table_name_history} WHERE not id is NULL);
                                    """)



def Delete_info_db():
    table_list = ['kvartiry_arenda_dolgosrochnaya__temp', 'zemlja_prodazha__temp',
                  'kommercheskie_pomeshcheniya_arenda__temp', 'doma_prodazha__temp', 'garazhi_stoyanki_prodazha__temp',
                  'kvartiry_arenda_posutochno__temp', 'kvartiry_prodazha__temp', 'kvartiry_obmen__temp',
                  'doma_arenda_dolgosrochnaya__temp', 'doma_arenda_posutochno__temp', 'doma_obmen__temp',
                  'zemlja_arenda__temp', 'garazhi_stoyanki_arenda__temp', 'kommercheskie_pomeshcheniya_prodazha__temp']

    with sqlite3.connect('info.db') as db_sql:
        cursor_lite = db_sql.cursor()

        for table_name in table_list:
            print('Deleting old records', table_name)
            id_list = []    
            cursor_lite.execute(f"SELECT id, valid_to_time FROM {table_name} WHERE not created_time is NULL")
            for id_post, time_post in cursor_lite.fetchall():
                t = str(time_post)[:-6]
                print(t)
                data = time.mktime(time.strptime(t, "%Y-%m-%dT%H:%M:%S"))
                print(t, data)
                if time.time() > data:
                    id_list.append(id_post)
                    print(t, data, 'бан')

            if len(id_list) != 0:
                cursor_lite.execute(f"DELETE FROM {table_name} WHERE id in ({str(id_list)[1:-1]});")
                print(len(id_list))
                db_sql.commit()



    with psycopg2.connect(
            host=config.host,
            user=config.user,
            password=config.password,
            database=config.database
                                ) as db_postgres:
        db_postgres.autocommit = True
        cursor_post = db_postgres.cursor()

        for table_name in table_list:
            print('Deleting old records', table_name)
            id_list = []
            cursor_post.execute(f"SELECT id, valid_to_time FROM {table_name} WHERE not created_time is NULL;")
            # НЕ ЗАБУДЬ ТУТ ПОПРПАВИТЬ

            count = 0
            for id_post, time_post in cursor_post.fetchall():
                t = str(time_post)
                # print(time_post)
                data = time.mktime(time.strptime(t, "%Y-%m-%d %H:%M:%S"))
                # print(data)
                count += 1
                if time.time() > data:
                    id_list.append(int(id_post))
                    print(time_post, 'В бан')

            print(f"{count=}")
            if len(id_list) != 0:
                cursor_post.execute(f"DELETE FROM {table_name} WHERE id in ({str(id_list)[1:-1]});")
            print(len(id_list))

if __name__ == '__main__':
    # db_sqlite_to_PostgreSQL()
    # PostgreSQL_history()
    Delete_info_db()
    
