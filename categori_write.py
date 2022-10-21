



# Стоянки и гаражи +
def garazhi_stoyanki(resp, table_name, db, cursor, id_2):
    print('Стоянки и гаражи')
    for data in resp["data"]:
        OUT_json = {
            'id': '',
            'id_2': '',
            'url': '',
            'created_time': '',
            'valid_to_time': '',
            'comission': '',
            'price': '',
            'currency': '',
            'user_id': '',
            'user_name': '',
            'b2c_business_page': '',
            'status': '',
            'premium_ad_page': '',
            'garage_type': '',
            'purpose_of_garage': '',
            'area': '',
            'lat': '',
            'lon': '',
            'zoom': '',
            'radius': '',
            'city': '',
            'district': '',
            'region': '',
            'quantity_of_spaces': '',
            'business': '',
            'b2c_ad_page': ''

        }

        OUT_json['id_2'] = id_2
        OUT_json['id'] = int(data.get('id'))
        OUT_json['url'] = data.get('url')
        OUT_json['title'] = data.get('title')
        OUT_json['created_time'] = data.get('created_time')
        OUT_json['valid_to_time'] = data.get('valid_to_time')

        for params in data.get('params'):

            if params["key"] == 'price':
                OUT_json['price'] = params.get('value', {}).get('value')
                OUT_json['currency'] = params.get('value', {}).get('currency')

            elif params["key"] == "total_area":
                OUT_json['total_area'] = params.get('value', {}).get('key')

            elif params["key"] == "year_of_construction_sale":
                OUT_json['year_of_construction_sale'] = params.get('value', {}).get('key')

            elif params["key"] == "year_of_construction_rent":
                OUT_json['year_of_construction_rent'] = params.get('value', {}).get('key')

            elif params["key"] == "land_area":
                OUT_json['land_area'] = params.get('value', {}).get('key')

            elif params['key'] == 'more_premises':
                OUT_json['more_premises'] = params.get('value', {}).get('label')

            else:
                OUT_json[params["key"]] = params.get('value', {}).get('label')

        OUT_json['busines'] = data.get('busines', '')
        OUT_json['business'] = data.get('business', '')

        OUT_json['user_id'] = int(data.get('user', {}).get('id'))
        OUT_json['user_name'] = data.get('user', {}).get('name')
        OUT_json['urgent'] = data.get('promotion', {}).get('urgent', '')

        OUT_json['lat'] = float(data.get('map', {}).get('lat'))
        OUT_json['lon'] = float(data.get('map', {}).get('lon'))


        OUT_json['zoom'] = float(data.get('map', {}).get('zoom'))
        OUT_json['radius'] = float(data.get('map', {}).get('radius'))

        OUT_json['city'] = data.get('location', {}).get('city', {}).get('name')
        OUT_json['district'] = data.get('location', {}).get('district', {}).get('name')
        OUT_json['region'] = data.get('location', {}).get('region', {}).get('name')

        OUT_json['b2c_ad_page'] = data.get('promotion', {}).get('b2c_ad_page')
        OUT_json['premium_ad_page'] = data.get('promotion', {}).get('premium_ad_page')


        OUT_json['b2c_business_page'] = data.get('user', {}).get('b2c_business_page')




        OUT_json['status'] = data.get('status', '')

        OUT_json['premium_ad_page'] = data.get('promotion', {}).get('premium_ad_page', '')
        # print(OUT_json['city'])

        keys = ['id', 'id_2', 'url', 'created_time', 'valid_to_time', 'comission',
                'price', 'currency', 'user_id', 'user_name', 'b2c_business_page',
                'status', 'premium_ad_page', 'garage_type', 'purpose_of_garage', 'area',
                'lat', 'lon', 'zoom', 'radius', 'city', 'district', 'region',
                'quantity_of_spaces', 'business', 'b2c_ad_page']




        out_clov = [OUT_json[key] for key in keys]
        print(f"{keys=}")
        print(f"{out_clov=}")

        cursor.execute(f'SELECT id from {table_name} WHERE id={int(data.get("id"))};')

        if cursor.fetchall():
            db.execute(f"""UPDATE {table_name} SET                          
                            id=?,
                            id_2=?,
                            url=?,
                            created_time=?,
                            valid_to_time=?,
                            comission=?,
                            price=?,
                            currency=?,
                            user_id=?,
                            user_name=?,
                            b2c_business_page=?,
                            status=?,
                            premium_ad_page=?,
                            garage_type=?,
                            purpose_of_garage=?,
                            area=?,
                            lat=?,
                            lon=?,
                            zoom=?,
                            radius=?,
                            city=?,
                            district=?,
                            region=?,
                            quantity_of_spaces=?,
                            business=?,
                            b2c_ad_page=?
                            WHERE id={OUT_json['id']};""", out_clov)

            db.commit()
            print('обновили')
        else:
            try:
                db.execute(
                    f'INSERT INTO {table_name} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);',
                    out_clov)

                print('записали в бд')
            except:
                print('уже есть')

        db.commit()



# Дома +
def doma(resp, table_name, db, cursor, id_2):
    print('doma')
    for data in resp["data"]:
        OUT_json = {
            'id': '',
            'url': '',
            'created_time': '',
            'valid_to_time': '',
            'number_of_rooms': '',
            'urgent': '',
            'total_floors': '',
            'house_type': '',
            'wc_house': '',
            'furnished_house': '',
            'more_house': '',
            'near_is': '',
            'house_repairs': '',
            'comission': '',
            'price': '',
            'currency': '',
            'user_id': '',
            'user_name': '',
            'lat': '',
            'lon': '',
            'zoom': '',
            'radius': '',
            'city': '',
            'district': '',
            'region': '',
            'year_of_construction_rent': '',
            'year_of_construction_sale': '',
            'ceiling_height': '',
            'total_area': '',
            'b2c_business_page': '',
            'status': '',
            'premium_ad_page': '',
            'business': '',
            'plot': '',
            'private_house_type': '',
            'water': '',
            'heating': '',
            'gas': '',
            'total_living_area': '',
            'electricity': '',
            'wall_type': '',
            'b2c_ad_page': '',
            'repairs': '',
            'rent_from': ''

        }
        OUT_json['id_2'] = id_2
        OUT_json['id'] = int(data.get('id'))
        OUT_json['url'] = data.get('url')
        OUT_json['title'] = data.get('title')
        OUT_json['created_time'] = data.get('created_time')
        OUT_json['valid_to_time'] = data.get('valid_to_time')

        for params in data.get('params'):

            if params["key"] == 'price':
                OUT_json['price'] = params.get('value', {}).get('value')
                OUT_json['currency'] = params.get('value', {}).get('currency')

            elif params["key"] == "total_area":
                OUT_json['total_area'] = params.get('value', {}).get('key')

            elif params["key"] == "year_of_construction_sale":
                OUT_json['year_of_construction_sale'] = params.get('value', {}).get('key')

            elif params["key"] == "year_of_construction_rent":
                OUT_json['year_of_construction_rent'] = params.get('value', {}).get('key')

            elif params["key"] == "land_area":
                OUT_json['land_area'] = params.get('value', {}).get('key')

            elif params['key'] == 'more_premises':
                OUT_json['more_premises'] = params.get('value', {}).get('label')

            else:
                OUT_json[params["key"]] = params.get('value', {}).get('label')

        OUT_json['busines'] = data.get('busines', '')
        OUT_json['business'] = data.get('business', '')

        OUT_json['user_id'] = int(data.get('user', {}).get('id'))
        OUT_json['user_name'] = data.get('user', {}).get('name')
        OUT_json['urgent'] = data.get('promotion', {}).get('urgent', '')

        try:
            OUT_json['lat'] = float(data.get('map', {}).get('lat'))
            OUT_json['lon'] = float(data.get('map', {}).get('lon'))


            OUT_json['zoom'] = float(data.get('map', {}).get('zoom'))
            OUT_json['radius'] = float(data.get('map', {}).get('radius'))

        except:
            print('ошибка float')

        OUT_json['city'] = data.get('location', {}).get('city', {}).get('name')
        OUT_json['district'] = data.get('location', {}).get('district', {}).get('name')
        OUT_json['region'] = data.get('location', {}).get('region', {}).get('name')

        OUT_json['b2c_ad_page'] = data.get('promotion', {}).get('b2c_ad_page')
        OUT_json['premium_ad_page'] = data.get('promotion', {}).get('premium_ad_page')


        OUT_json['b2c_business_page'] = data.get('user', {}).get('b2c_business_page')




        OUT_json['status'] = data.get('status', '')

        OUT_json['premium_ad_page'] = data.get('promotion', {}).get('premium_ad_page', '')
        # print(OUT_json['city'])

        keys = ['id', 'id_2', 'url', 'created_time', 'valid_to_time',
                'number_of_rooms', 'urgent', 'total_floors', 'house_type',
                'wc_house', 'furnished_house', 'more_house', 'near_is',
                'house_repairs', 'comission', 'price', 'currency',
                'user_id', 'user_name', 'lat', 'lon', 'zoom', 'radius',
                'city', 'district', 'region', 'year_of_construction_rent',
                'year_of_construction_sale', 'ceiling_height', 'total_area',
                'b2c_business_page', 'status', 'premium_ad_page',
                'business', 'plot', 'private_house_type', 'water', 'heating',
                'gas', 'total_living_area', 'electricity', 'wall_type', 'b2c_ad_page',
                'repairs', 'rent_from']




        out_clov = [OUT_json[key] for key in keys]
        print(f"{keys=}")
        print(f"{out_clov=}")

        cursor.execute(f'SELECT id from {table_name} WHERE id={int(data.get("id"))};')

        if cursor.fetchall():
            db.execute(f"""UPDATE {table_name} SET                          
                            id=?,
                            id_2=?,
                            url=?,
                            created_time=?,
                            valid_to_time=?,    
                            number_of_rooms=?,
                            urgent=?,
                            total_floors=?,
                            house_type=?,
                            wc_house=?,
                            furnished_house=?,
                            more_house=?,
                            near_is=?,
                            house_repairs=?,
                            comission=?,
                            price=?,
                            currency=?,
                            user_id=?,
                            user_name=?,
                            lat=?,
                            lon=?,
                            zoom=?,
                            radius=?,
                            city=?,
                            district=?,
                            region=?,
                            year_of_construction_rent=?,
                            year_of_construction_sale=?,
                            ceiling_height=?,
                            total_area=?,
                            b2c_business_page=?,
                            status=?,
                            premium_ad_page=?,
                            business=?,
                            plot=?,
                            private_house_type=?,
                            water=?,
                            heating=?,
                            gas=?,
                            total_living_area=?,
                            electricity=?,
                            wall_type=?,
                            b2c_ad_page=?,
                            Repairs=?,
                            rent_from=?
                            WHERE id={OUT_json['id']};""", out_clov)

            db.commit()
            print("Данные записаны")
        else:
            try:
                db.execute(
                f'INSERT INTO {table_name} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);',
                out_clov)
                print("Данные записаны")
            except:
                print('уже есть')

        db.commit()



# Коммерческие помещения +
def Commercial_premises(resp, table_name, db, cursor, id_2):
    print('kommercheskie_pomeshcheniya')
    for data in resp["data"]:
        OUT_json = {
            'id': '',
            'id_2': '',
            'url': '',
            'created_time': '',
            'valid_to_time': '',
            'urgent': '',
            'b2c_ad_page': '',
            'b2c_business_page': '',
            'premium_ad_page': '',
            'price': '',
            'currency': '',
            'premise_type': '',
            'user_id': '',
            'user_name': '',
            'total_area': '',
            'effective_area': '',
            'premises_location': '',
            'floor': '',
            'total_floors': '',
            'ceiling_height': '',
            'repairs': '',
            'more_premises': '',
            'parking_lot': '',
            'comission': '',
            'business': '',
            'status': '',
            'zoom': '',
            'lat': '',
            'lon': '',
            'radius': '',
            'city': '',
            'region': '',
            'land': '',
            'district': ''
        }

        OUT_json['id_2'] = id_2
        OUT_json['id'] = int(data.get('id'))
        OUT_json['url'] = data.get('url')
        OUT_json['title'] = data.get('title')
        OUT_json['created_time'] = data.get('created_time')
        OUT_json['valid_to_time'] = data.get('valid_to_time')

        for params in data.get('params'):

            if params["key"] == 'price':
                OUT_json['price'] = params.get('value', {}).get('value')
                OUT_json['currency'] = params.get('value', {}).get('currency')

            elif params["key"] == "total_area":
                OUT_json['total_area'] = params.get('value', {}).get('key')

            elif params["key"] == "land_area":
                OUT_json['land_area'] = params.get('value', {}).get('key')

            elif params['key'] == 'more_premises':
                OUT_json['more_premises'] = params.get('value', {}).get('label')

            else:
                OUT_json[params["key"]] = params.get('value', {}).get('label')

        OUT_json['busines'] = data.get('busines', '')
        OUT_json['business'] = data.get('business', '')

        OUT_json['user_id'] = int(data.get('user', {}).get('id'))
        OUT_json['user_name'] = data.get('user', {}).get('name')
        OUT_json['urgent'] = data.get('promotion', {}).get('urgent', '')

        OUT_json['lat'] = float(data.get('map', {}).get('lat'))
        OUT_json['lon'] = float(data.get('map', {}).get('lon'))


        OUT_json['zoom'] = float(data.get('map', {}).get('zoom'))
        OUT_json['radius'] = float(data.get('map', {}).get('radius'))

        OUT_json['city'] = data.get('location', {}).get('city', {}).get('name')
        OUT_json['district'] = data.get('location', {}).get('district', {}).get('name')
        OUT_json['region'] = data.get('location', {}).get('region', {}).get('name')

        OUT_json['b2c_ad_page'] = data.get('promotion', {}).get('b2c_ad_page')
        OUT_json['premium_ad_page'] = data.get('promotion', {}).get('premium_ad_page')


        OUT_json['b2c_business_page'] = data.get('user', {}).get('b2c_business_page')




        OUT_json['status'] = data.get('status', '')

        OUT_json['premium_ad_page'] = data.get('promotion', {}).get('premium_ad_page', '')
        # print(OUT_json['city'])

        keys = ['id', 'id_2', 'url', 'created_time', 'valid_to_time', 'urgent', 'b2c_ad_page',
                'b2c_business_page', 'premium_ad_page', 'price', 'currency', 'premise_type',
                'user_id', 'user_name', 'total_area', 'effective_area', 'premises_location',
                'floor', 'total_floors', 'ceiling_height', 'repairs', 'more_premises', 'parking_lot',
                'comission', 'business', 'status', 'zoom', 'lat', 'lon', 'radius',
                'city', 'region', 'land', 'district']





        out_clov = [OUT_json[key] for key in keys]
        print(f"{keys=}")
        print(f"{out_clov=}")

        cursor.execute(f'SELECT id from {table_name} WHERE id={int(data.get("id"))};')

        if cursor.fetchall():
            db.execute(f"""UPDATE {table_name} SET
                            id=?,
                            id_2=?,
                            url=?,
                            created_time=?,
                            valid_to_time=?,
                            urgent=?,
                            b2c_ad_page=?,
                            b2c_business_page=?,
                            premium_ad_page=?,
                            price=?,
                            currency=?,
                            premise_type=?,
                            user_id=?,
                            user_name=?,
                            total_area=?,
                            effective_area=?,
                            premises_location=?,
                            floor=?,
                            total_floor=?,
                            ceiling_height=?,
                            repairs=?,
                            more_premise=?,
                            parking_lot=?,
                            comission=?,
                            business=?,
                            status=?,
                            zoom=?,
                            lat=?,
                            lon=?,
                            radius=?,
                            city=?,
                            region=?,
                            land=?,
                            district=?
                            WHERE id={data.get('id')};""", out_clov)
            db.commit()
            print("Данные записаны")
        else:
            try:
                db.execute(
                f'INSERT INTO {table_name} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);',
                out_clov)
                print("Данные записаны")
            except:
                print('уже есть')

        db.commit()


# земля +
def land(resp, table_name, db, cursor, id_2):
    print('zemlja')
    for data in resp["data"]:
        OUT_json = {
            'id': '',
            'url': '',
            'created_time': '',
            'valid_to_time':'',
            'comission': '',
            'price': '',
            'currency': '',
            'user_id': '',
            'user_name': '',
            'b2c_ad_page':'',
            'status':'',
            'premium_ad_page':'',
            'land_area':'',
            'purpose':'',
            'location':'',
            'type_of_plot':'',
            'communications':'',
            'commission':'',
            'city': '',
            'district': '',
            'region':'',
            'busines':'',
            'zoom': '',
            'lat': '',
            'lon': '',
            'radius':'',
            'urgent':'',
            'b2c_business_page':''
        }
        OUT_json['id_2'] = id_2
        OUT_json['id'] = int(data.get('id'))
        OUT_json['url'] = data.get('url')
        OUT_json['title'] = data.get('title')
        OUT_json['created_time'] = data.get('created_time')
        OUT_json['valid_to_time'] = data.get('valid_to_time')

        for params in data.get('params'):

            if params["key"] == 'price':
                OUT_json['price'] = params.get('value', {}).get('value')
                OUT_json['currency'] = params.get('value', {}).get('currency')

            elif params["key"] == "total_area":
                OUT_json['total_area'] = params.get('value', {}).get('key')

            elif params["key"] == "land_area":
                OUT_json['land_area'] = params.get('value', {}).get('key')

            else:
                OUT_json[params["key"]] = params.get('value', {}).get('label')

        OUT_json['busines'] = data.get('busines', '')

        OUT_json['user_id'] = int(data.get('user', {}).get('id'))
        OUT_json['user_name'] = data.get('user', {}).get('name')

        OUT_json['lat'] = float(data.get('map', {}).get('lat'))
        OUT_json['lon'] = float(data.get('map', {}).get('lon'))

        OUT_json['zoom'] = float(data.get('map', {}).get('zoom'))
        OUT_json['radius'] = float(data.get('map', {}).get('radius'))

        OUT_json['city'] = data.get('location', {}).get('city', {}).get('name')
        OUT_json['district'] = data.get('location', {}).get('district', {}).get('name')
        OUT_json['region'] = data.get('location', {}).get('region', {}).get('name')

        OUT_json['b2c_ad_page'] = data.get('promotion', {}).get('b2c_ad_page')
        OUT_json['premium_ad_page'] = data.get('promotion', {}).get('premium_ad_page')


        OUT_json['b2c_business_page'] = data.get('user', {}).get('b2c_business_page')




        OUT_json['status'] = data.get('status', '')

        OUT_json['premium_ad_page'] = data.get('promotion', {}).get('premium_ad_page', '')
        # print(OUT_json['city'])

        keys = ['id', 'id_2', 'url', 'created_time', 'valid_to_time',
                'comission', 'price', 'currency', 'user_id', 'user_name',
                'b2c_ad_page', 'status', 'premium_ad_page', 'land_area',
                'purpose', 'location', 'type_of_plot', 'communications',
                'commission', 'city', 'district', 'region', 'busines',
                'zoom', 'lat', 'lon', 'radius', 'urgent', 'b2c_business_page']



        out_clov = [OUT_json[key] for key in keys]
        print(f"{keys=}")
        print(f"{out_clov=}")

        cursor.execute(f'SELECT id from {table_name} WHERE id={int(data.get("id"))};')

        if cursor.fetchall():
            db.execute(f"""UPDATE {table_name} SET
                                id=?,
                                id_2=?,
                                url=?,
                                created_time=?,
                                valid_to_time=?,
                                comission=?,
                                price=?,
                                currency=?,
                                user_id=?,
                                user_name=?,
                                b2c_ad_page=?,
                                status=?,
                                premium_ad_page=?,
                                land_area=?,
                                purpose=?,
                                location=?,
                                type_of_plot=?,
                                communications=?,
                                commission=?,
                                city=?,
                                district=?,
                                region=?,
                                busines=?,
                                zoom=?,
                                lat=?,
                                lon=?,
                                radius=?,
                                urgent=?,
                                b2c_business_page=?
                                WHERE id={data.get('id')};""", out_clov)

            db.commit()
            print("Данные записаны")
        else:
            db.execute(
                f'INSERT INTO {table_name} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);',
                out_clov)
            print("Данные записаны")

        db.commit()



# квартиры +
def apartments(resp, table_name, db, cursor, id_2):
    print('apartments')
    for data in resp["data"]:
        OUT_json = {
            'id': '',
            'url': '',
            'created_time': '',
            'valid_to_time':'',
            'type_of_market': '',
            'number_of_rooms': '',
            'urgent': '',
            'floor': '',
            'total_floors': '',
            'house_type': '',
            'layout': '',
            'wc': '',
            'furnished': '',
            'more': '',
            'near_is': '',
            'repairs': '',
            'comission': '',
            'price': '',
            'currency': '',
            'user_id': '',
            'user_name': '',
            'lat': '',
            'lon': '',
            'zoom':'',
            'radius':'',
            'city': '',
            'district': '',
            'region': '',
            'year_of_construction_sale':'',
            'ceiling_height':'',
            'total_area':'',
            'b2c_business_page':'',
            'status':'',
            'premium_ad_page':''
        }
        OUT_json['id_2'] = id_2
        OUT_json['id'] = data.get('id')
        OUT_json['url'] = data.get('url')
        OUT_json['title'] = data.get('title')
        OUT_json['created_time'] = data.get('created_time')
        OUT_json['valid_to_time'] = data.get('valid_to_time')

        for params in data.get('params'):

            if params["key"] == 'price':
                OUT_json['price'] = params.get('value', {}).get('value')
                OUT_json['currency'] = params.get('value', {}).get('currency')

            elif params["key"] == "total_area":
                OUT_json['total_area'] = params.get('value', {}).get('key')

            elif params["key"] == "year_of_construction_sale":
                OUT_json['year_of_construction_sale'] = params.get('value', {}).get('key')

            elif params["key"] == "year_of_construction_rent":
                OUT_json['year_of_construction_rent'] = params.get('value', {}).get('key')


            else:
                OUT_json[params["key"]] = params.get('value', {}).get('label')

        OUT_json['user_id'] = int(data.get('user', {}).get('id'))
        OUT_json['user_name'] = data.get('user', {}).get('name')

        OUT_json['urgent'] = data.get('promotion', {}).get('urgent', '')

        OUT_json['premium_ad_page'] = data.get('promotion', {}).get('premium_ad_page', '')

        # print(f"{data.get('promotion', {})=}\n\n\n\n{data.get('promotion', {}).get('premium_ad_page', '')=}")

        try:
            OUT_json['lat'] = float(data.get('map', {}).get('lat'))
            OUT_json['lon'] = float(data.get('map', {}).get('lon'))

            OUT_json['zoom'] = float(data.get('map', {}).get('zoom'))
            OUT_json['radius'] = float(data.get('map', {}).get('radius'))
        except:
            print('error float', data.get('id'))

        OUT_json['city'] = data.get('location', {}).get('city', {}).get('name')
        OUT_json['district'] = data.get('location', {}).get('district', {}).get('name')
        OUT_json['region'] = data.get('location', {}).get('region', {}).get('name')

        OUT_json['b2c_business_page'] = data.get('user', {}).get('b2c_business_page')




        OUT_json['status'] = data.get('status', '')



        # print(OUT_json['city'])

        keys = ['id', 'id_2', 'url', 'created_time', 'valid_to_time',
                'type_of_market', 'number_of_rooms', 'urgent', 'floor',
                'total_floors', 'house_type', 'layout', 'wc', 'furnished',
                'more', 'near_is', 'repairs', 'comission', 'price', 'currency',
                'user_id', 'user_name', 'lat', 'lon', 'zoom', 'radius', 'city',
                'district', 'region', 'year_of_construction_sale', 'ceiling_height',
                'total_area', 'b2c_business_page', 'status', 'premium_ad_page']


        out_clov = [OUT_json[key] for key in keys]
        print(f"{keys=}")
        print(f"{out_clov=}")

        cursor.execute(f'SELECT id from {table_name} WHERE id={int(data.get("id"))};')

        if cursor.fetchall():
            # print('лвоикплочк'*10, data.get("id"))
            db.execute(f"""UPDATE {table_name} SET
                                id=?,
                                id_2=?,
                                url=?,
                                created_time=?,
                                valid_to_time=?,
                                type_of_market=?,
                                number_of_rooms=?,
                                urgent=?,
                                floor=?,
                                total_floors=?,
                                house_type=?,
                                layout=?,
                                wc=?,
                                furnished=?,
                                more=?,
                                near_is=?,
                                repairs=?,
                                comission=?,
                                price=?,
                                currency=?,
                                user_id=?,
                                user_name=?,
                                lat=?,
                                lon=?,
                                zoom=?,
                                radius=?,
                                city=?,
                                district=?,
                                region=?,
                                year_of_construction_sale=?,
                                ceiling_height=?,
                                total_area=?,
                                b2c_business_page=?,
                                status=?,
                                premium_ad_page=?
                                WHERE id={int(OUT_json['id'])};""", out_clov)
            print("Данные записаны")
        else:
            try:
                db.execute(f'INSERT INTO {table_name} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);', out_clov)
                print("Данные записаны")
            except:
                print('Уже есть')


        db.commit()
