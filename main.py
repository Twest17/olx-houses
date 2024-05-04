from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
import math
import os
import traceback
import requests.exceptions
import threading
import concurrent.futures

thread_local = threading.local()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

cities = ['odessa', 'ivano-frankovsk', 'vinnitsa', 'lutsk', 'dnepr', 'donetsk', 'zhitomir',
          'uzhgorod', 'zaporozhe', 'kropivnitskiy', 'kiev', 'lugansk', 'lvov', 'nikolaev_106',
          'poltava', 'rovno', 'sumy', 'ternopol', 'kharkov', 'kherson', 'khmelnitskiy',
          'cherkassy', 'chernovtsy', 'chernigov']


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def home_start(city_start=0, house_type_start=0, page_start=1):

    home = 'https://olx.ua'
    i = page_start
    for city_index, city in enumerate(cities, city_start):
        business_start = f'https://www.olx.ua/uk/nedvizhimost/kvartiry/prodazha-kvartir/{city}/?currency=UAH&search%5Bprivate_business%5D=business'
        private_start = f'https://www.olx.ua/uk/nedvizhimost/kvartiry/prodazha-kvartir/{city}/?currency=UAH&search%5Bprivate_business%5D=private'

        for type_index, start_link in enumerate((business_start, private_start), house_type_start):

            count = [26]
            counted = False
            while i < count[0]:
                print(f'page {i} for {city}')
                if i != 1:
                    start_link = f'https://www.olx.ua/uk/nedvizhimost/kvartiry/prodazha-kvartir/{city}/?currency=UAH&page={i}&search%5Bprivate_business%5D=' + start_link.rsplit('=')[-1]
                try:
                    urlhand = requests.get(start_link).text

                    soup = BeautifulSoup(urlhand, 'lxml')

                    if not counted:
                        count_text = soup.find(attrs={'data-testid': "total-count"}).text
                        if len(count_text.split()) > 4:
                            count[0] = math.ceil(1000 / 40) + 1
                        else:
                            count[0] = math.ceil(int(count_text.split()[2]) / 40) + 1
                        counted = True

                    for a in soup('a'):
                        a_cls = a.get('class')
                        if a_cls and a_cls[0] == 'css-z3gu2d':
                            house_links.add(home + a.get('href'))
                except ConnectionError as e:
                    print(e)
                    print(f'city={city_index},house_type={type_index},page={i}')
                    with open('start_values.txt', 'w') as file:
                        file.write(f'{city_index},{type_index},{i},0')
                    time.sleep(10)
                    continue
                except Exception as e:
                    print(e)
                    print(f'city={city_index},house_type={type_index},page={i}')
                    with open('start_values.txt', 'w') as file:
                        file.write(f'{city_index},{type_index},{i},0')
                    exit()
                i += 1
            i = 1
        house_type_start = 0


def get_house_info(link):
    session = get_session()
    try:
        with session.get(link) as response:
            soup = BeautifulSoup(response.text, 'lxml')
            features = []
            if soup.find(attrs={'class': "css-1juynto"}):
                title = soup.find(attrs={'class': "css-1juynto"}).text
            else:
                title = ' '.join(link.split('/')[-1].split('.')[0].split('-')[:-1])

            if ' ' in soup.find_all(attrs={'class': "css-7dfllt"})[-2].text.split(' - ')[-1]:
                city = soup.find_all(attrs={'class': "css-7dfllt"})[-1].text.split()[-1]
            else:
                city = soup.find_all(attrs={'class': "css-7dfllt"})[-2].text.split()[-1]

            house = {'link': link,
                     'title': title,
                     'price': soup.find(attrs={'class': "css-12vqlj3"}).text,
                     'city': city,
                     'location': soup.find_all(attrs={'class': "css-7dfllt"})[-1].text.split()[-1]}

            # feature tags
            for tag in soup.find_all(attrs={'class': "css-b5m1rv er34gjf0"}):

                if all(x not in tag.text for x in ('Інфраструктура', 'Ландшафт', 'оголошення')):
                    features.append(tag.text)

            for i, f in enumerate(features):

                if ': ' not in f:
                    house[f'feature{i}'] = f
                    continue

                name, value = f.split(': ', 1)
                house[name] = value

            if house is not None:
                houses.append(house)

    except (ConnectionError, requests.exceptions.MissingSchema):
        print('Bad URL')
    except IndexError:
        print('probably house is no longer available')
    except AttributeError:
        print(traceback.format_exc())


if __name__ == '__main__':
    start_time = time.time()
    houses = []

    filenames = [entry.name for entry in os.scandir(os.getcwd())]

    if 'start_values.txt' not in filenames:
        with open('start_values.txt', 'w') as file:
            file.write('0,0,1,0')

    with open('start_values.txt', 'r') as file:
        city_start, house_type_start, page_start, link_index = [int(i) for i in file.read().split(',')]

    # print(city_start, house_type_start, page_start, link_index)

    if 'house_links.txt' not in filenames:
        # use set to eliminate duplicates. later we use file to save exact order, because set give values unpredictably
        house_links = set()
        home_start(city_start, house_type_start, page_start)
        print('time for get all links:', time.time() - start_time)

        print('count links:', len(house_links))

        # write links to the file and then read from it to get exact order of the links
        with open('house_links.txt', 'w') as file:
            for link in house_links:
                file.write(link+'\n')

    with open('house_links.txt', 'r') as file:
        house_links = [line.rstrip('\n') for line in file]

    house_links = list(house_links)
    print('total number of links:', len(house_links))

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(get_house_info, house_links[link_index:])

    # for link_number, link in enumerate(house_links[link_index:2], link_index):
    #     print(link_number)
    #     try:
    #         house_dict = get_house_info(link)
    #         if house_dict is None:
    #             continue
    #         houses.append(house_dict)
    #     except AttributeError:
    #         print(traceback.format_exc())
    #         continue
    #     except:
    #         print(traceback.format_exc())
    #         with open('start_values.txt', 'w') as file:
    #             file.write(f'0,0,1,{link_number}')
    #         break

    df = pd.DataFrame(houses)

    if 'houses_olx.csv' in filenames:
        df_prev = pd.read_csv('houses_olx.csv', index_col=0)
        df = pd.concat([df_prev, df], axis=0, ignore_index=True)

    df.to_csv('houses_olx.csv')

    print('total time:', time.time() - start_time)
