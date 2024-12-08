import asyncio
import aiohttp
import aiomysql.connection
import random as rd
import json
import config
import sys

start = int(input("Start: "))
end = int(input("End:"))
name_db = input("Data base name: ")
name_tb = input("Table name: ")

if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
    policy = asyncio.WindowsSelectorEventLoopPolicy()
    asyncio.set_event_loop_policy(policy)


def save_json(name, data):
    with open(name, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


def read_json(name):
    with open(name, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data


headers = read_json('headers.json')


async def get_json(client, url):
    while True:
        try:
            response = await client.get(url, headers=rd.choice(headers))
        except Exception as er:
            print(response.status)
            await asyncio.sleep(0.1)
        else:
            break
    assert response.status == 200
    return await response.json(content_type=None, encoding='utf-8')


def get_inf(data):

    result_list = {}

    for key in data.keys():
        if isinstance(data[key], list):
            for it in data[key]:
                keyss = it.keys()
                for jkey in keyss:
                    if isinstance(it[jkey], dict):
                        for ikey in it[jkey].keys():
                            name_key = f'{key}_{jkey}_{ikey}'
                            result_list[name_key] = it[jkey][ikey]
                    else:
                        name_key = f'{key}_{jkey}'
                        if name_key not in result_list.keys():
                            name_key = f'{key}_{jkey}'
                            result_list[name_key] = [it[jkey]]
                        else:
                            result_list[name_key].append(it[jkey])
        elif isinstance(data[key], dict):
            for ki in data[key].keys():
                result_list[f'{key}_{ki}'] = data[key][ki]
        else:
            result_list[key] = data[key]
    # print(result_list)
    for key in result_list.keys():
        if isinstance(result_list[key], list):
            if len(result_list[key]) > 1:
                result_list[key] = ','.join([str(result_list[key][j]) for j in range(len(result_list[key]))])
            else:
                result_list[key] = str(result_list[key][0])
            # print(result_list[key])

    return tuple(result_list.keys()), tuple(result_list.values())

def get_table_keys():
    product = read_json('product.json')['data']['products'][0]
    data_keys = {}
    data_keys['i'] = 'INT AUTO_INCREMENT PRIMARY KEY'
    keys, values = get_inf(product)

    for j in range(len(keys)):
        if isinstance(values[j], int):
            data_keys[keys[j]] = 'INT(100)'
        elif isinstance(values[j], str):
            data_keys[keys[j]] = 'VARCHAR(100)'
    save_json('keys.json', data_keys)

# get_table_keys()


def create_db():
    return f'create database if not exists {name_db}'


def create_table():
    keys = read_json('keys.json')
    k = ', '.join([f"{key} {keys[key]}" for key in keys.keys()])
    create_table_sql = f"USE {name_db}; CREATE TABLE IF NOT EXISTS {name_tb} ({k});"
    # create_table_sql = f"USE {name_db}; CREATE TABLE {name_tb} ({k});"
    # print(create_table_sql)
    return create_table_sql


def insert_data(keys, values):
    keys = ', '.join(keys)
    values = ['%s']*len(values)
    values = ', '.join(values)
    return f"use {name_db}; insert into {name_tb} ({keys}) values ({values})"
    

async def get_data(client, url, pool):
    json_data = await asyncio.gather(asyncio.ensure_future(get_json(client, url)))
    if not json_data[0]['data']['products']:
        return None
    else:
        a = json_data[0]['data']['products'][0]
        b = get_inf(a)
        await asyncio.gather(asyncio.ensure_future(insert(insert_data(*b), pool, b[1])))


async def insert(sql, pool, data):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, args=data)
            await conn.commit()


async def get_(loop):
    client = aiohttp.ClientSession(trust_env=True)
    chunk = 200
    tasks = []
    pool = await aiomysql.create_pool(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        loop=loop
    )
    await asyncio.gather(insert(create_db(), pool, None))
    await asyncio.gather(insert(create_table(), pool, None))
    for x in range(start, end):
        tasks.append(asyncio.create_task(get_data(client, f'https://wbxcatalog-ru.wildberries.ru/nm-2-card/catalog?nm={x}&locale=ru', pool)))
        if len(tasks) == chunk:
            await asyncio.gather(*tasks)
            tasks = []
    if len(tasks) != 0:
        await asyncio.gather(*tasks)
    await client.close()


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_(loop))


if __name__ == '__main__':
    main()
