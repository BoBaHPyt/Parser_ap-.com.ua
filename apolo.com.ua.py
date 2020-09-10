from asyncio import run, gather
from aiohttp import ClientSession
from json_dump import open_df
from csv import writer
from json import load
from lxml.html import fromstring, tostring
from tqdm import tqdm


DUMP_FILE = 'apolo.com.ua.json'
RESULT_FILE = 'apolo.com.ua.csv'
NUMS_THREADS = 10


async def get(url, **kwargs):
    async with ClientSession() as sess:
        async with sess.get(url, **kwargs) as req:
            return await req.text()


async def get_all_page_category(url_category):
    urls = []

    content_page = await get(url_category)

    document = fromstring(content_page)

    last_page = document.xpath('//ul[@class="pagination"]/li[last()]/a/@href')
    if last_page:
        last_page = int(last_page[0].split('=')[-1])
    else:
        last_page = 1

    for i in range(1, last_page + 1):
        urls.append(url_category + '?page={}'.format(i))

    return urls


async def get_product_urls_from_category(url_category):
    content_page = await get(url_category)

    document = fromstring(content_page)

    urls = document.xpath('//div[@class="product-name"]/a/@href')
    for i, url in enumerate(urls):
        if 'http' not in url:
            urls[i] = 'https://apolo.com.ua' + url

    return urls


async def parse_product(url_product):
    data = {'url': url_product}
    content_page = await get(url_product)

    document = fromstring(content_page)

    image = document.xpath('//div[@id="image-box"]/a/@href')
    if image:
        data['Изображение'] = image[0]
    else:
        data['Изображение'] = ''


    name = document.xpath('//h1[@itemprop="name"]/text()')
    if name:
        data['Название'] = name[0]
    else:
        data['Название'] = ''

    data['Производитель'] = 'Apple'

    breadcrumb = document.xpath('//ul[@class="breadcrumb"]/li[@itemprop="itemListElement"]/a/span[@itemprop="name"]/text()')
    if len(breadcrumb) > 1:
        data['Категория'] = breadcrumb[-2]
        data['Подкатегория'] = breadcrumb[-1]
    else:
        data['Категория'] = ''
        data['Подкатегория'] = ''

    price = document.xpath('//div[@class="price"]/span/text()')
    if price:
        data['Цена (грн.)'] = price[0].replace('грн.', '')
    else:
        data['Цена (грн.)'] = ''

    desc = document.xpath('//div[@itemprop="description"]')
    if desc:
        desc_tag = tostring(desc[0]).decode()
        desc_doc = fromstring(desc_tag)

        table = desc_doc.xpath('//table')
        for t in table:
            try:
                t.getparent().remove(t)
            except:
                pass
        data['Описание'] = tostring(desc_doc).decode()
    else:
        desc = document.xpath('//div[@itemprop="description"]/text()')
        if desc:
            data['Описание'] = desc[0]
        else:
            data['Описание'] = ''

    characteristics_text = document.xpath('//div[@itemprop="description"]/text()')
    if characteristics_text:
        data['Характеристики'] = '\n'.join(characteristics_text)
    else:
        data['Характеристики'] = ''

    characteristics_table = document.xpath('//*[@id="tab-description"]/table')
    if characteristics_table:
        data['Таблица характеристик'] = tostring(characteristics_table[0]).decode()
    else:
        data['Таблица характеристик'] = ''

    characteristics = document.xpath('//*[@id="tab-description"]/table/tbody/tr/td[@class="cell-4"]/span/text() | '
                                     '//*[@id="tab-description"]/table/tbody/tr/td[@class="cell-8"]//span[1]/text()')
    if not characteristics:
        characteristics = document.xpath('//*[@id="tab-description"]/table/tbody/tr/*[1]/*/text() | '
                                         '//*[@id="tab-description"]/table/tbody/tr/*[2]//span[1]/text()')
        if not characteristics:
            characteristics_c = document.xpath('//*[@id="tab-description"]/table/tbody/tr//text()')
            black_list = document.xpath('//*[@id="tab-description"]/table/tbody/tr//text()[../@colspan or ../../@colspan or ../../../@colspan or ../../../../@colspan]')

            characteristics = []

            for el in characteristics_c:
                if el not in black_list:
                    if el.replace('\r', '').replace('\n', '').replace('\t', ''):
                        characteristics.append(el.replace('\r', '').replace('\n', '').replace('\t', ''))

    if len(characteristics) % 2 == 0:
        next = False
        for i, characteristic_name in enumerate(characteristics[::2]):
            if not characteristics[i * 2 + 1][0] == characteristics[i * 2 + 1][0].upper():
                next = True
        if next:
            for i, characteristic_name in enumerate(characteristics[::2]):
                data[characteristic_name.replace(':', '').replace('\xa0', '')] = characteristics[i * 2 + 1]

    return data



async def main():
    category_urls = ['https://apolo.com.ua/AppleCo/macbook-air',
                     'https://apolo.com.ua/AppleCo/macbook-pro',
                     'https://apolo.com.ua/AppleCo/imac']
    all_category_urls = []
    all_product_urls = []

    for urls in await gather(*[get_all_page_category(url) for url in category_urls]):
        all_category_urls += urls

    for i in tqdm(range(0, len(all_category_urls), NUMS_THREADS)):
        urls = all_category_urls[i: i + NUMS_THREADS] if i + NUMS_THREADS < len(all_category_urls) else all_category_urls[i:]
        answers = await gather(*[get_product_urls_from_category(url) for url in urls])
        for answer in answers:
            all_product_urls += answer

    file = open_df(DUMP_FILE)
    for i in tqdm(range(0, len(all_product_urls), NUMS_THREADS)):
        urls = all_product_urls[i: i + NUMS_THREADS] if i + NUMS_THREADS < len(all_product_urls) else all_product_urls[i:]
        answers = await gather(*[parse_product(url) for url in urls])
        for answer in answers:
            file.write(answer)
    file.close()

    with open(DUMP_FILE, 'r') as file:
        write_to_csv(load(file))


def write_to_csv(data_products):
    default_characteristics = {}

    all_characteristics_name = []
    for product in data_products:  # Получение списка ВСЕХ возможных характеристик
        for name in product.keys():
            if name not in all_characteristics_name:
                all_characteristics_name.append(name)
                default_characteristics[name] = ''

    for i in range(len(data_products)):  # Добавление ВСЕХ характеристик к каждому продукту
        dh = default_characteristics.copy()
        dh.update(data_products[i])
        data_products[i] = dh

    with open(RESULT_FILE, 'w') as file:  # Запись в csv файл
        csv_writer = writer(file, delimiter=';')

        data = []
        for value in data_products[0].keys():
            data.append(value.replace('\n', '').replace('\r', ''))

        csv_writer.writerow(data)

        for product in data_products:
            csv_writer.writerow(product.values())


if __name__ == '__main__':
    run(main())
