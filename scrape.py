import requests
import json
from bs4 import BeautifulSoup
import xml.etree.ElementTree
import re
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy import Table, Column
from sqlalchemy import Integer, String, MetaData, Float
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import select, text

"""
Scrape Sephora Products on Aug 11th 2017
"""


def make_db_conn():
    engine = create_engine(
        'postgresql://ubuntu:ubuntu@ec2-13-59-36-9.us-east-2.compute.amazonaws.com:5432/sephora')
    conn = engine.connect()
    return conn


def get_sephora_product_table():
    metadata = MetaData()
    return Table('sephora_product', metadata,
                 Column('id', Integer),
                 Column('product_url', String),
                 Column('sku', Integer),
                 Column('category', String),
                 Column('brand', String),
                 Column('name', String),
                 Column('rating', Float),
                 Column('detail_text', String),
                 Column('size_oz', Integer),
                 Column('price', Integer))


def make_soup(url):
    headers = {
        'user-agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:32.0) '
                       'Gecko/20100101 Firefox/32.0')}
    response = requests.get(url, headers=headers)
    print('Fetched ', url)
    page = response.text
    soup = BeautifulSoup(page, "lxml")
    return soup


def make_sephora_review_soup(product_id, page):
    review_url = 'http://reviews.sephora.com/8723abredes/{}/reviews.htm?format=embedded&page={}'.format(
        product_id, page)
    soup = make_soup(review_url)
    return soup


def scrape_product_reviews(product_id):
    page = 1
    soup = make_sephora_review_soup(product_id, page)
    total_review_count = 0
    number_reviews_tag = soup.find('div', id='BVRRCustomRatingCountId')
    if number_reviews_tag:
        total_review_count = number_reviews_tag.find(
            'span', class_='BVRRNumber').text
        total_review_count = int(total_review_count)

    if total_review_count == 0:
        return None

    curr_review_count = 0
    reviews = []

    while (curr_review_count) < 200 and (
            curr_review_count < total_review_count):
        reviews_grid = soup.find('div', id='BVRRContentContainerID')
        reviews_tag = reviews_grid.findAll('span', {'itemprop': 'review'})
        print('got ', curr_review_count)
        for review_tag in reviews_tag:
            curr_review_count += 1
            age_range = skin_type = skin_tone = eye_color = None
            review_text = reviewer_username = tags = title = None

            review_text = review_tag.find('span', class_='BVRRReviewText').text
            title = review_tag.find('span', class_='BVRRReviewTitle').text
            rating = review_tag.find('span', class_='BVRRRatingNumber').text

            tags_tag = review_tag.find(
                'span', class_='BVRRReviewProTags')
            if tags_tag:
                tags_text = tags_tag.text
                tags_text = tags_text.replace('"', '')
                tags = tags_text.split(',')
                tags = [t.strip() for t in tags]

            username_tag = review_tag.find(
                'span', class_='BVRRNickname')
            if username_tag:
                reviewer_username = username_tag.text.strip()

            skin_type_tag = review_tag.find(
                'span', class_='BVRRContextDataValueskinType')
            if skin_type_tag:
                skin_type = skin_type_tag.text.strip().lower()

            skin_tone_tag = review_tag.find(
                'span', class_='BVRRContextDataValueskinTone')
            if skin_tone_tag:
                skin_tone = skin_tone_tag.text.strip().lower()

            eye_color_tag = review_tag.find(
                'span', class_='BVRRContextDataValueeyeColor')
            if eye_color_tag:
                eye_color = eye_color_tag.text.strip().lower()

            if (review_text is not None):
                review = {'product_id': product_id,
                          'review_text': review_text,
                          'review_title': title,
                          'rating': rating,
                          'age_range': age_range,
                          'skin_type': skin_type,
                          'skin_tone': skin_tone,
                          'eye_color': eye_color,
                          'reviewer_username': reviewer_username,
                          'tags': tags}
                reviews.append(review)
        # Get next review page
        page += 1
        soup = make_sephora_review_soup(product_id, page)

    return reviews


def store_sephora_product_reviews():
    conn = make_db_conn()
    metadata = MetaData()
    sephora_product_review_table = Table(
        'sephora_product_review', metadata,
        Column('product_id', String),
        Column('review_title', String),
        Column('review_text', String),
        Column('rating', Integer),
        Column('age_range', String),
        Column('skin_type', String),
        Column('skin_tone', String),
        Column('eye_color', String),
        Column('reviewer_username', String),
        Column('tags', ARRAY(String))
    )

    products_query = """SELECT id FROM sephora_product WHERE category IN
        ('moisturizers', 'face serums', 'face wash & cleansers',
         'eye creams & treatments', 'face masks', 'moisturizer & treatments',
         'face oils') ORDER BY id"""
    s = text(products_query)
    product_query_results = conn.execute(s).fetchall()
    product_ids = [p[0] for p in product_query_results]

    for i, product_id in enumerate(product_ids):
        print('Processing {}: {}'.format(i, product_id))
        results = scrape_product_reviews(product_id)
        print('Storing {} results for {}: {}...'.format(
            len(results), i, product_id))
        conn.execute(sephora_product_review_table.insert(), results)


def scrape_product(url):
    soup = make_soup(url)

    # Must have product id
    product_id_tag = soup.find('meta', {'property': 'product:id'})
    product_id = None
    if product_id_tag:
        product_id = product_id_tag['content']
    else:
        return None

    # Details Text
    details_tag = soup.find('div', id='details')
    details_text = None
    if details_tag:
        details_text = details_tag.text.strip()
        details_text = details_text.replace('\n', ' ').replace('\r', ' ')
        details_text = ' '.join(details_text.split())

    # Category
    category = None
    breadcrumb_tag = soup.find('li', class_='Breadcrumb-item--current')
    if breadcrumb_tag:
        category = breadcrumb_tag.text.strip().lower()

    # Name, Brand, Price, SKU, URL
    price = brand = name = sku = url = rating = sku_oz = None

    product_content_tag = soup.find('script', {'data-entity': 'Sephora.Sku'})
    if product_content_tag:
        product_json_text = product_content_tag.text
        product_obj = json.loads(product_json_text)
        price = product_obj.get('list_price', None)
        sku = int(product_obj.get('sku_number', None))
        product_primary_obj = product_obj.get('primary_product', {})
        brand = product_primary_obj.get('brand_name', None)
        rating = product_primary_obj.get('rating', None)
        name = product_primary_obj.get('display_name', None)
        url = product_primary_obj.get('product_url', None)
        sku_size = product_obj.get('sku_size', None)
        if sku_size:
            sku_oz_groups = re.search(r'([\d\.]*) oz', sku_size)
            if sku_oz_groups:
                try:
                    sku_oz_str = sku_oz_groups.group(1)
                    sku_oz = float(sku_oz_str)
                except:
                    print('couldn\'t process oz')

    return {'id': product_id,
            'product_url': url,
            'sku': sku,
            'category': category,
            'brand': brand,
            'name': name,
            'rating': rating,
            'detail_text': details_text,
            'size_oz': sku_oz,
            'price': price}


def get_sephora_sitemap_xml():
    sitemap_url = 'http://www.sephora.com/products-sitemap.xml'
    target_path = 'data/sitemap.xml'
    response = requests.get(sitemap_url, stream=True)
    handle = open(target_path, "wb")
    for chunk in response.iter_content(chunk_size=512):
        if chunk:  # filter out keep-alive new chunks
            handle.write(chunk)


def store_sephora_products():
    xml_file = 'data/sitemap.xml'
    products_list = xml.etree.ElementTree.parse(xml_file).getroot()
    sephora_urls_list = [product[0].text for product in products_list]

    conn = make_db_conn()
    sephora_product_table = get_sephora_product_table()

    for i, product_url in enumerate(sephora_urls_list):
        print('{}: {}'.format(i, product_url))
        product = scrape_product(product_url)

        if product:
            try:
                conn.execute(sephora_product_table.insert(), [product])
            except IntegrityError as e:
                print ("caught: ", e)
        else:
            print('No product id found for ', product_url)
        sleep(1)


def test():
    """
    product_url4 = 'http://www.sephora.com/skincare-travel-duo-P416922'
    print(scrape_product(product_url4))
    """
    # product_id = 'P413931'
    product_id = 'P418218'  # skin type
    return scrape_product_reviews(product_id)


if __name__ == '__main__':
    """
    Scraping code last tested 08 / 2017
    """
    store_sephora_products()
