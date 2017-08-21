import cnfg
from sqlalchemy import create_engine, text
from sklearn.feature_extraction.text import TfidfVectorizer
import csv
import re
import pandas as pd


def make_db_conn():
    config = cnfg.load(".metis_config")
    engine = create_engine(
        'postgresql://{}:{}@{}:5432/{}'.format(
            config['db_user'],
            config['db_pwd'],
            config['db_host'],
            'sephora'))
    conn = engine.connect()
    return conn


def tokenize(text):
    clean_text = re.sub(r'[,!.$\d%&~?()"]', ' ', text)
    clean_text = re.sub(r'[-]', '', clean_text)
    clean_text = ' '.join(clean_text.split())
    tokenizer_regex = re.compile(r"[\s]")
    return [tok.strip().lower() for tok in tokenizer_regex.split(clean_text)]


def main():
    conn = make_db_conn()
    query = 'SELECT * FROM sephora_product_review'
    df = pd.read_sql_query(query, conn)
    review_docs = df['review_text'].as_matrix()

    vectorizer = TfidfVectorizer(min_df=2, tokenizer=tokenize)
    vectorizer.fit_transform(review_docs)
    idf = vectorizer.idf_
    word_to_idf = list(zip(vectorizer.get_feature_names(), idf))
    word_to_idf.sort(key=lambda tup: tup[1], reverse=False)
    print('Total of {} words in Sephora reviews'.format(len(word_to_idf)))
    insignificant_words = [word[0] for word in word_to_idf[:200]]

    # Add brand names to stop words
    brand_query = text('SELECT DISTINCT brand FROM sephora_product')
    brand_results = conn.execute(brand_query).fetchall()
    brand_names = [b[0] for b in brand_results]

    stop_words_filename = 'data/stopwords_brand.txt'
    with open(stop_words_filename, 'a+') as stop_words_file:
        writer = csv.writer(stop_words_file)
        for word in insignificant_words:
            writer.writerow([word])
            stop_words_file.flush()

        for brand in brand_names:
            writer.writerow([brand.lower()])
            stop_words_file.flush()
    stop_words_file.close()


if __name__ == '__main__':
    main()
