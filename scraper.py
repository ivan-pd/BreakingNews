import requests
import sqlite3
import time
from bs4 import BeautifulSoup
from config import apikey

def get_articles(apikey, section_name, page, begin_date):
    query = f'document_type:(\"article\") AND type_of_material:(\"News\") AND section_name:(\"{section_name}\")'
    begin_date = begin_date  # YYYYMMDD
    page = str(page)  # <0-100>
    sort = "relevance" # newest, oldest
    query_url = f"https://api.nytimes.com/svc/search/v2/articlesearch.json?" \
                f"fq={query}" \
                f"&api-key={apikey}" \
                f"&begin_date={begin_date}" \
                f"&page={page}" \
                f"&sort={sort}"
    time.sleep(6) 

    # Query NYT API
    r = requests.get(query_url)
    json_obj = r.json()
    response_obj = json_obj["response"]

    # Extract API Response Data
    articles = []
    article_list = response_obj['docs']
    for article in article_list:
        authors = extract_authors(article['byline']['person'])
        headline = article['headline']['main']
        pub_date = article['pub_date']
        category = article['section_name']
        web_url = article['web_url']
        article_summary = article['snippet']
        article_text = get_text(web_url)

        # print_headlines and subsection aren't necessary feilds but nice to have
        print_headline = article['headline']['print_headline']
        if 'subsection_name' in article:
            sub_category = article['subsection_name']
        else:
            sub_category = ''

        articles.append({
            'authors': authors,
            'headline': headline,
            'print_headline': print_headline,
            'pub_date': pub_date,
            'category': category,
            'article_summary': article_summary,
            'sub_category': sub_category,
            'web_url': web_url,
            'article_text': article_text,
        })

    return articles

def extract_authors(author_data):
    authors = []
    if len(author_data) > 0:
        for author in author_data:
            firstname = ''
            lastname = ''
            if 'firstname' in author and author['firstname'] != None:
                firstname = author['firstname']
            if 'lastname' in author and author['lastname'] != None:
                lastname = author['lastname']
            authors.append(firstname + ' ' + lastname)
    return authors

def get_text(web_url):
    req = requests.get(web_url)
    req.raise_for_status() # Raises an error if the request fails
    bs = BeautifulSoup(req.content, 'html.parser')
    paragraphs = bs.find_all("p", {"class":"css-axufdj evys1bk0"})

    full_text = ''
    for p in paragraphs:
        full_text = full_text + p.text

    return full_text

def create_tables(conn):
    curr = conn.cursor()
    # Delete tables if they exist
    curr.execute('DROP TABLE IF EXISTS "nyt_articles";')

    sql_create_nyt_articles_table = """ CREATE TABLE IF NOT EXISTS nyt_articles (
                                            id integer primary key autoincrement,
                                            authors text,
                                            headline text,
                                            print_headline text,
                                            pub_date text,
                                            category text,
                                            article_summary text,
                                            sub_category text,
                                            article_text text,
                                            web_url text
                                        ); """

    curr.execute(sql_create_nyt_articles_table)


def add_article_data(conn, data):
    curr = conn.cursor()
    authors = ', '.join(data['authors'])
    headline = data['headline']
    print_headline = data['print_headline']
    pub_date = data['pub_date']
    category = data['category']
    article_summary = data['article_summary']
    sub_category = data['sub_category']
    article_text = data['article_text']
    web_url = data['web_url']
        
    command = ''' INSERT INTO nyt_articles(authors, headline, print_headline, pub_date, category, article_summary, sub_category, article_text, web_url)
            VALUES(?,?,?,?,?,?,?,?,?) '''

    curr.execute(command, (authors, headline, print_headline, pub_date, category, article_summary, sub_category, article_text, web_url))
    conn.commit()

def main():
    num_of_pages =  10 # should be 200 articles
    business_articles = [] 
    tech_articles = []
    science_articles = []
    education_articles = []
    art_articles = []
    for i in range(num_of_pages):
        business_articles += get_articles(apikey, "Business Day", i, "20180101")
        tech_articles += get_articles(apikey, "Technology", i, "20180101")
        science_articles += get_articles(apikey, "Science", i, "20180101")
        education_articles += get_articles(apikey, "Education", i, "20180101")
        art_articles += get_articles(apikey, "Arts", i, "20180101")
    conn = sqlite3.connect('data.db')
    create_tables(conn)
    for article in business_articles:
        add_article_data(conn, article)
    for article in tech_articles:
        add_article_data(conn, article)
    for article in science_articles:
        add_article_data(conn, article)
    for article in education_articles:
        add_article_data(conn, article)
    for article in art_articles:
        add_article_data(conn, article)
    conn.close()

if __name__ == "__main__":
    main()