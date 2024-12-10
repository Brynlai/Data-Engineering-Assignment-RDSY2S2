from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import requests
from bs4 import BeautifulSoup
from typing import List


# Function to scrape all comments
def scrape_comments(soup: BeautifulSoup, aid: int) -> List[tuple]:
    comments = []

    comments_container = soup.find('div', id='comment_ul')
    if not comments_container:
        return comments

    comment_tags = comments_container.find_all(['dl', 'dI'], id=True)

    for comment_tag in comment_tags:
        comment_id_str = comment_tag.get('id', '').replace('comment_', '').split('_')[0]
        comment_id = int(comment_id_str) if comment_id_str.isdigit() else 0

        user_tag = comment_tag.find('a', class_='xi2')
        user = user_tag.text.strip() if user_tag else "Anonymous"

        comment_text_tag = comment_tag.find('dd')
        if comment_text_tag:
            quote_tags = comment_text_tag.find_all('div', class_='quote')
            for quote_tag in quote_tags:
                quote_tag.extract()
            comment_text = comment_text_tag.get_text(strip=True)
        else:
            comment_text = "No comment text"

        comments.append((aid, comment_id, user, comment_text))

    return comments


# Function to scrape an article from URL and AID
def scrape_article(url: str, aid: int) -> tuple:
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        title_tag = soup.find('title')
        title = title_tag.text.strip() if title_tag else "Unknown"

        date_tag = soup.find('p', class_='xg1')
        date = date_tag.text.split('|')[0].strip() if date_tag else "Unknown"

        publisher_tag = date_tag.find('a') if date_tag else None
        publisher = publisher_tag.text.strip() if publisher_tag else "Unknown"

        views_tag = soup.find('em', id='_viewnum')
        views_str = views_tag.text.replace(',', '') if views_tag else "0"
        views = int(views_str) if views_str.isdigit() else 0

        comments_tag = soup.find('em', id='_commentnum')
        comments_count_str = comments_tag.text.replace(',', '') if comments_tag else "0"
        comments_count = int(comments_count_str) if comments_count_str.isdigit() else 0

        content_tag = soup.find('td', id='article_content')
        content = content_tag.get_text(strip=True) if content_tag else ""

        comments = scrape_comments(soup, aid)

        return (aid, title, date, publisher, views, comments_count, content, comments)
    except Exception as e:
        print(f"Error scraping AID {aid}: {e}")
        return None


# PySpark setup
spark = SparkSession.builder \
    .appName("ScrapedDataProcessor") \
    .getOrCreate()

# Scraping and data processing
base_url = "https://b.cari.com.my/portal.php?mod=view&aid="
aid_values = range(1, 501)  # Adjust range as needed

article_data = []
comments_data = []

for aid in aid_values:
    url = f"{base_url}{aid}"
    print(f"Scraping AID: {aid}")
    scraped_data = scrape_article(url, aid)
    if scraped_data:
        article = scraped_data[:-1]  # Exclude comments
        comments = scraped_data[-1]  # Extract comments
        article_data.append(article)
        comments_data.extend(comments)

# Defining schemas
article_schema = StructType([
    StructField("AID", IntegerType(), True),
    StructField("Title", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Publisher", StringType(), True),
    StructField("Views", IntegerType(), True),
    StructField("Comments_Count", IntegerType(), True),
    StructField("Content", StringType(), True),
])

comments_schema = StructType([
    StructField("AID", IntegerType(), True),
    StructField("Comment_ID", IntegerType(), True),
    StructField("User", StringType(), True),
    StructField("Comment_Text", StringType(), True),
])

# Creating DataFrames
article_df = spark.createDataFrame(article_data, schema=article_schema)
comments_df = spark.createDataFrame(comments_data, schema=comments_schema)

# Displaying data
print("Articles DataFrame:")
article_df.show(10, truncate=True)

print("Comments DataFrame:")
comments_df.show(10, truncate=True)

article_df.write.format("csv").mode("overwrite").option("header", "true").save("assignmentData/article_df.csv")
article_csv = spark.read.csv('assignmentData/article_df.csv', header=True)
comments_df.write.format("csv").mode("overwrite").option("header", "true").save("assignmentData/comments_df.csv")
comments_csv = spark.read.csv('assignmentData/comments_df.csv', header=True)

article_csv.show(5)
comments_csv.show(5)