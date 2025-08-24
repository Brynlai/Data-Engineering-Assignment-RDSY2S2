"""
Author  : Alia Tasnim binti Baco
Date    : 22/12/2024
"""

from pyspark.sql import Row
from pyspark.sql.functions import col, sum, desc, count, avg, format_number, concat, lit, max
from UtilsRedis import Redis_Utilities
from neo4j import GraphDatabase
from GlobalSparkSession import GlobalSparkSession
import matplotlib.pyplot as plt
import seaborn as sns
import redis

#Word frequency analysis
# Retrieve all frequencies from Redis
redis_client = Redis_Utilities()  # Assuming UtilsRedis is your class handling Redis interactions
word_frequencies_dict = redis_client.get_word_frequencies()

# Convert the dictionary to a PySpark DataFrame
spark = GlobalSparkSession.get_instance()
word_frequencies_list = [{"Cleaned_Word": word, "Frequency": int(freq)} for word, freq in word_frequencies_dict.items()]
word_frequencies_df = spark.createDataFrame(Row(**x) for x in word_frequencies_list)

# Show the first 7 rows
print("Preview of all word frequencies (first 7 rows):")
word_frequencies_df.show(7)

# Retrieve any word to confirm frequency

#########################
• Segment frequency by tatabahasa like kata kerja, kata nama, etc to understand distribution across tatabahasa
# Define the standalone function
def get_tatabahasa_batch(neo4j_uri, neo4j_user, neo4j_password, words):
    """
    Retrieve tatabahasa for a list of words in a single batch query.
    """
    # Prepare the list of words for the Cypher query
    words_str = ", ".join([f'"{word}"' for word in words])

    query = f"""
    MATCH (w:Word)
    WHERE w.word IN [{words_str}]
    RETURN w.word AS Cleaned_Word, collect(DISTINCT w.tatabahasa) AS Tatabahasa
    """

    # Connect to Neo4j and execute the query
    with GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password)) as driver:
        with driver.session() as session:
            result = session.run(query)
            return {record["Cleaned_Word"]: record["Tatabahasa"] for record in result}

# Setup Neo4j credentials
neo4j_uri = "neo4j+s://553a48f9.databases.neo4j.io"
neo4j_user = "neo4j"
neo4j_password = "WFSHORiVbtQSwjqT0V3n9Bph3F5FKsSdV_o4fOsX3NU"  # Replace with your actual password

# Retrieve the list of words from your DataFrame
words = word_frequencies_df.select("Cleaned_Word").distinct().rdd.flatMap(lambda x: x).collect()

# Fetch tatabahasa information for all words using the standalone function
word_tatabahasa_dict = get_tatabahasa_batch(neo4j_uri, neo4j_user, neo4j_password, words)

# Create a DataFrame from the fetched tatabahasa data
word_tatabahasa_list = [{"Cleaned_Word": word, "Tatabahasa": tatabahasa} 
                        for word, tatabahasa in word_tatabahasa_dict.items()]

word_tatabahasa_df = spark.createDataFrame(Row(**x) for x in word_tatabahasa_list)

# Join word frequencies with tatabahasa
word_with_tatabahasa_df = word_frequencies_df.join(word_tatabahasa_df, on="Cleaned_Word", how="inner")

# Group by tatabahasa and calculate total, average, and word count
tatabahasa_analysis_df = word_with_tatabahasa_df.groupBy("Tatabahasa").agg(
    sum("Frequency").alias("Total_Frequency"),
    format_number(avg("Frequency"),2).alias("Average_Frequency"),
    count("Cleaned_Word").alias("Word_Count")
)

# Sort by total frequency (descending)
sorted_tatabahasa_analysis_df = tatabahasa_analysis_df.orderBy(col("Total_Frequency").desc()) #.desc() if want descending order

# Show the results
#sorted_tatabahasa_analysis_df.show(truncate=False)
sorted_tatabahasa_analysis_df.show(n=sorted_tatabahasa_analysis_df.count(), truncate=False)
##########################
# Total_Frequency : cumulative frequency of all words within the tatabahasa category
# Word_Count : number of unique words within the tatabahasa category
#since all tatabahasa category have a high total frequency but low word count, that means a specific words is being used repeatedly.
# Group by tatabahasa and find the word with the highest frequency
highest_frequency_df = word_with_tatabahasa_df.groupBy("Tatabahasa").agg(
    max("Frequency").alias("Highest_Frequency")
)

# Join back to original DataFrame to identify the words with the highest frequency
highest_frequency_words_df = highest_frequency_df.alias("hf").join(
    word_with_tatabahasa_df.alias("wt"),
    (col("hf.Tatabahasa") == col("wt.Tatabahasa")) &
    (col("hf.Highest_Frequency") == col("wt.Frequency")),
    how="inner"
).select(
    col("hf.Tatabahasa").alias("Tatabahasa"),
    col("wt.Cleaned_Word").alias("Cleaned_Word"),
    col("hf.Highest_Frequency").alias("Highest_Frequency")
)

# Show the results
highest_frequency_words_df.show(truncate=False)
#######################3
#tatabahasa POS distribution 

# Retrieve tatabahasa counts from Redis
redis_client = Redis_Utilities()  # Assuming UtilsRedis is your class handling Redis interactions
tatabahasa_counts_dict = redis_client.get_tatabahasa_count()

# Convert the dictionary to a PySpark DataFrame
tatabahasa_counts_list = [{"Tatabahasa_Type": tatabahasa, "Count": int(count)} for tatabahasa, count in tatabahasa_counts_dict.items()]
tatabahasa_counts_df = spark.createDataFrame(Row(**x) for x in tatabahasa_counts_list)

# Exclude specific tatabahasa types
excluded_tatabahasa = ["kata nombor", "nombor", "tidak diketahui"]
filtered_tatabahasa_df = tatabahasa_counts_df.filter(~col("Tatabahasa_Type").isin(*excluded_tatabahasa))

# Calculate the total count after filtering
total_count = filtered_tatabahasa_df.agg(sum("Count").alias("Total")).collect()[0]["Total"]

# Add a column for percentage contribution with 2 decimal places and a `%` symbol
tatabahasa_percentage_df = filtered_tatabahasa_df.withColumn(
    "Percentage", concat(format_number((col("Count") / total_count) * 100, 2), lit("%"))
)

# Sort by count in descending order
sorted_tatabahasa_df = tatabahasa_percentage_df.orderBy(col("Count").desc())

# Show tatabahasa types with counts and percentages
print("Filtered Tatabahasa distribution with percentages (sorted by count):")
sorted_tatabahasa_df.show()
####################
# Sentiment Distribution
# Initialize Redis Utilities
redis_util = Redis_Utilities(host="localhost", port=6379, db=0)

# Fetch sentiment distribution from Redis (positive, neutral, negative)
sentiment_data = redis_util.get_sentiment_count()

# Retrieve the counts for each sentiment category (positive, neutral, negative)
positive_count = int(sentiment_data.get("positive", 0))
neutral_count = int(sentiment_data.get("neutral", 0))
negative_count = int(sentiment_data.get("negative", 0))

# Total word count across all sentiment categories
total_words = positive_count + neutral_count + negative_count

# Calculate proportions for each sentiment category
positive_proportion = round(positive_count / total_words * 100, 2) if total_words else 0
neutral_proportion = round(neutral_count / total_words * 100, 2) if total_words else 0
negative_proportion = round(negative_count / total_words * 100, 2) if total_words else 0

# Prepare the data for the table
sentiment_proportions_data = [
    {"Sentiment": "Positive", "Word_Count": positive_count, "Proportion": f"{positive_proportion}%"},
    {"Sentiment": "Neutral", "Word_Count": neutral_count, "Proportion": f"{neutral_proportion}%"},
    {"Sentiment": "Negative", "Word_Count": negative_count, "Proportion": f"{negative_proportion}%"},
]

# Convert the data to a Spark DataFrame
sentiment_proportions_df = spark.createDataFrame(Row(**x) for x in sentiment_proportions_data)

# Show the results
sentiment_proportions_df.show()



