"""
@File    : UtilsRedis.py
@Author  : Alia Tasnim binti Baco
@Date    : 18/12/2024
"""

import redis

#class Redis_Update_Count:
class Redis_Utilities:
    valid_tatabahasa = [
        "kata nama", "kata kerja", "kata adjektif", "kata sendi", "kata keterangan", "tidak diketahui",
        "kata singkatan", "kata nama khas", "kata sendi nama", "kata ganti nama diri", "kata nama jamak",
        "kata ganti nama", "kata kerja pasif", "kata seru", "kata sifat", "kata ganti nama diri tinggal",
        "singkatan", "kata ganti", "kata nama waktu", "kata tanya", "kata hubung", "kata tunjuk", "kata keterangan masa"
    ]

    def __init__(self, host="localhost", port=6379, db=0):
        """
        Initialize the Redis connection.

        Args:
            host (str): Redis server host.
            port (int): Redis server port.
            db (int): Redis database number.
        """
        self.redis_client = redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)

    def update_tatabahasa_count(self, tatabahasa):
        """
        Increment the count of a specific tatabahasa type in Redis after validation.

        Args:
            tatabahasa (str): The grammatical category to increment.
        """
        if tatabahasa not in self.valid_tatabahasa:
            tatabahasa = "tidak diketahui"  # Fallback for invalid tatabahasa types

        #redis best practice naming btw
        redis_key = "tatabahasa:counts"
        self.redis_client.hincrby(redis_key, tatabahasa, 1)

    def update_sentiment_count(self, sentiment):
        """
        Increment the count of sentiment distribution (positive, neutral, negative) in Redis.

        Args:
            sentiment (float): Sentiment score to categorize and increment.
        """
        redis_key = "sentiment:counts"
        if sentiment > 0:
            self.redis_client.hincrby(redis_key, "positive", 1)
        elif sentiment < 0:
            self.redis_client.hincrby(redis_key, "negative", 1)
        else:
            self.redis_client.hincrby(redis_key, "neutral", 1)

    def store_sentiment(self, word, sentiment):
        """
        Store or update the sentiment value for a specific word in Redis.

        Args:
            word (str): The word key to store the sentiment.
            sentiment (float): The sentiment score.
        """
        redis_key = f"sentiment:{word}"
        self.redis_client.hset(redis_key, mapping={"sentiment": sentiment})
        
    def update_word_frequencies(self, word_frequencies_df):
        """
        Increment the frequency of words in Redis.

        Args:
            word_frequencies_df (DataFrame): DataFrame containing word frequencies.
        """
        redis_key = "word:frequencies"  # Redis hash key to store word frequencies
        try:
            for row in word_frequencies_df.collect():
                word = row["Cleaned_Word"]
                frequency = row["Frequency"]
                # Increment the word count in Redis hash using hincrby
                self.redis_client.hincrby(redis_key, word, frequency)
        except Exception as e:
                print(f"Error saving to Redis: {e}")

    # get functions to retrieve

    def get_tatabahasa_count(self):
        """
        Retrieve all tatabahasa counts from Redis.

        Returns:
            dict: A dictionary containing tatabahasa counts.
        """
        redis_key = "tatabahasa:counts"
        return self.redis_client.hgetall(redis_key)

    def get_sentiment_count(self):
        """
        Retrieve sentiment distribution counts from Redis.

        Returns:
            dict: A dictionary containing counts for positive, neutral, and negative sentiments.
        """
        redis_key = "sentiment:counts"
        return self.redis_client.hgetall(redis_key)

    def get_sentiment(self, word):
        """
        Retrieve the sentiment value for a specific word from Redis.

        Args:
            word (str): The word key to retrieve the sentiment.

        Returns:
            dict: Sentiment data for the word, or None if not found.
        """
        redis_key = f"sentiment:{word}"
        return self.redis_client.hgetall(redis_key)
        
    def get_word_frequencies(self):
        """
        Retrieve all word frequencies from Redis.

        Returns:
            dict: A dictionary containing word frequencies.
        """
        redis_key = "word:frequencies"
        return self.redis_client.hgetall(redis_key)

    def get_word_frequency(self, word):
        """
        Retrieve the frequency of an individual word from Redis.

        Args:
            word (str): The word to retrieve the frequency for.

        Returns:
            int: Frequency of the word, or 0 if the word does not exist.
        """
        redis_key = "word:frequencies"
        frequency = self.redis_client.hget(redis_key, word)
        return int(frequency) if frequency else 0
    
    def close(self):
        """
        Close the Redis connection.
        """
        self.redis_client.close()



