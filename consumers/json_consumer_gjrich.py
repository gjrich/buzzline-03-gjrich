"""
json_consumer_gjrich.py

Consume json messages from a Kafka topic and process them.

JSON is a set of key:value pairs. 

Example serialized Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

Example JSON message (after deserialization) to be analyzed
{"message": "I love Python!", "author": "Eve"}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting author occurrences

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Import for Sentiment Analysis
from textblob import TextBlob


#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("NEWS_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> int:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("NEWS_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up Data Store to hold author counts
#####################################

# Initialize a dictionary to store author counts
# The defaultdict type initializes counts to 0
# pass in the int function as the default_factory
# to ensure counts are integers
# {author: count} author is the key and count is the value
author_counts = defaultdict(int)



#####################################
# Initialize Sentiment Analysis
######################################

# Initialize sentiment status
sentiment_status = 0.00
thresholds = [-2, -1, 1, 2]

last_alert_range = None

message_counter = 0

alert_messages = {
    # very negative sentament
    -2: "Hear ye, hear ye! The village is mourning the loss of several souls to a terrible illness; may God grant us strength in this trying time.",
    
    # slightly negative sentiment
    -1: "Hear ye, hear ye! The Black Knight has been spotted near our borders; be prepared to defend your homes and loved ones.",
    
    # slightly positive sentiment
    1: "Hear ye, hear ye! A bountiful harvest has been bestowed upon our town; let's gather in the town square for celebration and feasting.",
    
    # very positive sentiment
    2: "Hear ye, hear ye! Lord Xavier is hosting a grand feast at his castle; all are invited to share in the merriment and revelry."
}

author_weights = {
    "Isolde the Daft": 0.4,
    "Eleanor the Fair": 0.5,
    "Cedric Sunblade": 0.6,
    "Tristan Cuthbert": 0.7,
    "Alaric the Bold": 0.8
}


#####################################
# Function to process a single message
# #####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict: dict = json.loads(message)

        # Ensure the processed JSON is logged for debugging
        logger.info(f"Processed JSON message: {message_dict}")

        # Ensure it's a dictionary before accessing fields
        if isinstance(message_dict, dict):
            # Extract the 'author' field from the Python dictionary
            author = message_dict.get("author", "unknown")
            logger.info(f"Message received from author: {author}")

            # Increment the count for the author
            author_counts[author] += 1

            # Perform sentiment analysis
            news_headline = message_dict['message']
            blob = TextBlob(news_headline)
            sentiment_score = blob.sentiment.polarity
  
            # Apply author weight to the sentiment score
            if author in author_weights:
                weighted_sentiment = sentiment_score * author_weights[author]
            else:
                weighted_sentiment = sentiment_score
  
            # Update global sentiment status
            global sentiment_status, last_alert_range, message_counter
            sentiment_status += weighted_sentiment
            sentiment_status = round(sentiment_status, 2)  # Ensure it's rounded

            message_counter += 1

            # Log author counts every 10 messages
            if message_counter % 10 == 0:
                total_author_count = sum(author_counts.values())
                logger.info(f"Updated author counts: {dict(author_counts)}")
                logger.info(f"Total count across all authors: {total_author_count}")

            # Log the current sentiment status
            logger.info(f"Current Sentiment Status: {sentiment_status}")
  
            # Determine the current range of sentiment_status
            current_range = None
            for threshold in thresholds:
                if -1 >= sentiment_status > -2:
                    current_range = -1
                    break
                if -2 >= sentiment_status:
                    current_range = -2
                    break
                if 1 <= sentiment_status <= 2:
                    current_range = 1
                    break                             
                elif 2 < sentiment_status:
                    current_range = 2
                    break
  
            # Trigger alert only if we are entering a new range
            if current_range is not None and last_alert_range != current_range:
                logger.warning(alert_messages[current_range])
                last_alert_range = current_range

        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Performs analytics on messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
