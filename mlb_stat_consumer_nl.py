"""
    This program listens for messages continuously on RabbitMQ queues.
    It processes player statistics from the 'national-league' queue.

    Author: Graham Miller
    Date: June 9, 2024
"""

# Imports
import pika
import sys

# Configure Logging
from util_logger import setup_logger
logger, logname = setup_logger(__file__)

# Constants for Alert Criteria
BAT_AVG_THRESHOLD = 0.280
HR_THRESHOLD = 10
RBI_THRESHOLD = 30

# Callback for National League queue
def national_league_callback(ch, method, properties, body):
    process_message(ch, method, body, "National League")


def process_message(ch, method, body, league):
    try:
        try:
            message = body.decode('utf-8')
        except UnicodeDecodeError as e:
            logger.error(f"Error decoding message with UTF-8: {e}")
            logger.info("Trying with 'latin-1' encoding...")
            message = body.decode('latin-1')  # Fallback to a different encoding

        logger.info(f"Received from {league} queue: {message}")
        
        # Split message and parse values
        player_data = parse_message(message)
        
        if player_data:
            player_name = player_data['Player']
            avg = player_data['avg']
            hr = player_data['hr']
            rbi = player_data['rbi']

            # Check if all alert conditions are met
            if avg > BAT_AVG_THRESHOLD and hr > HR_THRESHOLD and rbi > RBI_THRESHOLD:
                logger.info(f"Alert: {player_name} in the {league} meets all criteria - AVG: {avg}, HR: {hr}, RBI: {rbi}")

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def parse_message(message):
    try:
        # Expected format: Player: Name, avg: X.XXX, hr: XX, rbi: XXX
        data = message.split(',')
        player_name = data[0].split(':')[1].strip()
        avg = float(data[1].split(':')[1].strip())
        hr = int(data[2].split(':')[1].strip())
        rbi = int(data[3].split(':')[1].strip())
        
        return {
            "Player": player_name,
            "avg": avg,
            "hr": hr,
            "rbi": rbi
        }
    except Exception as e:
        logger.error(f"Error parsing message: {e}")
        return None

def main(hn: str = "localhost"):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))
        channel = connection.channel()

        # Declare queues
        queues = ["national-league"]
        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)

        # Set up consumers
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue="national-league", on_message_callback=national_league_callback, auto_ack=False)

        logger.info(" [*] Ready for work. To exit press CTRL+C")
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"ERROR: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"ERROR: Something went wrong: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logger.info("Closing connection. Goodbye.")
        connection.close()

if __name__ == "__main__":
    main()