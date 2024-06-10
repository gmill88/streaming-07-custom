"""
    This program listens for messages continuously on RabbitMQ queues.
    It processes player statistics from the 'national-league' and 'american-league' queues.

    Author: Graham Miller
    Date: June 9, 2024

"""
# Imports
import pika
import sys

# Constants for Alert Criteria
BAT_AVG_THRESHOLD = 0.280
HR_THRESHOLD = 10
RBI_THRESHOLD = 30

# Callback for National League queue
def national_league_callback(ch, method, properties, body):
    process_message(ch, method, body, "National League")

# Callback for American League queue
def american_league_callback(ch, method, properties, body):
    process_message(ch, method, body, "American League")

def process_message(ch, method, body, league):
    try:
        try:
            message = body.decode('utf-8')
        except UnicodeDecodeError as e:
            print(f"Error decoding message with UTF-8: {e}")
            print(f"Trying with 'latin-1' encoding...")
            message = body.decode('latin-1')  # Fallback to a different encoding

        print(f"Received from {league} queue: {message}")
        
        # Split message and parse values
        player_data = parse_message(message)
        
        if player_data:
            player_name = player_data['Player']
            avg = player_data['avg']
            hr = player_data['hr']
            rbi = player_data['rbi']

            # Check if all alert conditions are met
            if avg > BAT_AVG_THRESHOLD and hr > HR_THRESHOLD and rbi > RBI_THRESHOLD:
                print(f"Alert: {player_name} in the {league} meets all criteria - AVG: {avg}, HR: {hr}, RBI: {rbi}")

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing message: {e}")

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
        print(f"Error parsing message: {e}")
        return None

def main(hn: str = "localhost"):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))
        channel = connection.channel()

        # Declare queues
        queues = ["national-league", "american-league"]
        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)

        # Set up consumers
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue="national-league", on_message_callback=national_league_callback, auto_ack=False)
        channel.basic_consume(queue="american-league", on_message_callback=american_league_callback, auto_ack=False)

        print(" [*] Ready for work. To exit press CTRL+C")
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as e:
        print(f"ERROR: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Something went wrong: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("Closing connection. Goodbye.")
        connection.close()

if __name__ == "__main__":
    main()
