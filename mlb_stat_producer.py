"""
    This program sends a message to a queue on the RabbitMQ server.

    Author: Graham Miller
    Date: June 9, 2024

"""
# Imports
import pika
import sys
import webbrowser
import csv
import time

# Configure Logging
from util_logger import setup_logger
logger, logname = setup_logger(__file__)

# Offer to open RabbitMQ Admin Page
def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        logger.info("Opened RabbitMQ")

# Define league mapping
AMERICAN_LEAGUE_TEAMS = {
    "BAL", "BOS", "NYY", "TB", "TOR", "CWS", "CLE", "DET", "KC", 
    "MIN", "HOU", "LAA", "OAK", "SEA", "TEX"
}

NATIONAL_LEAGUE_TEAMS = {
    "ATL", "MIA", "NYM", "PHI", "WSH", "CHC", "CIN", "MIL", "PIT", 
    "STL", "ARI", "COL", "LAD", "SD", "SF"
}

# Connect to RabbitMQ server
def connect_rabbitmq():
    try:
        # Create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch = conn.channel()

        queues = ["national-league", "american-league"]
        for queue_name in queues:
            # Declare the queue
            ch.queue_declare(queue=queue_name, durable=True)

        return conn, ch 
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)

# Process CSV and send message to RabbitMQ queues
def csv_processing():
    try:
        csv_path = "mlb-player-stats-Batters.csv"
        with open(csv_path, newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            for data_row in reader:
                player_name = data_row['Player']
                team_name = data_row['Team']
                avg_str = data_row['AVG']
                hr_str = data_row['HR']
                rbi_str = data_row['RBI']

                # Determine league based on team
                if team_name in AMERICAN_LEAGUE_TEAMS:
                    league_queue = "american-league"
                elif team_name in NATIONAL_LEAGUE_TEAMS:
                    league_queue = "national-league"
                else:
                    logger.warning(f"Team {team_name} not found in league lists.")
                    continue

                # Checks if strings are empty
                if avg_str and hr_str and rbi_str:
                    avg = float(avg_str)
                    hr = int(hr_str)
                    rbi = int(rbi_str)
                    message = f"Player: {player_name}, avg: {avg}, hr: {hr}, rbi: {rbi}"
                    send_message(league_queue, message)
    except FileNotFoundError:
        logger.error("File not found.")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Error processing file: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        sys.exit(1)

def send_message(queue_name: str, message: str):
    """ 
    Send message to queue
    
    Parameters:
        message: The message sent to the queue
        queue_name: name of the queue
    """
    try:
        conn, ch = connect_rabbitmq()
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        logger.info(f"Sent message to {queue_name}: {message}")
    except Exception as e:
        logger.error(f"Error sending message to {queue_name}: {e}")  
    finally:
        # Close connection to the server
        conn.close()  

if __name__ == "__main__":
    offer_rabbitmq_admin_site()
    csv_processing()