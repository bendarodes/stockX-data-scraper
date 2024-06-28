import logging
import logging.handlers
import os

import requests

# Initialize logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Define a rotating file handler for logging
logger_file_handler = logging.handlers.RotatingFileHandler(
    "status.log",  # Log file name
    maxBytes=1024 * 1024,  # Maximum size per log file (1MB)
    backupCount=1,  # Number of backup log files to keep
    encoding="utf8"  # Encoding for the log file
)

# Set the log format
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(logger_file_handler)

# Function to get environment variable or provide default value
def get_env_variable(name, default="Token not available!"):
    try:
        return os.environ[name]
    except KeyError:
        logger.warning(f"Environment variable '{name}' not set, using default value.")
        return default

if __name__ == "__main__":
    # Get the secret token or use a default value
    #SOME_SECRET = get_env_variable("SOME_SECRET")

    # Log the token value (will be 'Token not available!' if not set)
    #logger.info(f"Token value: {SOME_SECRET}")

    # Make a request to World Time API to get current time in Istanbul
    try:
        r = requests.get('https://worldtimeapi.org/api/timezone/Europe/Istanbul')

        # Check if request was successful
        if r.status_code == 200:
            data = r.json()
            utc_dtime = data["utc_datetime"]
            logger.info(f'Time in Europe/Istanbul: {utc_dtime}')
        else:
            logger.error(f"Failed to fetch time from World Time API. Status code: {r.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to connect to World Time API: {e}")
