import logging
import os
from datetime import datetime
import pandas as pd
import yfinance as yf

# Constants follow UPPER_CASE naming
LOG_FILE = "status.log"
COMPANIES_FILE = "companies.csv"
CSV_FILE = "stock_prices.csv"

def setup_logger(log_file=LOG_FILE):
    """Sets up the logger with a file handler and formatter."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Remove all existing handlers to prevent duplicates
    if not logger.hasHandlers():
        file_handler = logging.FileHandler(log_file, mode='a')
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

def get_env_variable(name, default="Token not available!"):
    """Fetches an environment variable, returning a default value if not found."""
    return os.environ.get(name, default)

def read_companies_from_csv(csv_file):
    """Reads company names from a CSV file, ensuring it contains a 'name' column."""
    try:
        df = pd.read_csv(csv_file)
        if 'name' not in df.columns:
            logger.error("CSV file missing 'name' column")
            return []
        return df['name'].tolist()
    except FileNotFoundError:
        logger.error("CSV file not found: %s", csv_file)
    except pd.errors.EmptyDataError:
        logger.error("CSV file is empty: %s", csv_file)
    except pd.errors.ParserError:
        logger.error("Error parsing CSV file: %s", csv_file)
    except Exception as err:
        logger.error("Unexpected error reading CSV file: %s", err)
    return []

def fetch_and_save_data(symbols, csv_file):
    """Fetches stock data from Yahoo Finance and saves it to a CSV file."""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        data = yf.download(symbols, period="1d", interval="1m")
        latest_prices = data['Close'].iloc[-1]

        if os.path.exists(csv_file):
            df = pd.read_csv(csv_file, sep=';')
        else:
            df = pd.DataFrame(columns=['Time'] + symbols)

        new_row = pd.DataFrame([[current_time] + latest_prices.tolist()], 
                               columns=['Time'] + symbols)

        df = pd.concat([df, new_row], ignore_index=True)
        df.to_csv(csv_file, sep=';', index=False)

        logger.info("Data saved for %s", current_time)
        return True

    except FileNotFoundError:
        logger.error("File not found: %s", csv_file)
    except pd.errors.EmptyDataError:
        logger.error("Stock data is empty, possibly due to an API issue")
    except KeyError:
        logger.error("Key error while accessing stock data, check symbol validity")
    except Exception as err:
        logger.error("Error fetching or saving data: %s", err)
    return False

if __name__ == "__main__":
    logger = setup_logger()

    companies = read_companies_from_csv(COMPANIES_FILE)
    if not companies:
        logger.error("No companies found in CSV file. Exiting.")
        exit(1)

    if fetch_and_save_data(companies, CSV_FILE):
        logger.info("Data write successful: %s", datetime.now())
    else:
        logger.error("Failed to fetch and save data")