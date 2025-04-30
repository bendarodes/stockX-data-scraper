import logging
import os
from datetime import datetime
import pandas as pd
import yfinance as yf


def setup_logger(log_file="status.log"):
    """Set up a logger for the application."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Remove all existing handlers to prevent duplicates
    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler(log_file, mode="a")
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def get_env_variable(name, default="Token not available!"):
    """Retrieve an environment variable with a default fallback."""
    return os.environ.get(name, default)


def read_companies_from_csv(csv_file, logger):
    """Read a list of company names from a CSV file."""
    try:
        df = pd.read_csv(csv_file)
        if "name" not in df.columns:
            logger.error("CSV file does not contain a 'name' column.")
            return []
        return df["name"].tolist()
    except FileNotFoundError:
        logger.error(f"CSV file '{csv_file}' not found.")
        return []
    except pd.errors.EmptyDataError:
        logger.error(f"CSV file '{csv_file}' is empty.")
        return []
    except Exception as e:
        logger.error(f"Error reading CSV file '{csv_file}': {str(e)}")
        return []


def fetch_and_save_data(symbols, csv_file, logger):
    """Fetch stock data for symbols and save to a CSV file."""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        data = yf.download(symbols, period="1d", interval="1m")
        latest_prices = data["Close"].iloc[-1]

        if os.path.exists(csv_file):
            df = pd.read_csv(csv_file, sep=";")
        else:
            df = pd.DataFrame(columns=["Time"] + symbols)

        new_row = pd.DataFrame([[current_time] + latest_prices.tolist()],
                               columns=["Time"] + symbols)

        df = pd.concat([df, new_row], ignore_index=True)

        df.to_csv(csv_file, sep=";", index=False)

        logger.info(f"Data saved for {current_time}")
        return True
    except KeyError:
        logger.error("Stock data is missing required fields.")
        return False
    except Exception as e:
        logger.error(f"Error fetching or saving data: {str(e)}")
        return False


if __name__ == "__main__":
    logger = setup_logger()

    COMPANIES_FILE = "companies.csv"
    companies = read_companies_from_csv(COMPANIES_FILE, logger)

    if not companies:
        logger.error("No companies found in CSV file. Exiting.")
        import sys

        sys.exit(1)

    STOCK_PRICES_FILE = "stock_prices.csv"

    if fetch_and_save_data(companies, STOCK_PRICES_FILE, logger):
        logger.info(f"Data write successful: {datetime.now()}")
    else:
        logger.error("Failed to fetch and save data.")
