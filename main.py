import logging
import os
from datetime import datetime
import pandas as pd
import yfinance as yf

def setup_logger(log_file="status.log"):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Remove all existing handlers to prevent duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    file_handler = logging.FileHandler(log_file, mode='a')
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

def get_env_variable(name, default="Token not available!"):
    return os.environ.get(name, default)

def read_companies_from_csv(csv_file):
    try:
        df = pd.read_csv(csv_file)
        if 'name' not in df.columns:
            logger.error("CSV file does not contain a 'name' column")
            return []
        return df['name'].tolist()
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        return []

def fetch_and_save_data(symbols, csv_file):
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
        
        logger.info(f"Data saved for {current_time}")
        return True
    except Exception as e:
        logger.error(f"Error fetching or saving data: {str(e)}")
        return False

if __name__ == "__main__":
    logger = setup_logger()
    
    companies_file = 'companies.csv'
    companies = read_companies_from_csv(companies_file)
    
    if not companies:
        logger.error("No companies found in CSV file. Exiting.")
        exit(1)
    
    csv_file = 'stock_prices.csv'
    
    if fetch_and_save_data(companies, csv_file):
        logger.info(f'Data write successful: {datetime.now()}')
    else:
        logger.error('Failed to fetch and save data')
