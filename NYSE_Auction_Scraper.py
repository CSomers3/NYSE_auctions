import os
import json
import logging
import time
from typing import Dict, Any, List, Optional, Callable
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, WebDriverException, NoSuchElementException
)
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from datetime import datetime, timedelta

class AdvancedWebScraper:
    def __init__(self, browser: str = 'chrome', headless: bool = True, log_level: int = logging.INFO, timeout: int = 30, retry_attempts: int = 3):
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.output_dir = os.path.join(os.getcwd(), 'web_scraper_output')
        os.makedirs(self.output_dir, exist_ok=True)
        self._setup_webdriver(browser, headless)

    def _setup_webdriver(self, browser: str, headless: bool):
        if browser.lower() != 'chrome':
            raise ValueError("Currently only Chrome is supported")
        service = Service(ChromeDriverManager().install())
        options = Options()
        if headless:
            options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-extensions')
        options.add_argument('--start-maximized')
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        self.driver = webdriver.Chrome(service=service, options=options)
        self.wait = WebDriverWait(self.driver, self.timeout)
        self.logger.info("WebDriver initialized successfully")

    def navigate(self, url: str, wait_condition: Optional[Callable] = None, additional_wait: float = 2.0) -> bool:
        try:
            self.driver.get(url)
            if wait_condition:
                self.wait.until(wait_condition)
            time.sleep(additional_wait)
            return True
        except Exception as e:
            self.logger.error(f"Navigation error: {e}")
            return False

    def close(self):
        try:
            if self.driver:
                self.driver.quit()
            self.logger.info("WebDriver closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing WebDriver: {e}")

def get_weekdays(start_date: str, end_date: str) -> List[str]:
    start = datetime.strptime(start_date, "%m-%d-%Y")
    end = datetime.strptime(end_date, "%m-%d-%Y")
    weekdays = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # Monday = 0, Friday = 4
            weekdays.append(current.strftime("%m-%d-%Y"))
        current += timedelta(days=1)
    return weekdays

def nyse_auction_scraper_example(tickers: List[str], dates: List[str]):
    scraper = AdvancedWebScraper(headless=False, log_level=logging.INFO)
    all_data = []

    try:
        for ticker in tqdm(tickers, desc="Processing tickers"):
            for date in tqdm(dates, desc=f"Processing dates for {ticker}", leave=False):
                url = f"https://www.nyse.com/nyse-auction-data?symbol={ticker}&date={date}"
                wait_condition = lambda driver: driver.execute_script(
                    "return typeof Highcharts !== 'undefined' && Highcharts.charts.length > 0"
                )
                if scraper.navigate(url, wait_condition=wait_condition, additional_wait=3.0):
                    raw_data_script = """
                    function extractSeriesData(chart) {
                        if (!chart || !chart.series || !chart.series[0]) return null;
                        return {
                            times: chart.xAxis[0].categories,
                            values: chart.series[0].points.map(function(point) {
                                return {
                                    y: point.y,
                                    label: point.dataLabel ? point.dataLabel.textStr : null
                                };
                            })
                        };
                    }
                    var rawData = { opening: {}, closing: {} };
                    var headers = Array.from(document.querySelectorAll('h6.mb-4.pl-8.text-black'));
                    var chartIndex = 0;
                    headers.forEach(function(header) {
                        var isOpening = header.textContent.includes('Opening');
                        var type = isOpening ? 'opening' : 'closing';
                        rawData[type].imbalance = extractSeriesData(Highcharts.charts[chartIndex]);
                        rawData[type].paired = extractSeriesData(Highcharts.charts[chartIndex + 1]);
                        rawData[type].price = extractSeriesData(Highcharts.charts[chartIndex + 2]);
                        chartIndex += 3;
                    });
                    return JSON.stringify(rawData);
                    """
                    raw_data = json.loads(scraper.driver.execute_script(raw_data_script))

                    for auction_type in ['opening', 'closing']:
                        if raw_data[auction_type]:
                            for metric in ['imbalance', 'paired', 'price']:
                                if raw_data[auction_type].get(metric):
                                    times = raw_data[auction_type][metric]['times']
                                    values = raw_data[auction_type][metric]['values']
                                    metric_name = 'paired_quantity' if metric == 'paired' else 'clearing_price' if metric == 'price' else metric
                                    for t, v in zip(times, values):
                                        all_data.append({
                                            'symbol': ticker,
                                            'date': date,
                                            'auction_type': auction_type,
                                            'metric': metric_name,
                                            'time': t,
                                            'value': v['y'],
                                            'label': v['label']
                                        })
                else:
                    scraper.logger.warning(f"Failed to navigate to URL for {ticker} on {date}")

        # Create a DataFrame from all collected data
        df = pd.DataFrame(all_data)

        # Save the consolidated data to a single CSV file
        output_file = os.path.join(scraper.output_dir, 'nyse_auction_data_consolidated.csv')
        df.to_csv(output_file, index=False)
        scraper.logger.info(f"Saved consolidated data to {output_file}")

    except Exception as e:
        scraper.logger.error(f"Scraping failed: {e}")
    finally:
        scraper.close()

def main():
    tickers = ["A", "MMM", "WFC"]
    start_date = "12-01-2024"
    end_date = "12-05-2024"
    dates = get_weekdays(start_date, end_date)
    nyse_auction_scraper_example(tickers, dates)

if __name__ == "__main__":
    main()