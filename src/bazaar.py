import datetime as dt
from typing import Any
from pathlib import Path
import threading
from time import sleep
from collections import deque

import requests
import polars as pl

class BazaarTracker:
    def __init__(self, base_url: str = "https://api.hypixel.net", bazaar_history = {}) -> None:
        self.base_url = base_url
        self.order_history: dict[dt.datetime, dict[str, tuple[pl.DataFrame, pl.DataFrame]]] = {}
        self.status_history: dict[dt.datetime, pl.DataFrame] = {}
        self.window_timestamps = deque()
        self.alive: bool = False
    
    def _fetch_data(self) -> dict[str, Any]:
        attempt_time = dt.datetime.now()
        url = f"{self.base_url}/v2/skyblock/bazaar"

        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                return {}
        except Exception as e:
            print(f"{attempt_time}: Failed to fetch data: {e}")
            return {}
    
    def _format_data(self, timestamp: dt.datetime, data: dist[str, Any]) -> tuple[dict[str, tuple[pl.DataFrame, pl.DataFrame]], pl.DataFrame]:
        products = data.get("products")

        fmt_orders = {}
        fmt_statuses = []
        
        for product_id, info in products.items():
            try:
                sell_orders = pl.from_dicts(info.get("sell_summary"))
                buy_orders = pl.from_dicts(info.get("buy_summary"))
                status = info.get("quick_status")

                fmt_orders[product_id] = (sell_orders, buy_orders)
                fmt_statuses.append(status)
            except Exception as e:
                # print(f"Failed to format {product_id}: {e}")
                pass
        
        statuses_df = pl.from_dicts(fmt_statuses)

        return fmt_orders, statuses_df

    def update(self, window: int = float('inf')) -> None:
        data = self._fetch_data()
        
        if data:
            unix_timestamp = int(data.get("lastUpdated"))
            timestamp = dt.datetime.fromtimestamp(unix_timestamp / 1000)

            if not self.window_timestamps or not timestamp == self.window_timestamps[-1]:
                orders, status = self._format_data(timestamp, data)
                
                self.order_history[timestamp] = orders
                self.status_history[timestamp] = status
                self.window_timestamps.append(timestamp)

                if len(self.window_timestamps) > window:
                    remove_ts = self.window_timestamps.popleft()

                    self.order_history.pop(remove_ts)
                    self.status_history.pop(remove_ts)
    
    def _live_worker(self, gap: int, window: int) -> None:
        while self.alive:
            self.update(window=window)

            sleep_time = gap - (dt.datetime.now() - self.window_timestamps[-1]).total_seconds()

            sleep(max(0.2, sleep_time))
    
    def live(self, gap: int = 10, window: int = float('inf')) -> None:
        self.alive = True
        thread = threading.Thread(target=self._live_worker, args=(gap,window))
        thread.start()
    
    def stop(self) -> None:
        self.alive = False
    
    def save(self, filepath: str) -> None:
        save_path = Path(filepath)

        for timestamp in self.status_history.keys():
            timestamp_path = save_path / str(timestamp)
            timestamp_path.mkdir(parents=True, exist_ok=True)
            self.status_history[timestamp].write_parquet(timestamp_path / "status.parquet")

            for product_id, (sell_orders, buy_orders) in self.order_history[timestamp].items():
                order_books_path = save_path / str(timestamp) / 'order_books' / f"{product_id.lower()}"
                order_books_path.mkdir(parents=True, exist_ok=True)
                sell_orders.write_parquet(order_books_path / "sell.parquet")
                buy_orders.write_parquet(order_books_path / "buy.parquet")

    def get_latest_item_orders(self, product_id: str) -> pl.DataFrame:
        return self.order_history[self.window_timestamps[-1]][product_id.upper()]
    
    def get_latest_status(self) -> pl.DataFrame:
        return self.status_history[self.window_timestamps[-1]]