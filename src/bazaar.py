import datetime as dt
from typing import Any
from pathlib import Path

import requests
import polars as pl

class BazaarTracker:
    def __init__(self, base_url: str, bazaar_history = {}) -> None:
        self.base_url = base_url
        self.order_history: dict[dt.datetime, dict[str, tuple[pl.DataFrame, pl.DataFrame]]] = {}
        self.status_history: dict[dt.datetime, pl.DataFrame] = {}
        self.latest_timestamp: dt.datetime = None
    
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
    
    def _format_data(self, timestamp: dt.datetime, data: dist[str, Any]) -> tuple[dt.datetime, dict[str, tuple[pl.DataFrame, pl.DataFrame]]]:
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

    def update(self) -> None:
        data = self._fetch_data()
        
        if data:
            unix_timestamp = int(data.get("lastUpdated"))
            timestamp = dt.datetime.fromtimestamp(unix_timestamp / 1000)

            if not timestamp == self.latest_timestamp:
                orders, status = self._format_data(timestamp, data)
                
                self.order_history[timestamp] = orders
                self.status_history[timestamp] = status
                self.latest_timestamp = timestamp
    
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