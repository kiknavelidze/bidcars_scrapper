#!/usr/bin/env python3

import os
import json
import time
import logging
from typing import Dict, Any

from dotenv import load_dotenv
import requests
from playwright.sync_api import sync_playwright

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration
BID_CARS_BASE_URL = 'https://bid.cars'
SEARCH_ENDPOINT = f'{BID_CARS_BASE_URL}/app/search/request'
SEARCH_PAGE_URL = f'{BID_CARS_BASE_URL}/en/search/results'

SEARCH_FILTERS = {
    'search-type': 'filters',
    'status': 'Fast-buy',
    'type': 'Automobile',
    'make': 'Toyota',
    'year-from': '2017',
    'auction-type': 'All',
    'odometer-to': '85000',
    'body-style': 'SUV',
    'drive-type': 'AWD',
    'fuel-type': 'Hybrid'
}

SEARCH_PAGE_SIZE = 50
STORAGE_SEEN_KEY = 'bidcars:seen-lots'
STORAGE_INIT_KEY = 'bidcars:seen-initialized'
TELEGRAM_MESSAGE_PREFIX = '🚗 ახალი Toyota (Bid.cars)'
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'

# Environment variables
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN__TOYOTA')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID__TOYOTA')
UPSTASH_REDIS_REST_URL = os.getenv('UPSTASH_REDIS_REST_URL')
UPSTASH_REDIS_REST_TOKEN = os.getenv('UPSTASH_REDIS_REST_TOKEN')


def get_redis_client():
    """Get Upstash Redis REST API client."""
    if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
        raise ValueError('UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN must be set')

    class UpstashRedis:
        def __init__(self, url: str, token: str):
            self.url = url.rstrip('/')
            self.token = token
            self.headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json',
            }

        def _request(self, command: str, *args):
            payload = [command] + list(args)
            response = requests.post(
                f'{self.url}',
                headers=self.headers,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            result = response.json()
            if result.get('error'):
                raise Exception(f'Redis error: {result["error"]}')
            return result.get('result')

        def smembers(self, key: str) -> set:
            result = self._request('SMEMBERS', key)
            return set(result) if result else set()

        def sadd(self, key: str, *values: str) -> int:
            return self._request('SADD', key, *values)

        def exists(self, key: str) -> bool:
            return bool(self._request('EXISTS', key))

        def set(self, key: str, value: str) -> str:
            return self._request('SET', key, value)

    return UpstashRedis(UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN)


def fetch_listings() -> list[dict]:
    params = {**SEARCH_FILTERS, "page": "1", "per-page": str(SEARCH_PAGE_SIZE)}
    search_url = f"{SEARCH_PAGE_URL}?{'&'.join(f'{k}={v}' for k,v in params.items())}"
    request_url = f"{SEARCH_ENDPOINT}?{'&'.join(f'{k}={v}' for k,v in params.items())}"

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            channel="chrome",
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
            ]
        )
        page = browser.new_page(user_agent=USER_AGENT, viewport={"width":1280, "height":720})
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
        page.goto(search_url, wait_until="networkidle", timeout=300000)
        page.wait_for_timeout(5000)

        response = page.evaluate(f"""
            async () => {{
                const res = await fetch("{request_url}", {{
                    method: "GET",
                    headers: {{
                        "Accept": "application/json, text/plain, */*",
                        "X-Requested-With": "XMLHttpRequest",
                        "User-Agent": "{USER_AGENT}"
                    }},
                    credentials: "include"
                }});
                const text = await res.text();
                return {{ status: res.status, body: text }};
            }}
        """)
        page.close()
        browser.close()

        if response["status"] != 200:
            raise Exception(f"Bid.cars request failed with status {response['status']}, body preview: {response['body'][:200]}")

        payload = json.loads(response["body"])
        if not payload or "data" not in payload:
            raise Exception("Unexpected Bid.cars payload format")

        return payload["data"]


def send_telegram_message(text: str) -> bool:
    """Send message via Telegram bot."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning('Telegram credentials not configured')
        return False

    url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': text,
        'parse_mode': 'HTML',
        'disable_web_page_preview': False,
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return response.json().get('ok', False)
    except Exception as e:
        logger.error(f'Telegram send failed: {e}')
        return False


def format_listing_message(listing: Dict[str, Any]) -> str:
    """Format a listing as Telegram message."""
    name = listing.get('name_long') or listing.get('name', 'Unknown')
    lot = listing.get('lot', 'N/A')
    vin = listing.get('vin', 'N/A')
    year = listing.get('name', '').split()[0] if listing.get('name') else 'N/A'
    odometer = listing.get('odometer_substr', 'N/A')
    location = listing.get('location', 'N/A')
    prebid = listing.get('prebid_price', 'N/A')
    final_bid = listing.get('final_bid_formatted')
    status = listing.get('search_status', 'N/A')
    url = f'https://bid.cars/en/lot/{lot}'

    lines = [
        f'{TELEGRAM_MESSAGE_PREFIX}',
        f'',
        f'<b>{name}</b>',
        f'',
        f'📅 Year: {year}',
        f'🔢 Lot: <code>{lot}</code>',
        f'🔑 VIN: <code>{vin}</code>',
        f'📊 Odometer: {odometer}K miles',
        f'📍 Location: {location}',
        f'💰 Prebid: {prebid}',
    ]
    if final_bid:
        lines.append(f'🏆 Final Bid: {final_bid}')
    lines.append(f'📌 Status: {status}')
    lines.append(f'')
    lines.append(f'🔗 <a href="{url}">View on Bid.cars</a>')
    return '\n'.join(lines)


def run_check(dry_run: bool = False) -> Dict[str, Any]:
    """Main check function."""
    try:
        redis = get_redis_client()
        is_init = redis.exists(STORAGE_INIT_KEY)

        if not is_init:
            logger.info('[check] First run: initializing storage')
            listings = fetch_listings()
            seen_lots = {listing.get('lot') for listing in listings if listing.get('lot')}
            if seen_lots:
                redis.sadd(STORAGE_SEEN_KEY, *seen_lots)
            redis.set(STORAGE_INIT_KEY, '1')
            return {'sent': 0, 'reason': 'bootstrap', 'total': len(listings)}

        listings = fetch_listings()
        logger.info(f'[check] Fetched {len(listings)} listings')
        seen_lots = redis.smembers(STORAGE_SEEN_KEY)
        new_listings = [l for l in listings if l.get('lot') and l.get('lot') not in seen_lots]

        if not new_listings:
            return {'sent': 0, 'reason': 'no_new_listings', 'total': len(listings)}

        sent = 0
        for listing in new_listings:
            lot = listing.get('lot')
            if not lot:
                continue
            message = format_listing_message(listing)
            if send_telegram_message(message):
                redis.sadd(STORAGE_SEEN_KEY, lot)
                sent += 1
                time.sleep(0.5)

        return {'sent': sent, 'reason': 'new_listings', 'new_count': len(new_listings), 'total': len(listings)}

    except Exception as e:
        logger.error(f'[check] Failed: {e}', exc_info=True)
        raise


if __name__ == '__main__':
    required = ['TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID', 'UPSTASH_REDIS_REST_URL', 'UPSTASH_REDIS_REST_TOKEN']
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        logger.error(f'Missing required environment variables: {", ".join(missing)}')
        exit(1)

    logger.info('Starting one-time check...')
    result = run_check()
    logger.info(f'Check completed: {result}')
