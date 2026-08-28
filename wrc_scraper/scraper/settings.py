from wrc_scraper.config import settings as env

BOT_NAME = "wrc"
SPIDER_MODULES = ["wrc_scraper.scraper.spiders"]

# --- POLITENESS: fast without getting blocked ---
ROBOTSTXT_OBEY = True
DOWNLOAD_DELAY = env.download_delay
CONCURRENT_REQUESTS = env.concurrent_requests
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 30.0

# --- RESILIENCE ---
RETRY_ENABLED = True
RETRY_TIMES = 3

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"