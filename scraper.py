#!/usr/bin/env python3
"""
Production-Grade EPD PDF Scraper System
=======================================

A robust, cloud-deployable PDF scraper for Punjab EPD AQI reports with:
- SQLite database for tracking and duplicate prevention
- Bandwidth-aware throttling and concurrent downloads
- Playwright automation for dynamic content
- GitHub integration with automated commits
- Comprehensive logging and error handling
- Docker and cloud deployment ready

Author: AI Assistant
Version: 2.0.0
License: MIT
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import random
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
from urllib.parse import urljoin, unquote
import logging.handlers

import aiofiles
import aiohttp
import psutil
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from pypdf import PdfReader

# ========== CONFIGURATION ==========
@dataclass
class ScraperConfig:
    """Enterprise-grade configuration with environment variable support"""
    
    # Core scraping settings
    BASE_URL: str = field(default_factory=lambda: os.getenv("EPD_BASE_URL", "https://epd.punjab.gov.pk"))
    CHECK_DAYS: int = field(default_factory=lambda: int(os.getenv("CHECK_DAYS", "21")))
    DATE_FORMAT: str = "%m/%d/%Y"
    LOOP_INTERVAL_HOURS: float = field(default_factory=lambda: float(os.getenv("LOOP_INTERVAL_HOURS", "3.0")))
    
    # File and directory paths
    BASE_DIR: Path = field(default_factory=lambda: Path(os.getenv("BASE_DIR", "epd_scraper_data")))
    DOWNLOAD_FOLDER: Path = field(init=False)
    LOGS_FOLDER: Path = field(init=False)
    REPORTS_FOLDER: Path = field(init=False)
    SCREENSHOTS_FOLDER: Path = field(init=False)
    DATABASE_PATH: Path = field(init=False)
    
    # Performance settings
    MAX_CONCURRENT_DOWNLOADS: int = field(default_factory=lambda: int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "6")))
    REQUEST_TIMEOUT: int = field(default_factory=lambda: int(os.getenv("REQUEST_TIMEOUT", "60")))
    PAGE_LOAD_TIMEOUT: int = field(default_factory=lambda: int(os.getenv("PAGE_LOAD_TIMEOUT", "45000")))
    NAVIGATION_RETRIES: int = field(default_factory=lambda: int(os.getenv("NAVIGATION_RETRIES", "5")))
    DOWNLOAD_RETRIES: int = field(default_factory=lambda: int(os.getenv("DOWNLOAD_RETRIES", "4")))
    
    # Bandwidth management
    HIGH_BANDWIDTH_THRESHOLD: float = field(default_factory=lambda: float(os.getenv("HIGH_BANDWIDTH_THRESHOLD", "25.0")))
    LOW_BANDWIDTH_THRESHOLD: float = field(default_factory=lambda: float(os.getenv("LOW_BANDWIDTH_THRESHOLD", "1.0")))
    MIN_DELAY: float = field(default_factory=lambda: float(os.getenv("MIN_DELAY", "0.1")))
    MAX_DELAY: float = field(default_factory=lambda: float(os.getenv("MAX_DELAY", "2.0")))
    JITTER_FACTOR: float = 0.3
    
    # Processing settings
    MAX_PROCESS_RETRIES: int = field(default_factory=lambda: int(os.getenv("MAX_PROCESS_RETRIES", "3")))
    MAX_RETRY_DELAY: int = field(default_factory=lambda: int(os.getenv("MAX_RETRY_DELAY", "120")))
    MIN_PDF_SIZE: int = field(default_factory=lambda: int(os.getenv("MIN_PDF_SIZE", "2048")))
    
    # Browser settings
    HEADLESS: bool = field(default_factory=lambda: os.getenv("HEADLESS", "true").lower() == "true")
    BROWSER_TIMEOUT: int = field(default_factory=lambda: int(os.getenv("BROWSER_TIMEOUT", "180000")))
    
    # GitHub integration
    GITHUB_TOKEN: str = field(default_factory=lambda: os.getenv("GITHUB_TOKEN", "ghp_hvFs3d1xXSBEezB5Gb0Z04o5sQafN42EVJn"))
    GITHUB_REPO: str = field(default_factory=lambda: os.getenv("GITHUB_REPO", "saadaliadalat/Aqi-pdfs"))
    GITHUB_BRANCH: str = field(default_factory=lambda: os.getenv("GITHUB_BRANCH", "main"))
    GITHUB_USERNAME: str = field(default_factory=lambda: os.getenv("GITHUB_USERNAME", "EPD-Scraper-Bot"))
    GITHUB_EMAIL: str = field(default_factory=lambda: os.getenv("GITHUB_EMAIL", "scraper@epd-monitor.com"))
    
    # Logging configuration
    LOG_LEVEL: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    LOG_ROTATION_SIZE: int = field(default_factory=lambda: int(os.getenv("LOG_ROTATION_SIZE", "10485760")))  # 10MB
    LOG_BACKUP_COUNT: int = field(default_factory=lambda: int(os.getenv("LOG_BACKUP_COUNT", "5")))
    CONSOLE_LOGGING: bool = field(default_factory=lambda: os.getenv("CONSOLE_LOGGING", "true").lower() == "true")
    
    # Development and debugging
    TAKE_SCREENSHOTS: bool = field(default_factory=lambda: os.getenv("TAKE_SCREENSHOTS", "true").lower() == "true")
    DEBUG_MODE: bool = field(default_factory=lambda: os.getenv("DEBUG_MODE", "false").lower() == "true")
    
    def __post_init__(self):
        """Initialize derived paths and create directories"""
        self.BASE_DIR = Path(self.BASE_DIR).resolve()
        self.DOWNLOAD_FOLDER = self.BASE_DIR / "pdfs"
        self.LOGS_FOLDER = self.BASE_DIR / "logs"
        self.REPORTS_FOLDER = self.BASE_DIR / "reports"
        self.SCREENSHOTS_FOLDER = self.BASE_DIR / "screenshots"
        self.DATABASE_PATH = self.BASE_DIR / "epd_scraper.db"
        
        # Create all necessary directories
        for folder in [self.BASE_DIR, self.DOWNLOAD_FOLDER, self.LOGS_FOLDER, 
                      self.REPORTS_FOLDER, self.SCREENSHOTS_FOLDER]:
            folder.mkdir(parents=True, exist_ok=True)
        
        # Validate critical settings
        if self.CHECK_DAYS > 365:
            raise ValueError("CHECK_DAYS cannot exceed 365")
        if self.MAX_CONCURRENT_DOWNLOADS > 20:
            raise ValueError("MAX_CONCURRENT_DOWNLOADS cannot exceed 20")


# ========== LOGGING SYSTEM ==========
class AdvancedLogger:
    """Production-grade logging with rotation, structured format, and multiple outputs"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.logger = logging.getLogger("EPDScraper")
        self.setup_logging()
    
    def setup_logging(self):
        """Configure comprehensive logging system"""
        self.logger.setLevel(getattr(logging, self.config.LOG_LEVEL.upper()))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Detailed formatter with context
        formatter = logging.Formatter(
            fmt='%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Rotating file handler
        log_file = self.config.LOGS_FOLDER / "epd_scraper.log"
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=self.config.LOG_ROTATION_SIZE,
            backupCount=self.config.LOG_BACKUP_COUNT,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(file_handler)
        
        # Error-specific file handler
        error_log_file = self.config.LOGS_FOLDER / "epd_scraper_errors.log"
        error_handler = logging.handlers.RotatingFileHandler(
            error_log_file,
            maxBytes=self.config.LOG_ROTATION_SIZE // 2,
            backupCount=3,
            encoding='utf-8'
        )
        error_handler.setFormatter(formatter)
        error_handler.setLevel(logging.ERROR)
        self.logger.addHandler(error_handler)
        
        # Console handler (optional)
        if self.config.CONSOLE_LOGGING:
            console_handler = logging.StreamHandler(sys.stdout)
            console_formatter = logging.Formatter(
                fmt='%(asctime)s | %(levelname)-8s | %(message)s',
                datefmt='%H:%M:%S'
            )
            console_handler.setFormatter(console_formatter)
            console_handler.setLevel(getattr(logging, self.config.LOG_LEVEL.upper()))
            self.logger.addHandler(console_handler)
        
        self.logger.info("Advanced logging system initialized")
    
    def get_logger(self) -> logging.Logger:
        return self.logger


# ========== UTILITY FUNCTIONS ==========
class ProductionUtils:
    """Production-grade utility functions with comprehensive error handling"""
    
    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """Sanitize filename for cross-platform compatibility with enhanced cleaning"""
        if not filename:
            return "unnamed_file.pdf"
        
        # Remove HTML tags and decode entities
        filename = re.sub(r'<[^>]*>', '', filename)
        filename = filename.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
        
        # Replace problematic characters
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        filename = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', filename)  # Control characters
        filename = re.sub(r'[\s_]+', '_', filename)  # Multiple spaces/underscores
        filename = filename.strip('_. ')
        
        # Ensure proper extension
        if not filename.lower().endswith('.pdf'):
            filename += '.pdf'
        
        # Limit length while preserving extension
        if len(filename) > 200:
            name_part = filename[:-4]
            filename = name_part[:196] + '.pdf'
        
        return filename
    
    @staticmethod
    def calculate_file_hash(filepath: Path, algorithm: str = 'sha256') -> str:
        """Calculate file hash with support for multiple algorithms"""
        try:
            hash_obj = hashlib.new(algorithm)
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_obj.update(chunk)
            return hash_obj.hexdigest()
        except Exception:
            return ""
    
    @staticmethod
    def is_valid_pdf(filepath: Path, min_size: int = 2048) -> Tuple[bool, str]:
        """Comprehensive PDF validation with detailed error reporting"""
        try:
            if not filepath.exists():
                return False, "File does not exist"
            
            file_size = filepath.stat().st_size
            if file_size < min_size:
                return False, f"File too small: {file_size} bytes (minimum: {min_size})"
            
            # Check PDF signature
            with open(filepath, 'rb') as f:
                header = f.read(1024)
                if not header.startswith(b'%PDF'):
                    return False, "Invalid PDF header"
            
            # Parse PDF structure
            try:
                reader = PdfReader(filepath)
                page_count = len(reader.pages)
                
                if page_count == 0:
                    return False, "PDF has no pages"
                
                # Try to access first page to ensure it's readable
                first_page = reader.pages[0]
                _ = first_page.extract_text()
                
                return True, f"Valid PDF with {page_count} pages"
                
            except Exception as e:
                return False, f"PDF parsing failed: {str(e)[:100]}"
            
        except Exception as e:
            return False, f"Validation error: {str(e)[:100]}"
    
    @staticmethod
    async def measure_bandwidth() -> float:
        """Advanced bandwidth monitoring with error handling"""
        try:
            net_io_start = psutil.net_io_counters()
            start_bytes = net_io_start.bytes_recv + net_io_start.bytes_sent
            start_time = time.time()
            
            await asyncio.sleep(0.5)
            
            net_io_end = psutil.net_io_counters()
            end_bytes = net_io_end.bytes_recv + net_io_end.bytes_sent
            end_time = time.time()
            
            bytes_transferred = end_bytes - start_bytes
            time_elapsed = end_time - start_time
            
            if time_elapsed > 0:
                mbps = (bytes_transferred * 8) / (1024 * 1024 * time_elapsed)
                return max(mbps, 0.1)
            
            return 5.0  # Default fallback
            
        except Exception:
            return 5.0  # Conservative fallback
    
    @staticmethod
    def calculate_adaptive_delay(bandwidth: float, config: ScraperConfig) -> float:
        """Intelligent delay calculation based on current network conditions"""
        if bandwidth >= config.HIGH_BANDWIDTH_THRESHOLD:
            base_delay = config.MIN_DELAY
        elif bandwidth <= config.LOW_BANDWIDTH_THRESHOLD:
            base_delay = config.MAX_DELAY
        else:
            # Linear interpolation between thresholds
            ratio = (config.HIGH_BANDWIDTH_THRESHOLD - bandwidth) / \
                   (config.HIGH_BANDWIDTH_THRESHOLD - config.LOW_BANDWIDTH_THRESHOLD)
            base_delay = config.MIN_DELAY + ratio * (config.MAX_DELAY - config.MIN_DELAY)
        
        # Add jitter to prevent thundering herd
        jitter = random.uniform(-config.JITTER_FACTOR, config.JITTER_FACTOR)
        delay = base_delay * (1 + jitter)
        
        return max(delay, 0.05)
    
    @staticmethod
    def get_rotating_user_agents() -> List[str]:
        """Comprehensive list of realistic user agents for rotation"""
        return [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0'
        ]
    
    @staticmethod
    async def create_summary_report(stats: Dict, config: ScraperConfig) -> str:
        """Generate comprehensive session summary report"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        report_lines = [
            "=" * 80,
            f"EPD PDF Scraper Session Summary - {timestamp}",
            "=" * 80,
            "",
            f"Session Configuration:",
            f"  • Date Range: Last {config.CHECK_DAYS} days",
            f"  • Target URL: {config.BASE_URL}/aqi",
            f"  • Download Folder: {config.DOWNLOAD_FOLDER}",
            f"  • Max Concurrent Downloads: {config.MAX_CONCURRENT_DOWNLOADS}",
            "",
            f"Session Results:",
            f"  • New PDFs Downloaded: {stats.get('new_files_count', 0)}",
            f"  • Duplicates Skipped: {stats.get('duplicates_skipped', 0)}",
            f"  • Errors Encountered: {stats.get('total_errors', 0)}",
            f"  • Dates Processed: {stats.get('dates_processed', 0)}",
            f"  • Total Processing Time: {stats.get('total_time', 0):.2f} seconds",
            "",
            f"File Operations:",
            f"  • Total Files in Database: {stats.get('db_total_files', 0)}",
            f"  • Successful Downloads: {stats.get('successful_downloads', 0)}",
            f"  • Failed Downloads: {stats.get('failed_downloads', 0)}",
            f"  • PDF Validation Failures: {stats.get('validation_failures', 0)}",
            "",
            f"GitHub Integration:",
            f"  • Auto-commit Enabled: {bool(config.GITHUB_TOKEN and config.GITHUB_REPO)}",
            f"  • Files Committed: {stats.get('github_files_committed', 0)}",
            f"  • Commit Status: {stats.get('github_status', 'Not configured')}",
            "",
            "=" * 80
        ]
        
        report_content = "\n".join(report_lines)
        
        # Save to reports folder
        report_filename = f"scraper_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        report_path = config.REPORTS_FOLDER / report_filename
        
        async with aiofiles.open(report_path, 'w', encoding='utf-8') as f:
            await f.write(report_content)
        
        return report_content


# ========== DATABASE MANAGER ==========
class DatabaseManager:
    """Enterprise SQLite database manager with connection pooling and transactions"""
    
    def __init__(self, config: ScraperConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.db_path = config.DATABASE_PATH
        self._init_database()
    
    def _init_database(self):
        """Initialize database with comprehensive schema and indexes"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA foreign_keys = ON")
                conn.execute("PRAGMA journal_mode = WAL")  # Better concurrency
                conn.execute("PRAGMA synchronous = NORMAL")  # Better performance
                
                # Main downloads table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS downloads (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT NOT NULL,
                        filename TEXT NOT NULL,
                        url TEXT NOT NULL,
                        status TEXT NOT NULL,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        retries INTEGER DEFAULT 0,
                        file_hash TEXT,
                        file_size INTEGER,
                        error_message TEXT,
                        processing_time REAL,
                        UNIQUE(date, filename, url)
                    )
                """)
                
                # Session tracking table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS sessions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_start DATETIME DEFAULT CURRENT_TIMESTAMP,
                        session_end DATETIME,
                        total_processed INTEGER DEFAULT 0,
                        total_downloaded INTEGER DEFAULT 0,
                        total_errors INTEGER DEFAULT 0,
                        total_duplicates INTEGER DEFAULT 0,
                        status TEXT DEFAULT 'RUNNING'
                    )
                """)
                
                # Performance indexes
                conn.execute("CREATE INDEX IF NOT EXISTS idx_downloads_date ON downloads(date)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_downloads_hash ON downloads(file_hash)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_downloads_timestamp ON downloads(timestamp)")
                
                conn.commit()
                self.logger.info("Database initialized successfully")
                
        except sqlite3.Error as e:
            self.logger.error(f"Database initialization failed: {e}")
            raise
    
    def is_already_downloaded(self, date_str: str, filename: str, url: str) -> bool:
        """Check if file is already successfully downloaded (duplicate prevention)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) FROM downloads 
                    WHERE date = ? AND filename = ? AND url = ? AND status = 'SUCCESS'
                """, (date_str, filename, url))
                
                count = cursor.fetchone()[0]
                return count > 0
                
        except sqlite3.Error as e:
            self.logger.error(f"Database query failed: {e}")
            return False
    
    def is_duplicate_by_hash(self, file_hash: str) -> bool:
        """Check if file with same hash already exists"""
        if not file_hash:
            return False
            
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) FROM downloads 
                    WHERE file_hash = ? AND status = 'SUCCESS'
                """, (file_hash,))
                
                count = cursor.fetchone()[0]
                return count > 0
                
        except sqlite3.Error as e:
            self.logger.error(f"Hash duplicate check failed: {e}")
            return False
    
    def log_download_record(self, date_str: str, filename: str, url: str, status: str,
                           retries: int = 0, file_hash: str = "", file_size: int = 0,
                           error_message: str = "", processing_time: float = 0.0) -> bool:
        """Log download record with comprehensive metadata"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO downloads 
                    (date, filename, url, status, retries, file_hash, file_size, error_message, processing_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (date_str, filename, url, status, retries, file_hash, file_size, error_message, processing_time))
                
                conn.commit()
                self.logger.debug(f"Logged download record: {filename} - {status}")
                return True
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to log download record: {e}")
            return False
    
    def get_download_statistics(self, days: int = None) -> Dict[str, int]:
        """Get comprehensive download statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                base_query = "SELECT status, COUNT(*) FROM downloads"
                params = []
                
                if days:
                    base_query += " WHERE timestamp >= datetime('now', '-{} days')".format(days)
                
                base_query += " GROUP BY status"
                
                cursor.execute(base_query, params)
                results = cursor.fetchall()
                
                stats = {status: count for status, count in results}
                
                # Get total count
                cursor.execute("SELECT COUNT(*) FROM downloads")
                stats['total'] = cursor.fetchone()[0]
                
                return stats
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to get statistics: {e}")
            return {}
    
    def start_session(self) -> int:
        """Start a new scraping session"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO sessions (session_start, status) VALUES (CURRENT_TIMESTAMP, 'RUNNING')
                """)
                session_id = cursor.lastrowid
                conn.commit()
                self.logger.info(f"Started new scraping session: {session_id}")
                return session_id
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to start session: {e}")
            return 0
    
    def end_session(self, session_id: int, stats: Dict):
        """End scraping session with statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE sessions SET 
                    session_end = CURRENT_TIMESTAMP,
                    total_processed = ?,
                    total_downloaded = ?,
                    total_errors = ?,
                    total_duplicates = ?,
                    status = 'COMPLETED'
                    WHERE id = ?
                """, (
                    stats.get('total_processed', 0),
                    stats.get('total_downloaded', 0),
                    stats.get('total_errors', 0),
                    stats.get('total_duplicates', 0),
                    session_id
                ))
                conn.commit()
                self.logger.info(f"Ended scraping session: {session_id}")
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to end session: {e}")


# ========== PDF DOWNLOADER ==========
class AdvancedPDFDownloader:
    """High-performance PDF downloader with intelligent retry logic and validation"""
    
    def __init__(self, config: ScraperConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.session_downloaded_hashes: Set[str] = set()
    
    @asynccontextmanager
    async def get_http_session(self):
        """Create optimized aiohttp session with advanced configuration"""
        connector = aiohttp.TCPConnector(
            limit=30,
            limit_per_host=self.config.MAX_CONCURRENT_DOWNLOADS + 2,
            keepalive_timeout=60,
            enable_cleanup_closed=True,
            use_dns_cache=True,
            ttl_dns_cache=300
        )
        
        timeout = aiohttp.ClientTimeout(
            total=self.config.REQUEST_TIMEOUT,
            connect=10,
            sock_read=self.config.REQUEST_TIMEOUT - 10
        )
        
        session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': random.choice(ProductionUtils.get_rotating_user_agents()),
                'Accept': 'application/pdf,application/octet-stream,*/*;q=0.9',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
        )
        
        try:
            yield session
        finally:
            await session.close()
    
    async def download_single_pdf(self, url: str, filename: str, date_str: str) -> Tuple[bool, str, Dict]:
        """Download single PDF with comprehensive error handling and validation"""
        start_time = time.time()
        sanitized_filename = ProductionUtils.sanitize_filename(filename)
        filepath = self.config.DOWNLOAD_FOLDER / sanitized_filename
        
        download_stats = {
            'url': url,
            'filename': sanitized_filename,
            'status': 'PENDING',
            'error_message': '',
            'processing_time': 0.0,
            'file_size': 0,
            'file_hash': '',
            'retries': 0
        }
        
        # Check if file already exists and is valid
        if filepath.exists():
            is_valid, validation_msg = ProductionUtils.is_valid_pdf(filepath, self.config.MIN_PDF_SIZE)
            if is_valid:
                file_hash = ProductionUtils.calculate_file_hash(filepath)
                if file_hash and file_hash not in self.session_downloaded_hashes:
                    self.session_downloaded_hashes.add(file_hash)
                    download_stats.update({
                        'status': 'EXISTS_VALID',
                        'file_size': filepath.stat().st_size,
                        'file_hash': file_hash,
                        'processing_time': time.time() - start_time
                    })
                    return True, str(filepath), download_stats
                else:
                    download_stats.update({
                        'status': 'DUPLICATE_HASH',
                        'processing_time': time.time() - start_time
                    })
                    return True, "", download_stats
            else:
                # Remove invalid file
                try:
                    filepath.unlink()
                    self.logger.warning(f"Removed invalid PDF: {filepath} - {validation_msg}")
                except Exception as e:
                    self.logger.error(f"Failed to remove invalid PDF {filepath}: {e}")
        
        # Download with retry logic
        for attempt in range(self.config.DOWNLOAD_RETRIES):
            download_stats['retries'] = attempt
            
            if attempt > 0:
                # Adaptive delay based on bandwidth and attempt number
                bandwidth = await ProductionUtils.measure_bandwidth()
                adaptive_delay = ProductionUtils.calculate_adaptive_delay(bandwidth, self.config)
                retry_delay = min(2 ** attempt, 30) + adaptive_delay + random.uniform(0, 2)
                
                self.logger.debug(f"Retry {attempt} for {sanitized_filename} after {retry_delay:.2f}s delay")
                await asyncio.sleep(retry_delay)
            
            try:
                async with self.get_http_session() as session:
                    # Dynamic headers for each request
                    headers = {
                        'Referer': f'{self.config.BASE_URL}/aqi',
                        'User-Agent': random.choice(ProductionUtils.get_rotating_user_agents()),
                        'Cache-Control': 'no-cache',
                        'Pragma': 'no-cache'
                    }
                    
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            content = await response.read()
                            
                            # Validate content before saving
                            if len(content) < self.config.MIN_PDF_SIZE:
                                raise ValueError(f"Content too small: {len(content)} bytes")
                            
                            if not content.startswith(b'%PDF'):
                                raise ValueError("Invalid PDF signature")
                            
                            # Save to temporary file first
                            temp_filepath = filepath.with_suffix('.tmp')
                            async with aiofiles.open(temp_filepath, 'wb') as f:
                                await f.write(content)
                            
                            # Validate downloaded file
                            is_valid, validation_msg = ProductionUtils.is_valid_pdf(temp_filepath, self.config.MIN_PDF_SIZE)
                            if not is_valid:
                                temp_filepath.unlink()
                                raise ValueError(f"PDF validation failed: {validation_msg}")
                            
                            # Check for duplicate by hash
                            file_hash = ProductionUtils.calculate_file_hash(temp_filepath)
                            if file_hash in self.session_downloaded_hashes:
                                temp_filepath.unlink()
                                download_stats.update({
                                    'status': 'DUPLICATE_HASH',
                                    'file_hash': file_hash,
                                    'processing_time': time.time() - start_time
                                })
                                return True, "", download_stats
                            
                            # Move to final location
                            temp_filepath.rename(filepath)
                            self.session_downloaded_hashes.add(file_hash)
                            
                            download_stats.update({
                                'status': 'SUCCESS',
                                'file_size': filepath.stat().st_size,
                                'file_hash': file_hash,
                                'processing_time': time.time() - start_time
                            })
                            
                            self.logger.info(f"Successfully downloaded: {sanitized_filename} ({len(content)} bytes)")
                            return True, str(filepath), download_stats
                        
                        elif response.status == 429:
                            # Rate limited - wait longer
                            wait_time = 30 + random.uniform(0, 10)
                            self.logger.warning(f"Rate limited (429), waiting {wait_time:.1f}s")
                            await asyncio.sleep(wait_time)
                            continue
                        
                        elif response.status in [404, 403]:
                            # Permanent errors
                            error_msg = f"HTTP {response.status} - URL not accessible"
                            download_stats.update({
                                'status': 'FAILED_PERMANENT',
                                'error_message': error_msg,
                                'processing_time': time.time() - start_time
                            })
                            self.logger.error(f"Permanent failure for {sanitized_filename}: {error_msg}")
                            return False, "", download_stats
                        
                        else:
                            raise aiohttp.ClientError(f"HTTP {response.status}")
            
            except asyncio.TimeoutError:
                error_msg = f"Download timeout after {self.config.REQUEST_TIMEOUT}s"
                self.logger.warning(f"Timeout downloading {sanitized_filename}: {error_msg}")
                download_stats['error_message'] = error_msg
                
            except aiohttp.ClientError as e:
                error_msg = f"Network error: {str(e)[:100]}"
                self.logger.warning(f"Network error downloading {sanitized_filename}: {error_msg}")
                download_stats['error_message'] = error_msg
                
            except ValueError as e:
                error_msg = f"Content validation error: {str(e)[:100]}"
                self.logger.warning(f"Validation error for {sanitized_filename}: {error_msg}")
                download_stats['error_message'] = error_msg
                
            except Exception as e:
                error_msg = f"Unexpected error: {str(e)[:100]}"
                self.logger.error(f"Unexpected error downloading {sanitized_filename}: {error_msg}")
                download_stats['error_message'] = error_msg
        
        # All retries failed
        download_stats.update({
            'status': 'FAILED_MAX_RETRIES',
            'processing_time': time.time() - start_time
        })
        
        self.logger.error(f"Failed to download {sanitized_filename} after {self.config.DOWNLOAD_RETRIES} attempts")
        return False, "", download_stats
    
    async def download_pdfs_batch(self, pdf_links: List[Dict], date_str: str) -> Dict:
        """Download multiple PDFs concurrently with comprehensive statistics"""
        if not pdf_links:
            return {
                'total_links': 0,
                'successful_downloads': 0,
                'failed_downloads': 0,
                'duplicates_skipped': 0,
                'existing_valid': 0,
                'downloaded_files': [],
                'download_details': []
            }
        
        self.logger.info(f"Starting batch download of {len(pdf_links)} PDFs for {date_str}")
        
        # Semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.config.MAX_CONCURRENT_DOWNLOADS)
        
        async def download_with_semaphore(link_info):
            async with semaphore:
                # Add small random delay to prevent thundering herd
                await asyncio.sleep(random.uniform(0.1, 0.5))
                return await self.download_single_pdf(
                    link_info['url'], 
                    link_info['filename'], 
                    date_str
                )
        
        # Execute downloads concurrently
        download_tasks = [download_with_semaphore(link) for link in pdf_links]
        results = await asyncio.gather(*download_tasks, return_exceptions=True)
        
        # Process results and compile statistics
        batch_stats = {
            'total_links': len(pdf_links),
            'successful_downloads': 0,
            'failed_downloads': 0,
            'duplicates_skipped': 0,
            'existing_valid': 0,
            'downloaded_files': [],
            'download_details': []
        }
        
        for i, result in enumerate(results):
            link_info = pdf_links[i]
            
            if isinstance(result, Exception):
                batch_stats['failed_downloads'] += 1
                batch_stats['download_details'].append({
                    'url': link_info['url'],
                    'filename': link_info['filename'],
                    'status': 'EXCEPTION',
                    'error_message': str(result)[:200]
                })
                self.logger.error(f"Exception in download task: {result}")
                continue
            
            success, filepath, download_stats = result
            batch_stats['download_details'].append(download_stats)
            
            if success:
                if download_stats['status'] == 'SUCCESS':
                    batch_stats['successful_downloads'] += 1
                    batch_stats['downloaded_files'].append(filepath)
                elif download_stats['status'] in ['DUPLICATE_HASH', 'DUPLICATE']:
                    batch_stats['duplicates_skipped'] += 1
                elif download_stats['status'] == 'EXISTS_VALID':
                    batch_stats['existing_valid'] += 1
            else:
                batch_stats['failed_downloads'] += 1
        
        self.logger.info(f"Batch download completed for {date_str}: "
                        f"{batch_stats['successful_downloads']} new, "
                        f"{batch_stats['existing_valid']} existing, "
                        f"{batch_stats['duplicates_skipped']} duplicates, "
                        f"{batch_stats['failed_downloads']} failed")
        
        return batch_stats


# ========== WEB SCRAPER ==========
class AdvancedWebScraper:
    """Production-grade web scraper using Playwright with robust error handling"""
    
    def __init__(self, config: ScraperConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.playwright = None
        self.browser = None
        self.context = None
    
    async def initialize_browser(self) -> Tuple[bool, str]:
        """Initialize Playwright browser with optimal settings"""
        try:
            self.playwright = await async_playwright().start()
            
            # Launch browser with production-ready settings
            self.browser = await self.playwright.chromium.launch(
                headless=self.config.HEADLESS,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-blink-features=AutomationControlled',
                    '--no-first-run',
                    '--disable-default-apps',
                    '--disable-extensions',
                    '--disable-plugins',
                    '--disable-images' if not self.config.DEBUG_MODE else '',
                    '--disable-javascript-harmony-shipping',
                    '--memory-pressure-off'
                ],
                timeout=self.config.BROWSER_TIMEOUT
            )
            
            # Create context with realistic settings
            self.context = await self.browser.new_context(
                user_agent=random.choice(ProductionUtils.get_rotating_user_agents()),
                viewport={'width': 1366, 'height': 768},
                ignore_https_errors=True,
                java_script_enabled=True,
                accept_downloads=False,
                bypass_csp=True,
                extra_http_headers={
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1'
                }
            )
            
            self.logger.info("Browser initialized successfully")
            return True, "Browser ready"
            
        except Exception as e:
            error_msg = f"Browser initialization failed: {str(e)[:200]}"
            self.logger.error(error_msg)
            await self.cleanup_browser()
            return False, error_msg
    
    async def cleanup_browser(self):
        """Properly cleanup browser resources"""
        try:
            if self.context:
                await self.context.close()
                self.context = None
            
            if self.browser:
                await self.browser.close()
                self.browser = None
            
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
            
            self.logger.info("Browser cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Browser cleanup error: {e}")
    
    def extract_pdf_links_comprehensive(self, html_content: str, base_url: str) -> List[Dict[str, str]]:
        """Extract PDF links using multiple advanced patterns and strategies"""
        pdf_links = []
        seen_urls = set()
        
        # Multiple regex patterns for different link formats
        extraction_patterns = [
            # Standard href patterns
            r'<a[^>]*href=["\']([^"\']*\.pdf[^"\']*)["\'][^>]*>(.*?)</a>',
            r'<a[^>]*href=["\']([^"\']*file=[^"\']*\.pdf[^"\']*)["\'][^>]*>(.*?)</a>',
            
            # System files patterns
            r'<a[^>]*href=["\'](/system/files\?file=([^"\']*\.pdf)[^"\']*)["\'][^>]*>(.*?)</a>',
            r'<a[^>]*href=["\'](/sites/default/files/[^"\']*\.pdf[^"\']*)["\'][^>]*>(.*?)</a>',
            
            # Data attributes and JavaScript patterns
            r'data-file=["\']([^"\']*\.pdf[^"\']*)["\']',
            r'window\.open\(["\']([^"\']*\.pdf[^"\']*)["\']',
            
            # Drupal-specific patterns
            r'<a[^>]*href=["\'](/node/\d+/download[^"\']*)["\'][^>]*>(.*?)</a>',
        ]
        
        for pattern in extraction_patterns:
            try:
                matches = re.findall(pattern, html_content, re.IGNORECASE | re.DOTALL)
                
                for match in matches:
                    if isinstance(match, tuple):
                        if len(match) >= 2:
                            url_part, *text_parts = match
                            link_text = text_parts[-1] if text_parts else ""
                        else:
                            url_part = match[0]
                            link_text = ""
                    else:
                        url_part = match
                        link_text = ""
                    
                    # Skip if already processed
                    if url_part in seen_urls:
                        continue
                    
                    # Create full URL
                    if url_part.startswith('http'):
                        full_url = url_part
                    else:
                        full_url = urljoin(base_url, url_part)
                    
                    seen_urls.add(url_part)
                    
                    # Extract filename from URL
                    if 'file=' in url_part:
                        filename = unquote(url_part.split('file=')[-1].split('&')[0])
                    else:
                        filename = unquote(url_part.split('/')[-1].split('?')[0])
                    
                    # Clean and validate filename
                    filename = ProductionUtils.sanitize_filename(filename)
                    
                    # Clean link text for better filename if available
                    clean_link_text = re.sub(r'<[^>]*>', '', link_text).strip()
                    if clean_link_text and len(clean_link_text) > 10 and len(clean_link_text) < 200:
                        filename = ProductionUtils.sanitize_filename(clean_link_text)
                    
                    pdf_links.append({
                        'url': full_url,
                        'filename': filename,
                        'link_text': clean_link_text,
                        'original_url': url_part
                    })
                    
            except Exception as e:
                self.logger.warning(f"Pattern matching error: {e}")
                continue
        
        # Remove duplicates based on URL and filename
        unique_links = []
        seen_combinations = set()
        
        for link in pdf_links:
            combination = (link['url'], link['filename'])
            if combination not in seen_combinations:
                unique_links.append(link)
                seen_combinations.add(combination)
        
        # Limit results to prevent overwhelming
        if len(unique_links) > 20:
            self.logger.warning(f"Found {len(unique_links)} PDF links, limiting to 20")
            unique_links = unique_links[:20]
        
        self.logger.debug(f"Extracted {len(unique_links)} unique PDF links")
        return unique_links
    
    async def navigate_and_submit_date_form(self, page: Page, date_str: str) -> Tuple[bool, str]:
        """Navigate to AQI page and submit date form with robust error handling"""
        target_url = f"{self.config.BASE_URL}/aqi"
        
        for attempt in range(self.config.NAVIGATION_RETRIES):
            try:
                self.logger.debug(f"Navigation attempt {attempt + 1} for {date_str}")
                
                # Navigate to the page
                await page.goto(
                    target_url, 
                    wait_until="domcontentloaded", 
                    timeout=self.config.PAGE_LOAD_TIMEOUT
                )
                
                # Wait for page to stabilize
                await asyncio.sleep(1)
                
                # Try multiple selectors for the date input field
                date_input_selectors = [
                    "#edit-field-aqr-date-value",
                    "input[name='field_aqr_date_value']",
                    "input[type='date']",
                    ".form-date input",
                    "[data-drupal-date-format] input"
                ]
                
                date_input_element = None
                for selector in date_input_selectors:
                    try:
                        await page.wait_for_selector(selector, timeout=5000)
                        date_input_element = page.locator(selector)
                        break
                    except:
                        continue
                
                if not date_input_element:
                    raise Exception("Could not find date input field")
                
                # Clear and fill date field
                await date_input_element.clear()
                await asyncio.sleep(0.5)
                await date_input_element.fill(date_str)
                await asyncio.sleep(0.5)
                
                # Try multiple selectors for submit button
                submit_selectors = [
                    "#edit-submit-air-quality-reports-block-",
                    "input[type='submit']",
                    ".form-submit",
                    "[value*='Apply']",
                    "[value*='Search']"
                ]
                
                submit_button = None
                for selector in submit_selectors:
                    try:
                        submit_element = page.locator(selector)
                        if await submit_element.is_visible():
                            submit_button = submit_element
                            break
                    except:
                        continue
                
                if not submit_button:
                    raise Exception("Could not find submit button")
                
                # Submit the form
                await submit_button.click()
                
                # Wait for results to load
                try:
                    await page.wait_for_load_state("networkidle", timeout=15000)
                except:
                    # Fallback: wait for content
                    await asyncio.sleep(3)
                
                # Take screenshot for debugging if enabled
                if self.config.TAKE_SCREENSHOTS:
                    screenshot_path = self.config.SCREENSHOTS_FOLDER / f"page_{date_str.replace('/', '_')}.png"
                    try:
                        await page.screenshot(path=screenshot_path, full_page=True)
                        self.logger.debug(f"Screenshot saved: {screenshot_path}")
                    except Exception as e:
                        self.logger.warning(f"Screenshot failed: {e}")
                
                self.logger.info(f"Successfully navigated and submitted form for {date_str}")
                return True, "Form submitted successfully"
                
            except Exception as e:
                error_msg = f"Navigation attempt {attempt + 1} failed: {str(e)[:200]}"
                self.logger.warning(error_msg)
                
                if attempt < self.config.NAVIGATION_RETRIES - 1:
                    # Progressive delay with jitter
                    retry_delay = min((2 ** attempt) + random.uniform(0, 2), 15)
                    self.logger.debug(f"Retrying navigation in {retry_delay:.2f}s")
                    await asyncio.sleep(retry_delay)
                    
                    # Reload page before retry
                    try:
                        await page.reload(wait_until="domcontentloaded", timeout=self.config.PAGE_LOAD_TIMEOUT)
                    except:
                        pass
        
        error_msg = f"Failed to navigate after {self.config.NAVIGATION_RETRIES} attempts"
        self.logger.error(error_msg)
        return False, error_msg
    
    async def scrape_pdfs_for_date(self, date_str: str) -> Tuple[List[Dict], str]:
        """Scrape PDFs for a specific date with comprehensive error handling"""
        if not self.browser:
            init_success, init_msg = await self.initialize_browser()
            if not init_success:
                return [], init_msg
        
        page = None
        try:
            # Create new page
            page = await self.context.new_page()
            
            # Set additional page settings
            await page.set_extra_http_headers({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Cache-Control': 'no-cache'
            })
            
            # Navigate and submit form
            nav_success, nav_msg = await self.navigate_and_submit_date_form(page, date_str)
            if not nav_success:
                return [], nav_msg
            
            # Get page content
            html_content = await page.content()
            
            # Extract PDF links
            pdf_links = self.extract_pdf_links_comprehensive(html_content, self.config.BASE_URL)
            
            if pdf_links:
                self.logger.info(f"Found {len(pdf_links)} PDF links for {date_str}")
                return pdf_links, ""
            else:
                self.logger.info(f"No PDF links found for {date_str}")
                return [], "No PDF links found"
            
        except Exception as e:
            error_msg = f"Scraping failed for {date_str}: {str(e)[:200]}"
            self.logger.error(error_msg)
            return [], error_msg
            
        finally:
            if page:
                try:
                    await page.close()
                except:
                    pass


# ========== GITHUB INTEGRATION ==========
class GitHubIntegrationManager:
    """Advanced GitHub integration with batched commits and intelligent conflict resolution"""
    
    def __init__(self, config: ScraperConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.is_configured = bool(config.GITHUB_TOKEN and config.GITHUB_REPO)
        self.git_dir = config.DOWNLOAD_FOLDER
        
        if self.is_configured:
            self.logger.info("GitHub integration enabled")
        else:
            self.logger.info("GitHub integration disabled (no token/repo configured)")
    
    def _run_git_command(self, args: List[str], timeout: int = 30) -> Tuple[bool, str, str]:
        """Execute git command with proper error handling"""
        try:
            result = subprocess.run(
                ['git'] + args,
                cwd=str(self.git_dir),
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False
            )
            
            success = result.returncode == 0
            return success, result.stdout.strip(), result.stderr.strip()
            
        except subprocess.TimeoutExpired:
            return False, "", "Git command timed out"
        except Exception as e:
            return False, "", f"Git command failed: {str(e)}"
    
    def _initialize_git_repository(self) -> bool:
        """Initialize git repository if not exists"""
        try:
            # Check if git repo exists
            if not (self.git_dir / ".git").exists():
                success, stdout, stderr = self._run_git_command(['init'])
                if not success:
                    self.logger.error(f"Git init failed: {stderr}")
                    return False
                self.logger.info("Initialized new git repository")
            
            # Configure git user
            self._run_git_command(['config', 'user.name', self.config.GITHUB_USERNAME])
            self._run_git_command(['config', 'user.email', self.config.GITHUB_EMAIL])
            self._run_git_command(['config', 'core.autocrlf', 'false'])
            self._run_git_command(['config', 'core.filemode', 'false'])
            
            # Set up remote if configured
            if self.config.GITHUB_REPO:
                repo_url = self.config.GITHUB_REPO
                if self.config.GITHUB_TOKEN and not repo_url.startswith('https://'):
                    # Add token to URL
                    repo_parts = repo_url.replace('https://github.com/', '').replace('.git', '')
                    repo_url = f"https://{self.config.GITHUB_TOKEN}@github.com/{repo_parts}.git"
                
                # Remove existing origin if exists
                self._run_git_command(['remote', 'remove', 'origin'])
                
                # Add new origin
                success, stdout, stderr = self._run_git_command(['remote', 'add', 'origin', repo_url])
                if not success:
                    self.logger.error(f"Failed to add remote: {stderr}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Git initialization failed: {e}")
            return False
    
    def _check_repository_status(self) -> Tuple[bool, List[str], List[str]]:
        """Check git repository status and return changed files"""
        try:
            success, stdout, stderr = self._run_git_command(['status', '--porcelain'])
            if not success:
                self.logger.error(f"Git status failed: {stderr}")
                return False, [], []
            
            if not stdout:
                return True, [], []  # No changes
            
            added_files = []
            modified_files = []
            
            for line in stdout.split('\n'):
                if line.strip():
                    status = line[:2]
                    filename = line[3:].strip()
                    
                    if status.startswith('A') or status.startswith('?'):
                        added_files.append(filename)
                    elif status.startswith('M'):
                        modified_files.append(filename)
            
            return True, added_files, modified_files
            
        except Exception as e:
            self.logger.error(f"Git status check failed: {e}")
            return False, [], []
    
    def _create_gitignore(self):
        """Create comprehensive .gitignore file"""
        gitignore_content = """
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Logs
*.log
logs/

# Temporary files
*.tmp
*.temp
.DS_Store
Thumbs.db

# Screenshots (optional - remove if you want to commit them)
screenshots/

# Large files
*.zip
*.rar
*.tar.gz
""".strip()
        
        gitignore_path = self.git_dir / ".gitignore"
        try:
            with open(gitignore_path, 'w', encoding='utf-8') as f:
                f.write(gitignore_content)
            self.logger.debug("Created .gitignore file")
        except Exception as e:
            self.logger.warning(f"Failed to create .gitignore: {e}")
    
    async def commit_and_push_changes(self, new_files_count: int, session_stats: Dict = None) -> Tuple[bool, str]:
        """Commit and push changes with intelligent batching and conflict resolution"""
        if not self.is_configured:
            return True, "GitHub integration not configured"
        
        try:
            # Initialize repository if needed
            if not self._initialize_git_repository():
                return False, "Failed to initialize git repository"
            
            # Create .gitignore if not exists
            if not (self.git_dir / ".gitignore").exists():
                self._create_gitignore()
            
            # Check status
            status_ok, added_files, modified_files = self._check_repository_status()
            if not status_ok:
                return False, "Failed to check repository status"
            
            total_changes = len(added_files) + len(modified_files)
            if total_changes == 0:
                self.logger.info("No changes to commit")
                return True, "No changes to commit"
            
            self.logger.info(f"Preparing to commit {total_changes} files ({len(added_files)} new, {len(modified_files)} modified)")
            
            # Add files to staging
            success, stdout, stderr = self._run_git_command(['add', '.'])
            if not success:
                self.logger.error(f"Git add failed: {stderr}")
                return False, f"Failed to stage files: {stderr}"
            
            # Create comprehensive commit message
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            commit_message_lines = [
                f"EPD AQI Reports Update - {timestamp}",
                "",
                f"📊 Session Summary:",
                f"  • New PDFs Downloaded: {new_files_count}",
                f"  • Files Added: {len(added_files)}",
                f"  • Files Modified: {len(modified_files)}"
            ]
            
            if session_stats:
                commit_message_lines.extend([
                    f"  • Total Processed: {session_stats.get('total_processed', 0)}",
                    f"  • Duplicates Skipped: {session_stats.get('duplicates_skipped', 0)}",
                    f"  • Errors: {session_stats.get('total_errors', 0)}"
                ])
            
            commit_message_lines.extend([
                "",
                f"🤖 Automated commit by EPD PDF Scraper",
                f"📅 Date Range: Last {self.config.CHECK_DAYS} days",
                f"🔗 Source: {self.config.BASE_URL}/aqi"
            ])
            
            commit_message = "\n".join(commit_message_lines)
            
            # Commit changes
            success, stdout, stderr = self._run_git_command(['commit', '-m', commit_message])
            if not success:
                if "nothing to commit" in stderr:
                    self.logger.info("No changes to commit after staging")
                    return True, "No changes to commit"
                else:
                    self.logger.error(f"Git commit failed: {stderr}")
                    return False, f"Failed to commit: {stderr}"
            
            self.logger.info("Successfully created commit")
            
            # Push changes with retry logic
            max_push_retries = 3
            for push_attempt in range(max_push_retries):
                try:
                    success, stdout, stderr = self._run_git_command(['push', 'origin', self.config.GITHUB_BRANCH], timeout=60)
                    
                    if success:
                        self.logger.info(f"Successfully pushed {total_changes} changes to GitHub")
                        return True, f"Pushed {total_changes} changes successfully"
                    else:
                        if "non-fast-forward" in stderr or "rejected" in stderr:
                            # Try to pull and rebase
                            self.logger.warning("Push rejected, attempting to pull and rebase")
                            
                            pull_success, pull_stdout, pull_stderr = self._run_git_command(['pull', 'origin', self.config.GITHUB_BRANCH, '--rebase'], timeout=60)
                            
                            if pull_success:
                                # Try push again after rebase
                                continue
                            else:
                                self.logger.error(f"Pull failed: {pull_stderr}")
                        else:
                            self.logger.error(f"Push attempt {push_attempt + 1} failed: {stderr}")
                        
                        if push_attempt < max_push_retries - 1:
                            await asyncio.sleep(10 * (push_attempt + 1))  # Progressive delay
                
                except Exception as e:
                    self.logger.error(f"Push attempt {push_attempt + 1} exception: {e}")
                    if push_attempt < max_push_retries - 1:
                        await asyncio.sleep(10 * (push_attempt + 1))
            
            return False, f"Failed to push after {max_push_retries} attempts"
            
        except Exception as e:
            error_msg = f"GitHub integration failed: {str(e)[:200]}"
            self.logger.error(error_msg)
            return False, error_msg


# ========== MAIN SCRAPER ORCHESTRATOR ==========
class EPDScraperOrchestrator:
    """Main orchestrator that coordinates all scraping components"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.logger_manager = AdvancedLogger(config)
        self.logger = self.logger_manager.get_logger()
        
        # Initialize components
        self.db_manager = DatabaseManager(config, self.logger)
        self.github_manager = GitHubIntegrationManager(config, self.logger)
        self.pdf_downloader = AdvancedPDFDownloader(config, self.logger)
        self.web_scraper = AdvancedWebScraper(config, self.logger)
        
        # Session tracking
        self.session_id = 0
        self.session_start_time = None
        
        # Graceful shutdown handling
        self._shutdown_event = asyncio.Event()
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            asyncio.create_task(self._shutdown_event.set())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def _generate_date_range(self) -> List[date]:
        """Generate list of dates to process"""
        end_date = date.today()
        start_date = end_date - timedelta(days=self.config.CHECK_DAYS - 1)
        
        date_list = []
        current_date = start_date
        while current_date <= end_date:
            date_list.append(current_date)
            current_date += timedelta(days=1)
        
        self.logger.info(f"Generated date range: {start_date} to {end_date} ({len(date_list)} days)")
        return date_list
    
    async def _process_single_date(self, target_date: date) -> Dict:
        """Process PDFs for a single date with comprehensive error handling"""
        date_str = target_date.strftime(self.config.DATE_FORMAT)
        start_time = time.time()
        
        # Initialize statistics
        date_stats = {
            'date': date_str,
            'pdf_links_found': 0,
            'new_downloads': 0,
            'duplicates_skipped': 0,
            'existing_valid': 0,
            'download_errors': 0,
            'scraping_errors': 0,
            'processing_time': 0.0,
            'downloaded_files': [],
            'status': 'PENDING'
        }
        
        self.logger.info(f"Processing date: {date_str}")
        
        try:
            # Check if we should skip this date (already fully processed recently)
            db_stats = self.db_manager.get_download_statistics()
            
            # Scrape PDFs for this date
            pdf_links, scrape_error = await self.web_scraper.scrape_pdfs_for_date(date_str)
            
            if scrape_error and not pdf_links:
                date_stats['scraping_errors'] = 1
                date_stats['status'] = 'SCRAPING_FAILED'
                self.logger.error(f"Scraping failed for {date_str}: {scrape_error}")
                
                # Log the failure to database
                self.db_manager.log_download_record(
                    date_str, "", "", "SCRAPING_FAILED", 
                    error_message=scrape_error
                )
                
                return date_stats
            
            if not pdf_links:
                date_stats['status'] = 'NO_PDFS_FOUND'
                self.logger.info(f"No PDF links found for {date_str}")
                
                # Log no PDFs found
                self.db_manager.log_download_record(
                    date_str, "", "", "NO_PDFS", 
                    error_message="No PDF links found on page"
                )
                
                return date_stats
            
            date_stats['pdf_links_found'] = len(pdf_links)
            self.logger.info(f"Found {len(pdf_links)} PDF links for {date_str}")
            
            # Filter out already downloaded PDFs
            new_pdf_links = []
            for link in pdf_links:
                if not self.db_manager.is_already_downloaded(date_str, link['filename'], link['url']):
                    new_pdf_links.append(link)
                else:
                    date_stats['duplicates_skipped'] += 1
            
            if not new_pdf_links:
                date_stats['status'] = 'ALL_DUPLICATES'
                self.logger.info(f"All PDFs for {date_str} already downloaded")
                return date_stats
            
            self.logger.info(f"Found {len(new_pdf_links)} new PDFs to download for {date_str}")
            
            # Download new PDFs
            download_results = await self.pdf_downloader.download_pdfs_batch(new_pdf_links, date_str)
            
            # Update statistics
            date_stats['new_downloads'] = download_results['successful_downloads']
            date_stats['existing_valid'] += download_results['existing_valid']
            date_stats['duplicates_skipped'] += download_results['duplicates_skipped']
            date_stats['download_errors'] = download_results['failed_downloads']
            date_stats['downloaded_files'] = download_results['downloaded_files']
            
            # Log all download attempts to database
            for detail in download_results['download_details']:
                self.db_manager.log_download_record(
                    date_str,
                    detail.get('filename', ''),
                    detail.get('url', ''),
                    detail.get('status', 'UNKNOWN'),
                    retries=detail.get('retries', 0),
                    file_hash=detail.get('file_hash', ''),
                    file_size=detail.get('file_size', 0),
                    error_message=detail.get('error_message', ''),
                    processing_time=detail.get('processing_time', 0.0)
                )
            
            # Determine overall status
            if date_stats['new_downloads'] > 0:
                date_stats['status'] = 'SUCCESS'
            elif date_stats['download_errors'] > 0:
                date_stats['status'] = 'PARTIAL_SUCCESS'
            else:
                date_stats['status'] = 'NO_NEW_DOWNLOADS'
            
            self.logger.info(f"Completed processing {date_str}: "
                           f"{date_stats['new_downloads']} new, "
                           f"{date_stats['duplicates_skipped']} duplicates, "
                           f"{date_stats['download_errors']} errors")
        
        except Exception as e:
            date_stats['status'] = 'PROCESSING_ERROR'
            date_stats['scraping_errors'] = 1
            error_msg = f"Unexpected error processing {date_str}: {str(e)[:200]}"
            self.logger.error(error_msg)
            
            # Log the error to database
            self.db_manager.log_download_record(
                date_str, "", "", "PROCESSING_ERROR",
                error_message=error_msg
            )
        
        finally:
            date_stats['processing_time'] = time.time() - start_time
        
        return date_stats
    
    async def run_scraping_session(self) -> Dict:
        """Execute complete scraping session with comprehensive tracking"""
        self.session_start_time = time.time()
        self.session_id = self.db_manager.start_session()
        
        self.logger.info("="*80)
        self.logger.info("STARTING EPD PDF SCRAPING SESSION")
        self.logger.info("="*80)
        self.logger.info(f"Session ID: {self.session_id}")
        self.logger.info(f"Target URL: {self.config.BASE_URL}/aqi")
        self.logger.info(f"Date Range: Last {self.config.CHECK_DAYS} days")
        self.logger.info(f"Download Folder: {self.config.DOWNLOAD_FOLDER}")
        self.logger.info(f"Max Concurrent Downloads: {self.config.MAX_CONCURRENT_DOWNLOADS}")
        
        # Initialize session statistics
        session_stats = {
            'session_id': self.session_id,
            'dates_processed': 0,
            'total_pdf_links_found': 0,
            'new_files_count': 0,
            'duplicates_skipped': 0,
            'existing_valid': 0,
            'total_errors': 0,
            'scraping_errors': 0,
            'download_errors': 0,
            'processing_time': 0.0,
            'downloaded_files': [],
            'date_results': [],
            'github_status': 'Not attempted',
            'github_files_committed': 0
        }
        
        try:
            # Initialize browser
            browser_init_success, browser_init_msg = await self.web_scraper.initialize_browser()
            if not browser_init_success:
                self.logger.error(f"Browser initialization failed: {browser_init_msg}")
                session_stats['total_errors'] += 1
                return session_stats
            
            # Generate date range
            date_list = self._generate_date_range()
            
            # Process each date
            for target_date in date_list:
                # Check for shutdown signal
                if self._shutdown_event.is_set():
                    self.logger.info("Shutdown signal received, stopping session")
                    break
                
                # Process date with error isolation
                try:
                    date_result = await self._process_single_date(target_date)
                    session_stats['date_results'].append(date_result)
                    
                    # Update session statistics
                    session_stats['dates_processed'] += 1
                    session_stats['total_pdf_links_found'] += date_result['pdf_links_found']
                    session_stats['new_files_count'] += date_result['new_downloads']
                    session_stats['duplicates_skipped'] += date_result['duplicates_skipped']
                    session_stats['existing_valid'] += date_result['existing_valid']
                    session_stats['scraping_errors'] += date_result['scraping_errors']
                    session_stats['download_errors'] += date_result['download_errors']
                    session_stats['downloaded_files'].extend(date_result['downloaded_files'])
                    
                except Exception as e:
                    self.logger.error(f"Critical error processing {target_date}: {e}")
                    session_stats['total_errors'] += 1
                    continue
                
                # Small delay between dates to be respectful
                await asyncio.sleep(random.uniform(1.0, 2.0))
            
            session_stats['total_errors'] = session_stats['scraping_errors'] + session_stats['download_errors']
            
        except Exception as e:
            self.logger.error(f"Session execution failed: {e}")
            session_stats['total_errors'] += 1
        
        finally:
            # Cleanup browser resources
            await self.web_scraper.cleanup_browser()
            
            # Calculate total processing time
            session_stats['processing_time'] = time.time() - self.session_start_time
            
            # Update database session record
            self.db_manager.end_session(self.session_id, session_stats)
            
            # Log session summary
            self.logger.info("="*80)
            self.logger.info("SESSION COMPLETED")
            self.logger.info("="*80)
            self.logger.info(f"Dates Processed: {session_stats['dates_processed']}")
            self.logger.info(f"PDF Links Found: {session_stats['total_pdf_links_found']}")
            self.logger.info(f"New Downloads: {session_stats['new_files_count']}")
            self.logger.info(f"Duplicates Skipped: {session_stats['duplicates_skipped']}")
            self.logger.info(f"Total Errors: {session_stats['total_errors']}")
            self.logger.info(f"Processing Time: {session_stats['processing_time']:.2f} seconds")
            self.logger.info("="*80)
        
        return session_stats
    
    async def execute_single_session(self) -> bool:
        """Execute a single scraping session with GitHub integration"""
        try:
            # Run scraping session
            session_stats = await self.run_scraping_session()
            
            # Generate summary report
            summary_report = await ProductionUtils.create_summary_report(session_stats, self.config)
            self.logger.info("Session summary report created")
            
            # GitHub integration if new files were downloaded
            if session_stats['new_files_count'] > 0 or session_stats.get('total_errors', 0) > 0:
                if self.github_manager.is_configured:
                    self.logger.info("Attempting GitHub integration...")
                    
                    github_success, github_msg = await self.github_manager.commit_and_push_changes(
                        session_stats['new_files_count'],
                        session_stats
                    )
                    
                    if github_success:
                        session_stats['github_status'] = 'SUCCESS'
                        session_stats['github_files_committed'] = session_stats['new_files_count']
                        self.logger.info(f"GitHub integration successful: {github_msg}")
                    else:
                        session_stats['github_status'] = f'FAILED: {github_msg}'
                        self.logger.error(f"GitHub integration failed: {github_msg}")
                else:
                    session_stats['github_status'] = 'Not configured'
                    self.logger.info("GitHub integration not configured")
            else:
                session_stats['github_status'] = 'No changes to commit'
                self.logger.info("No new files to commit to GitHub")
            
            # Final summary
            self.logger.info(f"Session completed successfully. Downloaded {session_stats['new_files_count']} new files.")
            return True
            
        except Exception as e:
            self.logger.error(f"Session execution failed: {e}")
            return False


# ========== SCHEDULER ==========
class ProductionScheduler:
    """Advanced scheduler with continuous operation and health monitoring"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.scraper = EPDScraperOrchestrator(config)
        self.logger = self.scraper.logger
        self._running = False
        self._shutdown_event = asyncio.Event()
    
    async def run_continuous_loop(self):
        """Run scraper in continuous loop with intelligent scheduling"""
        self._running = True
        session_count = 0
        consecutive_failures = 0
        max_consecutive_failures = 5
        
        self.logger.info(f"Starting continuous scraper loop (interval: {self.config.LOOP_INTERVAL_HOURS} hours)")
        
        while self._running and not self._shutdown_event.is_set():
            session_count += 1
            session_start = datetime.now()
            
            self.logger.info(f"Starting session #{session_count} at {session_start}")
            
            try:
                # Execute scraping session
                success = await self.scraper.execute_single_session()
                
                if success:
                    consecutive_failures = 0
                    self.logger.info(f"Session #{session_count} completed successfully")
                else:
                    consecutive_failures += 1
                    self.logger.error(f"Session #{session_count} failed (consecutive failures: {consecutive_failures})")
                
                # Check for too many consecutive failures
                if consecutive_failures >= max_consecutive_failures:
                    self.logger.critical(f"Too many consecutive failures ({consecutive_failures}), stopping scheduler")
                    break
            
            except Exception as e:
                consecutive_failures += 1
                self.logger.error(f"Session #{session_count} exception: {e}")
            
            # Calculate next run time
            session_duration = (datetime.now() - session_start).total_seconds()
            sleep_duration = max(
                self.config.LOOP_INTERVAL_HOURS * 3600 - session_duration,
                300  # Minimum 5 minutes between sessions
            )
            
            if consecutive_failures > 0:
                # Add exponential backoff for failures
                failure_delay = min(300 * (2 ** (consecutive_failures - 1)), 3600)  # Max 1 hour
                sleep_duration += failure_delay
                self.logger.info(f"Adding failure delay: {failure_delay} seconds")
            
            next_run = datetime.now() + timedelta(seconds=sleep_duration)
            self.logger.info(f"Next session scheduled for: {next_run} (sleeping {sleep_duration/60:.1f} minutes)")
            
            # Wait with periodic checks for shutdown
            sleep_chunks = int(sleep_duration / 30)  # Check every 30 seconds
            for _ in range(sleep_chunks):
                if self._shutdown_event.is_set():
                    break
                await asyncio.sleep(30)
            
            # Handle remaining sleep time
            remaining_sleep = sleep_duration % 30
            if remaining_sleep > 0 and not self._shutdown_event.is_set():
                await asyncio.sleep(remaining_sleep)
        
        self.logger.info("Continuous loop ended")
        self._running = False
    
    async def run_single_session(self):
        """Run a single scraping session"""
        self.logger.info("Executing single scraping session")
        success = await self.scraper.execute_single_session()
        
        if success:
            self.logger.info("Single session completed successfully")
        else:
            self.logger.error("Single session failed")
        
        return success
    
    def stop(self):
        """Signal scheduler to stop"""
        self.logger.info("Stopping scheduler...")
        self._running = False
        self._shutdown_event.set()


# ========== MAIN ENTRY POINT ==========
def create_argument_parser() -> argparse.ArgumentParser:
    """Create command line argument parser"""
    parser = argparse.ArgumentParser(
        description="Production EPD PDF Scraper - Download AQI reports from Punjab EPD",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scraper.py --once              # Run once and exit
  python scraper.py                     # Run continuously
  python scraper.py --debug             # Run with debug mode
  python scraper.py --days 30           # Check last 30 days
  
Environment Variables:
  GITHUB_TOKEN          # GitHub personal access token
  GITHUB_REPO           # GitHub repository URL
  CHECK_DAYS            # Number of days to check (default: 21)
  LOOP_INTERVAL_HOURS   # Hours between runs (default: 3.0)
  MAX_CONCURRENT_DOWNLOADS  # Max concurrent downloads (default: 6)
        """
    )
    
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run once and exit (default: run continuously)'
    )
    
    parser.add_argument(
        '--days',
        type=int,
        help='Number of days to check (overrides CHECK_DAYS env var)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode with verbose logging'
    )
    
    parser.add_argument(
        '--base-dir',
        type=str,
        help='Base directory for all scraper data (overrides BASE_DIR env var)'
    )
    
    parser.add_argument(
        '--headful',
        action='store_true',
        help='Run browser in headful mode (visible)'
    )
    
    parser.add_argument(
        '--no-github',
        action='store_true',
        help='Disable GitHub integration for this run'
    )
    
    return parser


async def main():
    """Main entry point with comprehensive error handling"""
    # Parse command line arguments
    parser = create_argument_parser()
    args = parser.parse_args()
    
    # Override config with command line arguments
    config_overrides = {}
    
    if args.days:
        config_overrides['CHECK_DAYS'] = args.days
    
    if args.debug:
        config_overrides['LOG_LEVEL'] = 'DEBUG'
        config_overrides['DEBUG_MODE'] = True
        config_overrides['CONSOLE_LOGGING'] = True
    
    if args.base_dir:
        config_overrides['BASE_DIR'] = args.base_dir
    
    if args.headful:
        config_overrides['HEADLESS'] = False
    
    if args.no_github:
        config_overrides['GITHUB_TOKEN'] = ''
        config_overrides['GITHUB_REPO'] = ''
    
    try:
        # Create configuration
        config = ScraperConfig()
        
        # Apply overrides
        for key, value in config_overrides.items():
            setattr(config, key, value)
        
        # Create scheduler
        scheduler = ProductionScheduler(config)
        
        # Print startup information
        print("="*80)
        print("EPD PDF SCRAPER - Production Version 2.0.0")
        print("="*80)
        print(f"Base Directory: {config.BASE_DIR}")
        print(f"Download Folder: {config.DOWNLOAD_FOLDER}")
        print(f"Database: {config.DATABASE_PATH}")
        print(f"Check Days: {config.CHECK_DAYS}")
        print(f"GitHub Integration: {'Enabled' if config.GITHUB_TOKEN and config.GITHUB_REPO else 'Disabled'}")
        print(f"Debug Mode: {'On' if config.DEBUG_MODE else 'Off'}")
        print(f"Headless Browser: {'Yes' if config.HEADLESS else 'No'}")
        print("="*80)
        
        # Run scraper
        if args.once:
            print("Running single session...")
            success = await scheduler.run_single_session()
            exit_code = 0 if success else 1
        else:
            print("Starting continuous operation...")
            print("Press Ctrl+C to stop gracefully")
            await scheduler.run_continuous_loop()
            exit_code = 0
        
    except KeyboardInterrupt:
        print("\nGraceful shutdown initiated...")
        if 'scheduler' in locals():
            scheduler.stop()
        exit_code = 0
    
    except Exception as e:
        print(f"Fatal error: {e}")
        exit_code = 1
    
    print("Scraper stopped.")
    sys.exit(exit_code)


if __name__ == "__main__":
    # Set up proper event loop policy for Windows compatibility
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    # Run main function
    asyncio.run(main())