import asyncio
import logging
import os
import random
import time
import csv
import json
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin, unquote
import re
from typing import List, Dict, Set, Tuple
from dataclasses import dataclass
from contextlib import asynccontextmanager
import aiohttp
from playwright.async_api import async_playwright
from pypdf import PdfReader
import psutil
import schedule



@dataclass
class Config:
    
    CHECK_DAYS: int = 21  
    RUN_INTERVAL_HOURS: int = 1
    BASE_URL: str = "https://epd.punjab.gov.pk"
    DATE_FORMAT: str = "%m/%d/%Y"
    
   
    DOWNLOAD_FOLDER: Path = Path("pdfs")
    ERROR_FOLDER: Path = Path("errors")
    CSV_FILE: Path = Path("downloads.csv")
    SUMMARY_FILE: Path = Path("summary.txt")
    LOG_FILE: Path = Path("scraper.log")
    
    
    MAX_RETRIES: int = 3
    REQUEST_TIMEOUT: int = 60  # seconds
    DELAY_RANGE: Tuple[float, float] = (0.3, 0.8)
    MAX_CONCURRENT: int = 3
    BANDWIDTH_THRESHOLD: float = 15.0  # Mbps
    
    
    HEADLESS: bool = True
    

    GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN", "")
    GITHUB_REPO: str = os.getenv("GITHUB_REPO", "https://github.com/saadaliadalat/Aqi-pdfs.git")
    GITHUB_BRANCH: str = os.getenv("GITHUB_BRANCH", "main")
    
    # Email settings (optional)
    # EMAIL_NOTIFICATIONS: bool = os.getenv("EMAIL_NOTIFICATIONS", "false").lower() == "true"
    # EMAIL_SENDER: str = os.getenv("EMAIL_SENDER", "")
    # EMAIL_RECEIVER: str = os.getenv("EMAIL_RECEIVER", "")
    # SMTP_SERVER: str = os.getenv("SMTP_SERVER", "")
    # SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    # SMTP_USERNAME: str = os.getenv("SMTP_USERNAME", "")
    # SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")

    def __post_init__(self):
        # Create directories
        self.DOWNLOAD_FOLDER.mkdir(parents=True, exist_ok=True)
        self.ERROR_FOLDER.mkdir(parents=True, exist_ok=True)
        
        # Validate GitHub settings
        if not self.GITHUB_TOKEN or not self.GITHUB_REPO:
            logging.warning("GitHub integration disabled: GITHUB_TOKEN or GITHUB_REPO not set")


# ========== LOGGING SETUP ==========
def setup_logging(config: Config):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        handlers=[
            logging.FileHandler(config.LOG_FILE),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


# ========== UTILITIES ==========
class Utils:
    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """Sanitize filename for cross-platform compatibility"""
        invalid_chars = r'[<>:"/\\|?*=&]'
        filename = re.sub(invalid_chars, '_', filename).strip()
        return re.sub(r'\s+', '_', filename)[:200]  # Limit length
    
    @staticmethod
    def get_user_agents() -> List[str]:
        return [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
    
    @staticmethod
    async def check_bandwidth() -> float:
        """Check current network bandwidth usage"""
        try:
            net_io = psutil.net_io_counters()
            bytes_start = net_io.bytes_recv + net_io.bytes_sent
            await asyncio.sleep(1)
            net_io = psutil.net_io_counters()
            bytes_end = net_io.bytes_recv + net_io.bytes_sent
            mbps = ((bytes_end - bytes_start) * 8 / 1_000_000)
            return mbps
        except Exception:
            return float('inf')  # Assume no throttling if check fails


# ========== CSV MANAGER ==========
class CSVManager:
    def __init__(self, config: Config):
        self.config = config
        self.csv_file = config.CSV_FILE
        self.fieldnames = ['date', 'filename', 'url', 'status', 'timestamp']
        self._init_csv()
    
    def _init_csv(self):
        """Initialize CSV file with headers if it doesn't exist"""
        if not self.csv_file.exists():
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()
    
    def get_existing_records(self) -> Set[Tuple[str, str, str]]:
        """Get set of (date, filename, url) tuples from CSV"""
        records = set()
        try:
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('status') == 'SUCCESS':
                        records.add((row.get('date', ''), row.get('filename', ''), row.get('url', '')))
        except FileNotFoundError:
            pass
        except Exception as e:
            logging.error(f"Error reading CSV: {e}")
        return records
    
    def log_record(self, date_str: str, filename: str, url: str, status: str):
        """Append a record to CSV"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            with open(self.csv_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writerow({
                    'date': date_str,
                    'filename': filename,
                    'url': url,
                    'status': status,
                    'timestamp': timestamp
                })
        except Exception as e:
            logging.error(f"Error writing to CSV: {e}")
    
    def get_summary_stats(self) -> Dict:
        """Get summary statistics from CSV"""
        stats = {
            'total': 0,
            'success': 0,
            'errors': 0,
            'no_pdf': 0,
            'today_downloads': 0
        }
        today = date.today().strftime('%Y-%m-%d')
        
        try:
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    stats['total'] += 1
                    status = row.get('status', '')
                    timestamp = row.get('timestamp', '')
                    
                    if status == 'SUCCESS':
                        stats['success'] += 1
                        if timestamp.startswith(today):
                            stats['today_downloads'] += 1
                    elif status == 'ERROR':
                        stats['errors'] += 1
                    elif status == 'NO_PDF':
                        stats['no_pdf'] += 1
        except Exception as e:
            logging.error(f"Error reading CSV for stats: {e}")
        
        return stats


# ========== GITHUB INTEGRATION ==========
class GitHubManager:
    def __init__(self, config: Config):
        self.config = config
        self.token = config.GITHUB_TOKEN
        self.repo = config.GITHUB_REPO
        self.branch = config.GITHUB_BRANCH
    
    def is_configured(self) -> bool:
        return bool(self.token and self.repo)
    
    def setup_git_config(self):
        """Setup git configuration"""
        try:
            subprocess.run(['git', 'config', 'user.name', 'PDF Scraper Bot'], check=True)
            subprocess.run(['git', 'config', 'user.email', 'scraper@epd-monitor.com'], check=True)
            
            # Setup remote with token
            remote_url = f"https://{self.token}@github.com/{self.repo}.git"
            subprocess.run(['git', 'remote', 'set-url', 'origin', remote_url], check=True)
        except subprocess.CalledProcessError as e:
            logging.error(f"Git config failed: {e}")
            return False
        return True
    
    def has_changes(self) -> bool:
        """Check if there are uncommitted changes"""
        try:
            result = subprocess.run(['git', 'status', '--porcelain'], 
                                  capture_output=True, text=True, check=True)
            return bool(result.stdout.strip())
        except subprocess.CalledProcessError:
            return False
    
    def push_changes(self, new_files_count: int) -> bool:
        """Add, commit, and push changes to GitHub"""
        if not self.is_configured():
            logging.warning("GitHub not configured, skipping push")
            return False
        
        if not self.has_changes():
            logging.info("No changes to push")
            return True
        
        try:
            # Setup git config
            if not self.setup_git_config():
                return False
            
            # Add all files
            subprocess.run(['git', 'add', '.'], check=True)
            
            # Commit with meaningful message
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            commit_msg = f"Auto-update: {new_files_count} new PDFs - {timestamp}"
            subprocess.run(['git', 'commit', '-m', commit_msg], check=True)
            
            # Push to remote
            subprocess.run(['git', 'push', 'origin', self.branch], check=True)
            
            logging.info(f"Successfully pushed {new_files_count} new files to GitHub")
            return True
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Git push failed: {e}")
            return False


# ========== PDF SCRAPER ==========
class PDFScraper:
    def __init__(self, config: Config):
        self.config = config
        self.csv_manager = CSVManager(config)
        self.github_manager = GitHubManager(config)
        self.logger = logging.getLogger(__name__)
    
    def find_pdf_links(self, html: str) -> List[Dict[str, str]]:
        """Extract PDF links from HTML content"""
        pdf_links = []
        patterns = [
            r'<a[^>]*href="(/system/files[^"]*\.pdf)"[^>]*>',
            r'<a[^>]*href="([^"]*\.pdf)"[^>]*>'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            for match in matches:
                # Extract filename
                if 'file=' in match:
                    filename = unquote(match.split('file=')[-1])
                else:
                    filename = unquote(match.split('/')[-1])
                
                filename = Utils.sanitize_filename(filename)
                if not filename.endswith('.pdf'):
                    filename += '.pdf'
                
                full_url = urljoin(self.config.BASE_URL, match)
                
                # Avoid duplicates
                if full_url not in [link['url'] for link in pdf_links]:
                    pdf_links.append({
                        'url': full_url,
                        'filename': filename
                    })
        
        return pdf_links[:5]  # Limit to avoid spam
    
    async def download_pdf(self, session: aiohttp.ClientSession, url: str, 
                          filename: str, date_str: str) -> bool:
        """Download a single PDF file"""
        filepath = self.config.DOWNLOAD_FOLDER / filename
        
        # Check if file already exists and is valid
        if filepath.exists() and filepath.stat().st_size > 0:
            try:
                PdfReader(filepath)
                self.logger.info(f"File already exists and valid: {filename}")
                return True
            except Exception:
                filepath.unlink()
                self.logger.warning(f"Removed corrupt existing file: {filename}")
        
        # Download with retries
        for attempt in range(self.config.MAX_RETRIES):
            try:
                # Check bandwidth and throttle if needed
                bandwidth = await Utils.check_bandwidth()
                if bandwidth < self.config.BANDWIDTH_THRESHOLD:
                    self.logger.warning(f"Low bandwidth ({bandwidth:.2f} Mbps), throttling...")
                    await asyncio.sleep(2)
                
                headers = {
                    'User-Agent': random.choice(Utils.get_user_agents()),
                    'Accept': 'application/pdf,*/*;q=0.9',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': f'{self.config.BASE_URL}/aqi',
                    'Accept-Encoding': 'gzip, deflate, br'
                }
                
                timeout = aiohttp.ClientTimeout(total=self.config.REQUEST_TIMEOUT)
                async with session.get(url, headers=headers, timeout=timeout) as response:
                    if response.status == 200:
                        content = await response.read()
                        
                        # Validate content size
                        if len(content) < 1000:  # Suspiciously small PDF
                            raise Exception(f"PDF too small: {len(content)} bytes")
                        
                        # Save file
                        filepath.write_bytes(content)
                        
                        # Validate PDF
                        try:
                            PdfReader(filepath)
                            self.logger.info(f"Downloaded: {filename} ({len(content)} bytes)")
                            return True
                        except Exception as e:
                            filepath.unlink()
                            raise Exception(f"Invalid PDF: {e}")
                    
                    elif response.status == 429:
                        self.logger.warning(f"Rate limited for {filename}, waiting...")
                        await asyncio.sleep(30 + random.uniform(0, 10))
                        continue
                    else:
                        raise Exception(f"HTTP {response.status}: {response.reason}")
            
            except Exception as e:
                self.logger.error(f"Download attempt {attempt+1}/{self.config.MAX_RETRIES} failed for {filename}: {e}")
                if attempt == self.config.MAX_RETRIES - 1:
                    self.csv_manager.log_record(date_str, filename, url, 'ERROR')
                    return False
                
                # Exponential backoff
                await asyncio.sleep(2 ** attempt + random.uniform(0, 1))
        
        return False
    
    async def process_date(self, page, date_str: str, session: aiohttp.ClientSession, 
                          existing_records: Set) -> Tuple[int, int]:
        """Process a single date and return (downloaded, errors)"""
        downloaded = 0
        errors = 0
        
        try:
            # Navigate and fill date
            await page.fill("#edit-field-aqr-date-value", date_str)
            await page.click("#edit-submit-air-quality-reports-block-")
            
            # Wait for results
            try:
                await page.wait_for_selector("a[href*='.pdf'], .no-results", timeout=15000)
            except Exception:
                # Try to find any content
                await asyncio.sleep(3)
            
            # Get page content
            html = await page.content()
            pdf_links = self.find_pdf_links(html)
            
            if not pdf_links:
                self.logger.info(f"No PDFs found for {date_str}")
                self.csv_manager.log_record(date_str, '', '', 'NO_PDF')
                return 0, 0
            
            # Process each PDF
            for link in pdf_links:
                record_key = (date_str, link['filename'], link['url'])
                
                # Skip if already downloaded
                if record_key in existing_records:
                    self.logger.info(f"Skipping existing: {link['filename']} for {date_str}")
                    continue
                
                # Download PDF
                if await self.download_pdf(session, link['url'], link['filename'], date_str):
                    self.csv_manager.log_record(date_str, link['filename'], link['url'], 'SUCCESS')
                    downloaded += 1
                else:
                    errors += 1
                
                # Small delay between downloads
                await asyncio.sleep(random.uniform(*self.config.DELAY_RANGE))
            
            self.logger.info(f"Processed {date_str}: {downloaded} downloaded, {errors} errors")
            return downloaded, errors
        
        except Exception as e:
            self.logger.error(f"Error processing {date_str}: {e}")
            
            # Save error screenshot
            try:
                error_file = self.config.ERROR_FOLDER / f"error_{date_str.replace('/', '_')}_{int(time.time())}.png"
                await page.screenshot(path=error_file)
                self.logger.info(f"Error screenshot saved: {error_file}")
            except Exception:
                pass
            
            self.csv_manager.log_record(date_str, '', '', 'ERROR')
            return 0, 1
    
    async def run_scraping_session(self) -> Dict:
        """Run a complete scraping session"""
        start_time = time.time()
        total_downloaded = 0
        total_errors = 0
        
        # Get date range (last 21 days)
        end_date = date.today()
        start_date = end_date - timedelta(days=self.config.CHECK_DAYS - 1)
        date_list = [start_date + timedelta(days=x) for x in range(self.config.CHECK_DAYS)]
        
        # Get existing records
        existing_records = self.csv_manager.get_existing_records()
        self.logger.info(f"Found {len(existing_records)} existing records in CSV")
        
        # Setup browser
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=self.config.HEADLESS,
                args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
            )
            
            context = await browser.new_context(
                user_agent=random.choice(Utils.get_user_agents()),
                viewport={'width': 1280, 'height': 720},
                bypass_csp=True
            )
            
            # Setup HTTP session
            connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
            timeout = aiohttp.ClientTimeout(total=self.config.REQUEST_TIMEOUT)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                page = await context.new_page()
                
                # Navigate to main page once
                await page.goto(f"{self.config.BASE_URL}/aqi", 
                              wait_until="domcontentloaded", 
                              timeout=30000)
                
                # Process each date
                for current_date in date_list:
                    date_str = current_date.strftime(self.config.DATE_FORMAT)
                    
                    try:
                        downloaded, errors = await self.process_date(
                            page, date_str, session, existing_records
                        )
                        total_downloaded += downloaded
                        total_errors += errors
                        
                    except Exception as e:
                        self.logger.error(f"Unexpected error for {date_str}: {e}")
                        total_errors += 1
                    
                    # Delay between dates
                    await asyncio.sleep(random.uniform(*self.config.DELAY_RANGE))
                
                await page.close()
            await browser.close()
        
        # Generate summary
        duration = time.time() - start_time
        stats = self.csv_manager.get_summary_stats()
        
        summary = {
            'session_downloaded': total_downloaded,
            'session_errors': total_errors,
            'duration_seconds': duration,
            'total_success': stats['success'],
            'total_errors': stats['errors'],
            'total_no_pdf': stats['no_pdf'],
            'today_downloads': stats['today_downloads']
        }
        
        return summary
    
    def generate_summary_report(self, summary: Dict) -> str:
        """Generate human-readable summary report"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        duration_str = f"{summary['duration_seconds']:.1f}s"
        
        report = f"""
EPD Punjab AQI PDF Scraper - Run Summary
{'='*50}
Timestamp: {timestamp}
Duration: {duration_str}

Session Results:
- New PDFs Downloaded: {summary['session_downloaded']}
- Errors: {summary['session_errors']}
- Today's Total Downloads: {summary['today_downloads']}

Overall Statistics:
- Total Successful Downloads: {summary['total_success']}
- Total Errors: {summary['total_errors']}
- Days with No PDFs: {summary['total_no_pdf']}

Status: {'✅ SUCCESS' if summary['session_errors'] == 0 else '⚠️  WITH ERRORS'}
""".strip()
        
        return report
    
    async def run_scheduled_job(self):
        """Run the complete scraping job"""
        self.logger.info("Starting scheduled PDF scraping job...")
        
        try:
            # Run scraping session
            summary = await self.run_scraping_session()
            
            # Generate report
            report = self.generate_summary_report(summary)
            print(report)
            
            # Save summary to file
            with open(self.config.SUMMARY_FILE, 'w') as f:
                f.write(report)
            
            # Push to GitHub if there are new files
            if self.github_manager.is_configured() and summary['session_downloaded'] > 0:
                success = self.github_manager.push_changes(summary['session_downloaded'])
                if success:
                    self.logger.info("Successfully pushed changes to GitHub")
                else:
                    self.logger.error("Failed to push changes to GitHub")
            
            self.logger.info(f"Job completed: {summary['session_downloaded']} new PDFs, {summary['session_errors']} errors")
            
        except Exception as e:
            self.logger.error(f"Job failed with error: {e}")
            error_report = f"SCRAPER ERROR at {datetime.now()}: {e}"
            with open(self.config.SUMMARY_FILE, 'w') as f:
                f.write(error_report)


# ========== MAIN SCHEDULER ==========
class ScraperScheduler:
    def __init__(self):
        self.config = Config()
        self.logger = setup_logging(self.config)
        self.scraper = PDFScraper(self.config)
        
        # Validate configuration
        self._validate_config()
    
    def _validate_config(self):
        """Validate configuration and log warnings"""
        if not self.config.GITHUB_TOKEN:
            self.logger.warning("GITHUB_TOKEN not set - GitHub integration disabled")
        
        if not self.config.GITHUB_REPO:
            self.logger.warning("GITHUB_REPO not set - GitHub integration disabled")
        
        # Test git availability
        try:
            subprocess.run(['git', '--version'], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            self.logger.error("Git not found - GitHub integration will fail")
    
    def run_once(self):
        """Run scraping job once (for testing)"""
        asyncio.run(self.scraper.run_scheduled_job())
    
    def start_scheduler(self):
        """Start the continuous scheduler"""
        self.logger.info(f"Starting PDF scraper scheduler - runs every {self.config.RUN_INTERVAL_HOURS} hours")
        
        # Schedule the job
        schedule.every(2).minutes.do(   # 🔥 run every 2 minutes
    lambda: asyncio.run(self.scraper.run_scheduled_job())
)

        
        # Run once immediately
        self.logger.info("Running initial job...")
        try:
            asyncio.run(self.scraper.run_scheduled_job())
        except Exception as e:
            self.logger.error(f"Initial job failed: {e}")
        
        # Keep running scheduled jobs
        self.logger.info("Entering scheduled mode...")
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                self.logger.info("Scheduler stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Scheduler error: {e}")
                time.sleep(300)  # Wait 5 minutes before retrying


# ========== ENTRY POINT ==========
def main():
    """Main entry point"""
    import sys
    
    scheduler = ScraperScheduler()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--once":
            print("Running scraper once...")
            scheduler.run_once()
            return
        elif sys.argv[1] == "--help":
            print("Usage:")
            print("  python scraper.py           # Run continuous scheduler")
            print("  python scraper.py --once    # Run once and exit")
            print("  python scraper.py --help    # Show this help")
            return
    
    # Default: run continuous scheduler
    scheduler.start_scheduler()


if __name__ == "__main__":
    main()