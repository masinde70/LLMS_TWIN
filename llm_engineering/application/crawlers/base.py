# Import the time module to handle delays and pauses in the crawler
import time
# Import abstract base class and abstractmethod decorator for defining interfaces
from abc import ABC, abstractmethod
# Import mkdtemp for creating temporary directories securely
from tempfile import mkdtemp

# Import the chromedriver_autoinstaller to automatically manage ChromeDriver versions
import chromedriver_autoinstaller
# Import the main Selenium webdriver module
from selenium import webdriver
# Import Options class to configure Chrome browser settings
from selenium.webdriver.chrome.options import Options

# Import the base document class from the domain layer
# This is used for storing crawled data in a NoSQL database
from llm_engineering.domain.documents import NoSQLBaseDocument

# Check if the current version of chromedriver exists
# and if it doesn't exist, download it automatically,
# then add chromedriver to path
chromedriver_autoinstaller.install()


class BaseCrawler(ABC):
    """
    Abstract base class for all crawlers in the system.
    
    This defines the common interface that all crawler implementations must follow.
    The model class attribute specifies which document model should store the crawled data.
    """
    # Class attribute to define which document model will store the crawled data
    model: type[NoSQLBaseDocument]

    @abstractmethod
    def extract(self, link: str, **kwargs) -> None:
        """
        Abstract method that must be implemented by all crawlers.
        
        Args:
            link: The URL to extract data from
            **kwargs: Additional parameters specific to each crawler implementation
            
        Returns:
            None: Extracted data is typically saved to the database
        """
        # The ellipsis (...) indicates this is an abstract method that subclasses must implement
        ...


class BaseSeleniumCrawler(BaseCrawler, ABC):
    """
    Base class for crawlers that use Selenium for web automation.
    
    This class extends BaseCrawler and provides common functionality for
    Selenium-based web crawling, including browser configuration and page scrolling.
    """
    def __init__(self, scroll_limit: int = 5) -> None:
        """
        Initialize a Selenium-based crawler with Chrome WebDriver.
        
        Args:
            scroll_limit: Maximum number of times to scroll the page (default: 5)
        """
        # Create Chrome options object to configure the browser
        options = webdriver.ChromeOptions()

        # Set various Chrome options to optimize for headless crawling:
        
        # Disable the sandbox for better performance (especially in containerized environments)
        options.add_argument("--no-sandbox")
        # Run in headless mode (without UI) using the new headless implementation
        options.add_argument("--headless=new")
        # Avoid using /dev/shm which can cause issues in containerized environments
        options.add_argument("--disable-dev-shm-usage")
        # Set logging level to minimal (3)
        options.add_argument("--log-level=3")
        # Block pop-up windows that might interfere with crawling
        options.add_argument("--disable-popup-blocking")
        # Disable browser notifications
        options.add_argument("--disable-notifications")
        # Disable Chrome extensions which aren't needed for crawling
        options.add_argument("--disable-extensions")
        # Disable background network activity to improve performance
        options.add_argument("--disable-background-networking")
        # Ignore SSL/TLS certificate errors to allow crawling sites with invalid certificates
        options.add_argument("--ignore-certificate-errors")
        
        # Use temporary directories for Chrome user data, cache, and profile
        # mkdtemp() creates secure temporary directories that are automatically cleaned up
        options.add_argument(f"--user-data-dir={mkdtemp()}")
        options.add_argument(f"--data-path={mkdtemp()}")
        options.add_argument(f"--disk-cache-dir={mkdtemp()}")
        
        # Enable remote debugging on port 9226 (useful for troubleshooting)
        options.add_argument("--remote-debugging-port=9226")

        # Allow subclasses to add their own specific Chrome options
        self.set_extra_driver_options(options)

        # Store the scroll limit as an instance attribute
        self.scroll_limit = scroll_limit
        
        # Initialize the Chrome WebDriver with the configured options
        self.driver = webdriver.Chrome(
            options=options,
        )

    def set_extra_driver_options(self, options: Options) -> None:
        """
        Hook method for subclasses to add additional Chrome options.
        
        Args:
            options: The ChromeOptions object to modify
            
        Returns:
            None: The options object is modified in-place
        """
        # Base implementation does nothing; subclasses can override this
        pass

    def login(self) -> None:
        """
        Hook method for subclasses to implement login functionality.
        
        This is useful for sites that require authentication before crawling.
        
        Returns:
            None
        """
        # Base implementation does nothing; subclasses can override this
        pass

    def scroll_page(self) -> None:
        """
        Scroll through the page to load dynamically generated content.
        
        This method scrolls to the bottom of the page repeatedly until either:
        1. The page height doesn't change (no more content to load)
        2. The scroll_limit is reached
        
        Between scrolls, it waits 5 seconds to allow dynamic content to load.
        """
        # Initialize a counter for the number of scrolls performed
        current_scroll = 0
        
        # Get the initial scroll height of the page using JavaScript
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        
        # Continue scrolling until we reach the bottom or the scroll limit
        while True:
            # Scroll to the bottom of the page using JavaScript
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # Wait for dynamic content to load
            time.sleep(5)
            
            # Get the new scroll height after loading more content
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            
            # Break the loop if:
            # 1. The page height didn't change (reached the bottom)
            # 2. We've reached the scroll limit (if one is set)
            if new_height == last_height or (self.scroll_limit and current_scroll >= self.scroll_limit):
                break
                
            # Update the last height for the next iteration
            last_height = new_height
            
            # Increment the scroll counter
            current_scroll += 1
