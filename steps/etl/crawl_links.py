# Import the urlparse function from urllib.parse to extract domain information from URLs
# This allows us to analyze and categorize URLs by their domain names
from urllib.parse import urlparse

# Import logging functionality from loguru, a feature-rich logging library
# This provides advanced logging capabilities beyond Python's standard logging
from loguru import logger
# Import tqdm for progress bars in the console
# This displays visual progress feedback when processing multiple links
from tqdm import tqdm
# Import Annotated from typing_extensions for enhanced type annotations
# This allows adding metadata to return types for better clarity and documentation
from typing_extensions import Annotated
# Import ZenML framework utilities for pipeline step management
# get_step_context allows access to the current pipeline execution context
# step decorator transforms a regular function into a ZenML pipeline step
from zenml import get_step_context, step

# Import the custom crawler dispatcher from the application layer
# This class manages different types of web crawlers for various platforms
from llm_engineering.application.crawlers.dispatcher import CrawlerDispatcher
# Import the UserDocument domain model
# This represents a user entity in the system's domain model
from llm_engineering.domain.documents import UserDocument


# Apply the @step decorator to mark this function as a ZenML pipeline step
# This enables ZenML to track inputs, outputs, and execution metadata
@step
def crawl_links(user: UserDocument, links: list[str]) -> Annotated[list[str], "crawled_links"]:
    """
    Pipeline step to crawl multiple web links and associate the extracted content with a user.
    
    Args:
        user: The UserDocument representing the user who owns these links
        links: A list of URLs to crawl
        
    Returns:
        list[str]: The list of processed links, annotated with the label "crawled_links"
    """
    # Initialize a crawler dispatcher with registered crawlers for different platforms
    # The build() method creates a new dispatcher instance
    # Each register_* method adds a specific crawler implementation to the dispatcher
    dispatcher = CrawlerDispatcher.build().register_linkedin().register_medium().register_github()

    # Log the start of the crawling process with the number of links to process
    # This helps with monitoring and debugging the pipeline execution
    logger.info(f"Starting to crawl {len(links)} link(s).")

    # Initialize an empty dictionary to collect metadata about the crawling process
    # This will store statistics grouped by domain (e.g., success/failure counts)
    metadata = {}
    
    # Initialize a counter for successful crawls to track overall success rate
    successfull_crawls = 0
    
    # Loop through each link with a progress bar provided by tqdm
    # tqdm wraps the links list to display a visual progress indicator in the console
    for link in tqdm(links):
        # Attempt to crawl each link and get the success status and domain
        # _crawl_link is a helper function defined below that handles individual crawls
        successfull_crawl, crawled_domain = _crawl_link(dispatcher, link, user)
        
        # Increment the success counter if this link was successfully crawled
        successfull_crawls += successfull_crawl

        # Update the metadata dictionary with information about this crawl attempt
        # _add_to_metadata is a helper function that organizes the statistics by domain
        metadata = _add_to_metadata(metadata, crawled_domain, successfull_crawl)

    # Get the current ZenML step execution context
    # This provides access to the pipeline's runtime environment
    step_context = get_step_context()
    
    # Add the collected metadata to the ZenML step's output
    # This enriches the pipeline with domain-grouped statistics about the crawling process
    step_context.add_output_metadata(output_name="crawled_links", metadata=metadata)

    # Log the final summary of the crawling process
    # Shows how many links were successfully processed out of the total
    logger.info(f"Successfully crawled {successfull_crawls} / {len(links)} links.")

    # Return the original list of links
    # This allows downstream steps to access the same list if needed
    return links


# Helper function to crawl a single link using the appropriate crawler
def _crawl_link(dispatcher: CrawlerDispatcher, link: str, user: UserDocument) -> tuple[bool, str]:
    """
    Process a single link using the appropriate crawler from the dispatcher.
    
    Args:
        dispatcher: The CrawlerDispatcher instance with registered crawlers
        link: The URL to crawl
        user: The UserDocument representing the owner of the link
        
    Returns:
        tuple: A tuple containing (success_status, domain_name)
    """
    # Get the appropriate crawler for this link from the dispatcher
    # The dispatcher selects a crawler based on the link's domain or format
    crawler = dispatcher.get_crawler(link)
    
    # Extract the domain from the URL using urlparse
    # This helps categorize the link by its source (e.g., linkedin.com, medium.com)
    crawler_domain = urlparse(link).netloc

    try:
        # Attempt to extract content from the link using the selected crawler
        # The extracted content is automatically associated with the user
        crawler.extract(link=link, user=user)

        # Return success status (True) and the domain if extraction succeeded
        return (True, crawler_domain)
    except Exception as e:
        # Log any errors that occur during the crawling process
        # The !s format specifier converts the exception to a string representation
        logger.error(f"An error occurred while crawling: {e!s}")

        # Return failure status (False) and the domain if extraction failed
        return (False, crawler_domain)


# Helper function to update metadata with information about each crawl attempt
def _add_to_metadata(metadata: dict, domain: str, successfull_crawl: bool) -> dict:
    """
    Update the metadata dictionary with statistics for a domain.
    
    Args:
        metadata: The existing metadata dictionary to update
        domain: The domain name of the crawled URL
        successfull_crawl: Boolean indicating if the crawl was successful
        
    Returns:
        dict: The updated metadata dictionary
    """
    # If this domain is not yet in the metadata dictionary, initialize it
    if domain not in metadata:
        metadata[domain] = {}
    
    # Increment the successful crawl count for this domain if applicable
    # The get() method with default values handles the case where keys don't exist yet
    metadata[domain]["successful"] = metadata.get(domain, {}).get("successful", 0) + successfull_crawl
    
    # Increment the total crawl attempt count for this domain
    # This counts both successful and failed attempts
    metadata[domain]["total"] = metadata.get(domain, {}).get("total", 0) + 1

    # Return the updated metadata dictionary
    return metadata
