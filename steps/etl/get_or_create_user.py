# Import logging functionality from loguru, which is a feature-rich logging library
# This allows for structured and customizable logging
from loguru import logger
# Import Annotated from typing_extensions for output type annotation
# Annotated allows adding metadata to return types for better documentation
from typing_extensions import Annotated
# Import ZenML decorators and utilities
# get_step_context allows access to the current pipeline execution context
# step decorator marks this function as a pipeline step in the ZenML framework
from zenml import get_step_context, step

# Import utility functions from the application layer
# These provide helper functionality for working with user data
from llm_engineering.application import utils
# Import the UserDocument domain model
# This is the core data structure representing a user in the system
from llm_engineering.domain.documents import UserDocument


# Apply the @step decorator to mark this function as a ZenML pipeline step
# This allows ZenML to track inputs, outputs, and metadata for this function
@step
def get_or_create_user(user_full_name: str) -> Annotated[UserDocument, "user"]:
    """
    Pipeline step to get an existing user or create a new one if not found.
    
    Args:
        user_full_name: The full name of the user (first and last name)
        
    Returns:
        UserDocument: A user document object annotated with the label "user"
    """
    # Log the operation with user's full name for debugging and monitoring
    logger.info(f"Getting or creating user: {user_full_name}")

    # Split the user's full name into first and last name components
    # using a utility function from the application layer
    first_name, last_name = utils.split_user_full_name(user_full_name)

    # Attempt to find an existing user with the given name, or create a new one if not found
    # This is a common "get or create" pattern to prevent duplicates in the database
    user = UserDocument.get_or_create(first_name=first_name, last_name=last_name)

    # Get the current ZenML step execution context
    # This provides access to the pipeline's runtime environment and metadata
    step_context = get_step_context()
    
    # Add metadata about the user to the step's outputs
    # This enriches the pipeline's observability and tracking capabilities
    # The metadata contains both the input query and the retrieved/created user information
    step_context.add_output_metadata(output_name="user", metadata=_get_metadata(user_full_name, user))

    # Return the user document object to be passed to the next step in the pipeline
    return user


# Private helper function to generate metadata about the user operation
# The underscore prefix indicates this is an internal function not meant to be called externally
def _get_metadata(user_full_name: str, user: UserDocument) -> dict:
    """
    Create a structured metadata dictionary for the user document.
    
    Args:
        user_full_name: The full name used in the query
        user: The UserDocument instance that was retrieved or created
        
    Returns:
        dict: A nested dictionary containing query parameters and user data
    """
    # Return a structured dictionary with two main sections:
    # 1. "query": The original parameters used to find/create the user
    # 2. "retrieved": The actual user data that was found or created
    return {
        "query": {
            # Store the original full name that was provided to the function
            "user_full_name": user_full_name,
        },
        "retrieved": {
            # Store the MongoDB ObjectID as a string for serialization purposes
            "user_id": str(user.id),
            # Store the user's first name from the document
            "first_name": user.first_name,
            # Store the user's last name from the document
            "last_name": user.last_name,
        },
    }
