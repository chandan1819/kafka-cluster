"""
Retry utilities for handling transient failures.

This module provides decorators and utilities for implementing retry logic
with exponential backoff, jitter, and configurable retry conditions.
"""

import asyncio
import logging
import random
import time
from functools import wraps
from typing import Any, Callable, Optional, Tuple, Type, Union, List
from dataclasses import dataclass

from ..exceptions import LocalKafkaManagerError, TimeoutError


logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    
    max_attempts: int = 3
    base_delay: float = 1.0  # Base delay in seconds
    max_delay: float = 60.0  # Maximum delay in seconds
    exponential_base: float = 2.0  # Exponential backoff multiplier
    jitter: bool = True  # Add random jitter to delays
    timeout: Optional[float] = None  # Overall timeout in seconds
    
    # Exception types that should trigger retries
    retryable_exceptions: Tuple[Type[Exception], ...] = (
        ConnectionError,
        TimeoutError,
        OSError,
    )
    
    # Exception types that should NOT trigger retries
    non_retryable_exceptions: Tuple[Type[Exception], ...] = (
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
    )


class RetryExhaustedError(LocalKafkaManagerError):
    """Raised when all retry attempts have been exhausted."""
    
    def __init__(self, operation: str, attempts: int, last_exception: Exception):
        super().__init__(
            message=f"Retry exhausted for operation '{operation}' after {attempts} attempts",
            details={
                "operation": operation,
                "attempts": attempts,
                "last_exception": str(last_exception),
                "last_exception_type": type(last_exception).__name__
            },
            cause=last_exception
        )


def calculate_delay(attempt: int, config: RetryConfig) -> float:
    """Calculate the delay for a given retry attempt.
    
    Args:
        attempt: The current attempt number (0-based)
        config: Retry configuration
        
    Returns:
        Delay in seconds
    """
    # Exponential backoff
    delay = config.base_delay * (config.exponential_base ** attempt)
    
    # Cap at max delay
    delay = min(delay, config.max_delay)
    
    # Add jitter if enabled
    if config.jitter:
        # Add up to 25% jitter
        jitter_amount = delay * 0.25
        delay += random.uniform(-jitter_amount, jitter_amount)
        delay = max(0, delay)  # Ensure non-negative
    
    return delay


def should_retry(exception: Exception, config: RetryConfig) -> bool:
    """Determine if an exception should trigger a retry.
    
    Args:
        exception: The exception that occurred
        config: Retry configuration
        
    Returns:
        True if the exception should trigger a retry
    """
    # Check non-retryable exceptions first
    if isinstance(exception, config.non_retryable_exceptions):
        return False
    
    # Check retryable exceptions
    if isinstance(exception, config.retryable_exceptions):
        return True
    
    # For LocalKafkaManagerError, check if it's a service availability issue
    if isinstance(exception, LocalKafkaManagerError):
        from ..exceptions import (
            ServiceUnavailableError, 
            DockerNotAvailableError,
            KafkaNotAvailableError,
            KafkaRestProxyNotAvailableError,
            TimeoutError as LocalTimeoutError
        )
        
        retryable_local_exceptions = (
            ServiceUnavailableError,
            DockerNotAvailableError,
            KafkaNotAvailableError,
            KafkaRestProxyNotAvailableError,
            LocalTimeoutError
        )
        
        return isinstance(exception, retryable_local_exceptions)
    
    # Default to not retrying unknown exceptions
    return False


def retry_sync(config: Optional[RetryConfig] = None):
    """Decorator for adding retry logic to synchronous functions.
    
    Args:
        config: Retry configuration. If None, uses default config.
        
    Returns:
        Decorated function with retry logic
    """
    if config is None:
        config = RetryConfig()
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            operation_name = f"{func.__module__}.{func.__name__}"
            start_time = time.time()
            last_exception = None
            
            for attempt in range(config.max_attempts):
                try:
                    # Check overall timeout
                    if config.timeout and (time.time() - start_time) > config.timeout:
                        raise TimeoutError(
                            operation=operation_name,
                            timeout_seconds=config.timeout
                        )
                    
                    # Execute the function
                    return func(*args, **kwargs)
                    
                except Exception as e:
                    last_exception = e
                    
                    # Check if we should retry
                    if not should_retry(e, config):
                        logger.debug(f"Not retrying {operation_name} due to non-retryable exception: {e}")
                        raise
                    
                    # Check if this is the last attempt
                    if attempt == config.max_attempts - 1:
                        logger.warning(f"Retry exhausted for {operation_name} after {attempt + 1} attempts")
                        break
                    
                    # Calculate delay and wait
                    delay = calculate_delay(attempt, config)
                    logger.info(f"Retrying {operation_name} in {delay:.2f}s (attempt {attempt + 1}/{config.max_attempts})")
                    time.sleep(delay)
            
            # All retries exhausted
            raise RetryExhaustedError(operation_name, config.max_attempts, last_exception)
        
        return wrapper
    return decorator


def retry_async(config: Optional[RetryConfig] = None):
    """Decorator for adding retry logic to asynchronous functions.
    
    Args:
        config: Retry configuration. If None, uses default config.
        
    Returns:
        Decorated async function with retry logic
    """
    if config is None:
        config = RetryConfig()
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            operation_name = f"{func.__module__}.{func.__name__}"
            start_time = time.time()
            last_exception = None
            
            for attempt in range(config.max_attempts):
                try:
                    # Check overall timeout
                    if config.timeout and (time.time() - start_time) > config.timeout:
                        raise TimeoutError(
                            operation=operation_name,
                            timeout_seconds=config.timeout
                        )
                    
                    # Execute the function
                    return await func(*args, **kwargs)
                    
                except Exception as e:
                    last_exception = e
                    
                    # Check if we should retry
                    if not should_retry(e, config):
                        logger.debug(f"Not retrying {operation_name} due to non-retryable exception: {e}")
                        raise
                    
                    # Check if this is the last attempt
                    if attempt == config.max_attempts - 1:
                        logger.warning(f"Retry exhausted for {operation_name} after {attempt + 1} attempts")
                        break
                    
                    # Calculate delay and wait
                    delay = calculate_delay(attempt, config)
                    logger.info(f"Retrying {operation_name} in {delay:.2f}s (attempt {attempt + 1}/{config.max_attempts})")
                    await asyncio.sleep(delay)
            
            # All retries exhausted
            raise RetryExhaustedError(operation_name, config.max_attempts, last_exception)
        
        return wrapper
    return decorator


class RetryContext:
    """Context manager for retry operations with custom logic."""
    
    def __init__(self, operation_name: str, config: Optional[RetryConfig] = None):
        self.operation_name = operation_name
        self.config = config or RetryConfig()
        self.attempt = 0
        self.start_time = time.time()
        self.last_exception: Optional[Exception] = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            return False  # No exception, continue normally
        
        self.last_exception = exc_val
        
        # Check if we should retry
        if not should_retry(exc_val, self.config):
            return False  # Don't suppress exception
        
        # Check if we've exhausted retries
        if self.attempt >= self.config.max_attempts - 1:
            # Replace with retry exhausted error
            raise RetryExhaustedError(self.operation_name, self.config.max_attempts, exc_val)
        
        # Check overall timeout
        if self.config.timeout and (time.time() - self.start_time) > self.config.timeout:
            raise TimeoutError(
                operation=self.operation_name,
                timeout_seconds=self.config.timeout
            )
        
        # Calculate delay and wait
        delay = calculate_delay(self.attempt, self.config)
        logger.info(f"Retrying {self.operation_name} in {delay:.2f}s (attempt {self.attempt + 1}/{self.config.max_attempts})")
        time.sleep(delay)
        
        self.attempt += 1
        return True  # Suppress the exception to retry


async def retry_async_operation(
    operation: Callable,
    operation_name: str,
    config: Optional[RetryConfig] = None,
    *args,
    **kwargs
) -> Any:
    """Retry an async operation with the given configuration.
    
    Args:
        operation: The async function to retry
        operation_name: Name of the operation for logging
        config: Retry configuration
        *args: Arguments to pass to the operation
        **kwargs: Keyword arguments to pass to the operation
        
    Returns:
        Result of the operation
        
    Raises:
        RetryExhaustedError: If all retry attempts are exhausted
    """
    if config is None:
        config = RetryConfig()
    
    start_time = time.time()
    last_exception = None
    
    for attempt in range(config.max_attempts):
        try:
            # Check overall timeout
            if config.timeout and (time.time() - start_time) > config.timeout:
                raise TimeoutError(
                    operation=operation_name,
                    timeout_seconds=config.timeout
                )
            
            # Execute the operation
            return await operation(*args, **kwargs)
            
        except Exception as e:
            last_exception = e
            
            # Check if we should retry
            if not should_retry(e, config):
                logger.debug(f"Not retrying {operation_name} due to non-retryable exception: {e}")
                raise
            
            # Check if this is the last attempt
            if attempt == config.max_attempts - 1:
                logger.warning(f"Retry exhausted for {operation_name} after {attempt + 1} attempts")
                break
            
            # Calculate delay and wait
            delay = calculate_delay(attempt, config)
            logger.info(f"Retrying {operation_name} in {delay:.2f}s (attempt {attempt + 1}/{config.max_attempts})")
            await asyncio.sleep(delay)
    
    # All retries exhausted
    raise RetryExhaustedError(operation_name, config.max_attempts, last_exception)


# Predefined retry configurations for common scenarios

# Quick retries for fast operations
QUICK_RETRY = RetryConfig(
    max_attempts=3,
    base_delay=0.5,
    max_delay=5.0,
    timeout=15.0
)

# Standard retries for normal operations
STANDARD_RETRY = RetryConfig(
    max_attempts=3,
    base_delay=1.0,
    max_delay=30.0,
    timeout=60.0
)

# Patient retries for slow operations like cluster startup
PATIENT_RETRY = RetryConfig(
    max_attempts=5,
    base_delay=2.0,
    max_delay=60.0,
    timeout=300.0  # 5 minutes
)

# Network retries for external service calls
NETWORK_RETRY = RetryConfig(
    max_attempts=4,
    base_delay=1.0,
    max_delay=30.0,
    timeout=120.0,
    retryable_exceptions=(
        ConnectionError,
        TimeoutError,
        OSError,
        # Add HTTP-specific exceptions if using httpx
    )
)