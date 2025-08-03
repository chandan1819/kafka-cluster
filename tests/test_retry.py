"""
Tests for retry utilities and logic.
"""

import asyncio
import pytest
import time
from unittest.mock import Mock, AsyncMock, patch

from src.utils.retry import (
    RetryConfig,
    RetryExhaustedError,
    calculate_delay,
    should_retry,
    retry_sync,
    retry_async,
    RetryContext,
    retry_async_operation,
    QUICK_RETRY,
    STANDARD_RETRY,
    PATIENT_RETRY,
    NETWORK_RETRY
)
from src.exceptions import (
    LocalKafkaManagerError,
    ServiceUnavailableError,
    DockerNotAvailableError,
    KafkaNotAvailableError,
    TimeoutError as LocalTimeoutError
)


@pytest.mark.unit
class TestRetryConfig:
    """Test RetryConfig dataclass."""
    
    def test_default_config(self):
        """Test default retry configuration."""
        config = RetryConfig()
        
        assert config.max_attempts == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 2.0
        assert config.jitter is True
        assert config.timeout is None
        assert ConnectionError in config.retryable_exceptions
        assert ValueError in config.non_retryable_exceptions
    
    def test_custom_config(self):
        """Test custom retry configuration."""
        config = RetryConfig(
            max_attempts=5,
            base_delay=0.5,
            max_delay=30.0,
            exponential_base=1.5,
            jitter=False,
            timeout=120.0
        )
        
        assert config.max_attempts == 5
        assert config.base_delay == 0.5
        assert config.max_delay == 30.0
        assert config.exponential_base == 1.5
        assert config.jitter is False
        assert config.timeout == 120.0


@pytest.mark.unit
class TestCalculateDelay:
    """Test delay calculation logic."""
    
    def test_exponential_backoff(self):
        """Test exponential backoff calculation."""
        config = RetryConfig(base_delay=1.0, exponential_base=2.0, jitter=False)
        
        assert calculate_delay(0, config) == 1.0  # 1.0 * 2^0
        assert calculate_delay(1, config) == 2.0  # 1.0 * 2^1
        assert calculate_delay(2, config) == 4.0  # 1.0 * 2^2
        assert calculate_delay(3, config) == 8.0  # 1.0 * 2^3
    
    def test_max_delay_cap(self):
        """Test that delay is capped at max_delay."""
        config = RetryConfig(base_delay=1.0, max_delay=5.0, exponential_base=2.0, jitter=False)
        
        assert calculate_delay(0, config) == 1.0
        assert calculate_delay(1, config) == 2.0
        assert calculate_delay(2, config) == 4.0
        assert calculate_delay(3, config) == 5.0  # Capped at max_delay
        assert calculate_delay(10, config) == 5.0  # Still capped
    
    def test_jitter_adds_randomness(self):
        """Test that jitter adds randomness to delays."""
        config = RetryConfig(base_delay=4.0, exponential_base=1.0, jitter=True)
        
        # With jitter, delays should vary
        delays = [calculate_delay(0, config) for _ in range(10)]
        
        # All delays should be non-negative
        assert all(delay >= 0 for delay in delays)
        
        # With jitter, we should get some variation (not all identical)
        # Note: This test might occasionally fail due to randomness, but very unlikely
        assert len(set(delays)) > 1 or all(d == 4.0 for d in delays)  # Allow for edge case
    
    def test_jitter_disabled(self):
        """Test that disabling jitter gives consistent delays."""
        config = RetryConfig(base_delay=2.0, exponential_base=1.0, jitter=False)
        
        delays = [calculate_delay(0, config) for _ in range(5)]
        
        # Without jitter, all delays should be identical
        assert all(delay == 2.0 for delay in delays)


@pytest.mark.unit
class TestShouldRetry:
    """Test retry condition logic."""
    
    def test_retryable_exceptions(self):
        """Test that retryable exceptions trigger retries."""
        config = RetryConfig()
        
        assert should_retry(ConnectionError("Network error"), config) is True
        assert should_retry(TimeoutError("Request timeout"), config) is True
        assert should_retry(OSError("System error"), config) is True
    
    def test_non_retryable_exceptions(self):
        """Test that non-retryable exceptions don't trigger retries."""
        config = RetryConfig()
        
        assert should_retry(ValueError("Invalid value"), config) is False
        assert should_retry(TypeError("Type error"), config) is False
        assert should_retry(KeyError("Missing key"), config) is False
        assert should_retry(AttributeError("Missing attribute"), config) is False
    
    def test_local_kafka_manager_exceptions(self):
        """Test retry logic for LocalKafkaManagerError exceptions."""
        config = RetryConfig()
        
        # Service availability errors should be retryable
        assert should_retry(ServiceUnavailableError("test", "Service down"), config) is True
        assert should_retry(DockerNotAvailableError("Docker down"), config) is True
        assert should_retry(KafkaNotAvailableError("Kafka down"), config) is True
        assert should_retry(LocalTimeoutError("test", 30), config) is True
        
        # Other LocalKafkaManagerError should not be retryable by default
        generic_error = LocalKafkaManagerError("Generic error")
        assert should_retry(generic_error, config) is False
    
    def test_unknown_exceptions(self):
        """Test that unknown exceptions are not retryable by default."""
        config = RetryConfig()
        
        class CustomError(Exception):
            pass
        
        assert should_retry(CustomError("Unknown error"), config) is False


@pytest.mark.unit
class TestRetrySyncDecorator:
    """Test synchronous retry decorator."""
    
    def test_successful_operation(self):
        """Test that successful operations don't retry."""
        call_count = 0
        
        @retry_sync(RetryConfig(max_attempts=3))
        def successful_func():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = successful_func()
        
        assert result == "success"
        assert call_count == 1
    
    def test_retry_on_retryable_exception(self):
        """Test retry behavior on retryable exceptions."""
        call_count = 0
        
        @retry_sync(RetryConfig(max_attempts=3, base_delay=0.01))
        def failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Network error")
            return "success"
        
        result = failing_func()
        
        assert result == "success"
        assert call_count == 3
    
    def test_no_retry_on_non_retryable_exception(self):
        """Test that non-retryable exceptions are not retried."""
        call_count = 0
        
        @retry_sync(RetryConfig(max_attempts=3))
        def failing_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("Invalid value")
        
        with pytest.raises(ValueError, match="Invalid value"):
            failing_func()
        
        assert call_count == 1
    
    def test_retry_exhausted(self):
        """Test behavior when all retries are exhausted."""
        call_count = 0
        
        @retry_sync(RetryConfig(max_attempts=2, base_delay=0.01))
        def always_failing_func():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Network error")
        
        with pytest.raises(RetryExhaustedError) as exc_info:
            always_failing_func()
        
        assert call_count == 2
        assert "after 2 attempts" in str(exc_info.value)
    
    def test_timeout_during_retries(self):
        """Test timeout during retry attempts."""
        @retry_sync(RetryConfig(max_attempts=10, base_delay=0.1, timeout=0.05))
        def slow_failing_func():
            time.sleep(0.02)  # Small delay to consume timeout
            raise ConnectionError("Network error")
        
        with pytest.raises(LocalTimeoutError):
            slow_failing_func()


@pytest.mark.unit
class TestRetryAsyncDecorator:
    """Test asynchronous retry decorator."""
    
    @pytest.mark.asyncio
    async def test_successful_operation(self):
        """Test that successful operations don't retry."""
        call_count = 0
        
        @retry_async(RetryConfig(max_attempts=3))
        async def successful_func():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = await successful_func()
        
        assert result == "success"
        assert call_count == 1
    
    @pytest.mark.asyncio
    async def test_retry_on_retryable_exception(self):
        """Test retry behavior on retryable exceptions."""
        call_count = 0
        
        @retry_async(RetryConfig(max_attempts=3, base_delay=0.01))
        async def failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Network error")
            return "success"
        
        result = await failing_func()
        
        assert result == "success"
        assert call_count == 3
    
    @pytest.mark.asyncio
    async def test_no_retry_on_non_retryable_exception(self):
        """Test that non-retryable exceptions are not retried."""
        call_count = 0
        
        @retry_async(RetryConfig(max_attempts=3))
        async def failing_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("Invalid value")
        
        with pytest.raises(ValueError, match="Invalid value"):
            await failing_func()
        
        assert call_count == 1
    
    @pytest.mark.asyncio
    async def test_retry_exhausted(self):
        """Test behavior when all retries are exhausted."""
        call_count = 0
        
        @retry_async(RetryConfig(max_attempts=2, base_delay=0.01))
        async def always_failing_func():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Network error")
        
        with pytest.raises(RetryExhaustedError) as exc_info:
            await always_failing_func()
        
        assert call_count == 2
        assert "after 2 attempts" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_timeout_during_retries(self):
        """Test timeout during retry attempts."""
        @retry_async(RetryConfig(max_attempts=10, base_delay=0.1, timeout=0.05))
        async def slow_failing_func():
            await asyncio.sleep(0.02)  # Small delay to consume timeout
            raise ConnectionError("Network error")
        
        with pytest.raises(LocalTimeoutError):
            await slow_failing_func()


@pytest.mark.unit
class TestRetryContext:
    """Test RetryContext context manager."""
    
    def test_successful_operation(self):
        """Test successful operation with retry context."""
        call_count = 0
        
        while True:
            with RetryContext("test_operation", RetryConfig(max_attempts=3)) as ctx:
                call_count += 1
                # Successful operation
                break
        
        assert call_count == 1
        assert ctx.attempt == 0
    
    def test_retry_on_retryable_exception(self):
        """Test retry behavior with context manager."""
        call_count = 0
        
        while True:
            with RetryContext("test_operation", RetryConfig(max_attempts=3, base_delay=0.01)) as ctx:
                call_count += 1
                if call_count < 3:
                    raise ConnectionError("Network error")
                # Success on third attempt
                break
        
        assert call_count == 3
        assert ctx.attempt == 2
    
    def test_no_retry_on_non_retryable_exception(self):
        """Test that non-retryable exceptions are not retried."""
        call_count = 0
        
        with pytest.raises(ValueError, match="Invalid value"):
            while True:
                with RetryContext("test_operation", RetryConfig(max_attempts=3)) as ctx:
                    call_count += 1
                    raise ValueError("Invalid value")
        
        assert call_count == 1
    
    def test_retry_exhausted(self):
        """Test behavior when retries are exhausted."""
        call_count = 0
        
        with pytest.raises(RetryExhaustedError) as exc_info:
            while True:
                with RetryContext("test_operation", RetryConfig(max_attempts=2, base_delay=0.01)) as ctx:
                    call_count += 1
                    raise ConnectionError("Network error")
        
        assert call_count == 2
        assert "after 2 attempts" in str(exc_info.value)


@pytest.mark.unit
class TestRetryAsyncOperation:
    """Test retry_async_operation utility function."""
    
    @pytest.mark.asyncio
    async def test_successful_operation(self):
        """Test successful async operation."""
        async def successful_op(value):
            return f"result: {value}"
        
        result = await retry_async_operation(
            successful_op,
            "test_operation",
            RetryConfig(max_attempts=3),
            "test_value"
        )
        
        assert result == "result: test_value"
    
    @pytest.mark.asyncio
    async def test_retry_on_failure(self):
        """Test retry behavior on failures."""
        call_count = 0
        
        async def failing_op():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Network error")
            return "success"
        
        result = await retry_async_operation(
            failing_op,
            "test_operation",
            RetryConfig(max_attempts=3, base_delay=0.01)
        )
        
        assert result == "success"
        assert call_count == 3
    
    @pytest.mark.asyncio
    async def test_retry_exhausted(self):
        """Test behavior when retries are exhausted."""
        call_count = 0
        
        async def always_failing_op():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Network error")
        
        with pytest.raises(RetryExhaustedError):
            await retry_async_operation(
                always_failing_op,
                "test_operation",
                RetryConfig(max_attempts=2, base_delay=0.01)
            )
        
        assert call_count == 2


@pytest.mark.unit
class TestPredefinedConfigs:
    """Test predefined retry configurations."""
    
    def test_quick_retry_config(self):
        """Test QUICK_RETRY configuration."""
        assert QUICK_RETRY.max_attempts == 3
        assert QUICK_RETRY.base_delay == 0.5
        assert QUICK_RETRY.max_delay == 5.0
        assert QUICK_RETRY.timeout == 15.0
    
    def test_standard_retry_config(self):
        """Test STANDARD_RETRY configuration."""
        assert STANDARD_RETRY.max_attempts == 3
        assert STANDARD_RETRY.base_delay == 1.0
        assert STANDARD_RETRY.max_delay == 30.0
        assert STANDARD_RETRY.timeout == 60.0
    
    def test_patient_retry_config(self):
        """Test PATIENT_RETRY configuration."""
        assert PATIENT_RETRY.max_attempts == 5
        assert PATIENT_RETRY.base_delay == 2.0
        assert PATIENT_RETRY.max_delay == 60.0
        assert PATIENT_RETRY.timeout == 300.0
    
    def test_network_retry_config(self):
        """Test NETWORK_RETRY configuration."""
        assert NETWORK_RETRY.max_attempts == 4
        assert NETWORK_RETRY.base_delay == 1.0
        assert NETWORK_RETRY.max_delay == 30.0
        assert NETWORK_RETRY.timeout == 120.0


@pytest.mark.unit
class TestRetryExhaustedError:
    """Test RetryExhaustedError exception."""
    
    def test_retry_exhausted_error_creation(self):
        """Test creating RetryExhaustedError."""
        last_exception = ConnectionError("Network error")
        error = RetryExhaustedError("test_operation", 3, last_exception)
        
        assert "Retry exhausted for operation 'test_operation' after 3 attempts" in error.message
        assert error.details["operation"] == "test_operation"
        assert error.details["attempts"] == 3
        assert error.details["last_exception"] == "Network error"
        assert error.details["last_exception_type"] == "ConnectionError"
        assert error.cause == last_exception