"""
Port allocation manager for multi-cluster deployments.
"""

import socket
import asyncio
from typing import Set, Dict, List, Optional, Tuple
from dataclasses import dataclass
from ..models.multi_cluster import PortAllocation
from ..exceptions import PortAllocationError
from ..utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class PortRange:
    """Represents a range of ports."""
    start: int
    end: int
    
    def __post_init__(self):
        if self.start > self.end:
            raise ValueError(f"Invalid port range: {self.start} > {self.end}")
        if self.start < 1024 or self.end > 65535:
            raise ValueError(f"Port range must be between 1024 and 65535")
    
    def contains(self, port: int) -> bool:
        """Check if port is within this range."""
        return self.start <= port <= self.end
    
    def size(self) -> int:
        """Get the number of ports in this range."""
        return self.end - self.start + 1
    
    def to_list(self) -> List[int]:
        """Convert range to list of ports."""
        return list(range(self.start, self.end + 1))


class PortAllocator:
    """Manages port allocation for cluster services."""
    
    def __init__(self, 
                 port_ranges: Optional[List[PortRange]] = None,
                 reserved_ports: Optional[Set[int]] = None):
        """Initialize port allocator.
        
        Args:
            port_ranges: List of port ranges available for allocation
            reserved_ports: Set of ports that should never be allocated
        """
        # Default port ranges if none provided
        if port_ranges is None:
            port_ranges = [
                PortRange(9000, 9999),  # Kafka brokers
                PortRange(8000, 8999),  # REST proxies and UIs
                PortRange(7000, 7999),  # JMX ports
            ]
        
        self.port_ranges = port_ranges
        self.reserved_ports = reserved_ports or set()
        
        # Track allocated ports
        self.allocated_ports: Set[int] = set()
        self.cluster_allocations: Dict[str, PortAllocation] = {}
        
        # Lock for thread safety
        self._lock = asyncio.Lock()
        
        # Add commonly reserved ports
        self._add_system_reserved_ports()
        
        logger.info(f"Port allocator initialized with {len(self.port_ranges)} ranges")
    
    async def allocate_ports(self, cluster_id: str, 
                           preferred_ports: Optional[PortAllocation] = None) -> PortAllocation:
        """Allocate unique ports for a cluster.
        
        Args:
            cluster_id: Unique cluster identifier
            preferred_ports: Preferred port allocation (will use if available)
            
        Returns:
            PortAllocation with assigned ports
            
        Raises:
            PortAllocationError: If unable to allocate required ports
        """
        async with self._lock:
            logger.info(f"Allocating ports for cluster: {cluster_id}")
            
            # Check if cluster already has allocation
            if cluster_id in self.cluster_allocations:
                logger.warning(f"Cluster {cluster_id} already has port allocation")
                return self.cluster_allocations[cluster_id]
            
            try:
                # Try to use preferred ports if provided and available
                if preferred_ports and await self._can_use_preferred_ports(preferred_ports):
                    allocation = preferred_ports
                    logger.info(f"Using preferred ports for cluster {cluster_id}")
                else:
                    # Allocate new ports
                    allocation = await self._allocate_new_ports(cluster_id)
                
                # Reserve the allocated ports
                self._reserve_ports(allocation)
                self.cluster_allocations[cluster_id] = allocation
                
                logger.info(f"Allocated ports for {cluster_id}: {allocation.to_dict()}")
                return allocation
                
            except Exception as e:
                logger.error(f"Failed to allocate ports for {cluster_id}: {e}")
                raise PortAllocationError(
                    f"Failed to allocate ports for cluster {cluster_id}: {e}",
                    details={"cluster_id": cluster_id}
                )
    
    async def release_ports(self, cluster_id: str) -> bool:
        """Release ports allocated to a cluster.
        
        Args:
            cluster_id: Unique cluster identifier
            
        Returns:
            True if ports were released successfully
        """
        async with self._lock:
            if cluster_id not in self.cluster_allocations:
                logger.warning(f"No port allocation found for cluster {cluster_id}")
                return False
            
            allocation = self.cluster_allocations[cluster_id]
            
            # Remove ports from allocated set
            for port in allocation.get_all_ports():
                self.allocated_ports.discard(port)
            
            # Remove cluster allocation
            del self.cluster_allocations[cluster_id]
            
            logger.info(f"Released ports for cluster {cluster_id}")
            return True
    
    async def get_allocation(self, cluster_id: str) -> Optional[PortAllocation]:
        """Get port allocation for a cluster.
        
        Args:
            cluster_id: Unique cluster identifier
            
        Returns:
            PortAllocation if found, None otherwise
        """
        return self.cluster_allocations.get(cluster_id)
    
    async def is_port_available(self, port: int) -> bool:
        """Check if a port is available for allocation.
        
        Args:
            port: Port number to check
            
        Returns:
            True if port is available
        """
        # Check if port is in reserved set
        if port in self.reserved_ports:
            return False
        
        # Check if port is already allocated
        if port in self.allocated_ports:
            return False
        
        # Check if port is within allowed ranges
        if not any(port_range.contains(port) for port_range in self.port_ranges):
            return False
        
        # Check if port is actually available on the system
        return await self._is_port_free_on_system(port)
    
    async def get_available_ports(self, count: int = 10) -> List[int]:
        """Get a list of available ports.
        
        Args:
            count: Number of ports to return
            
        Returns:
            List of available port numbers
        """
        available_ports = []
        
        for port_range in self.port_ranges:
            for port in port_range.to_list():
                if len(available_ports) >= count:
                    break
                
                if await self.is_port_available(port):
                    available_ports.append(port)
            
            if len(available_ports) >= count:
                break
        
        return available_ports[:count]
    
    async def get_port_usage_stats(self) -> Dict[str, any]:
        """Get port usage statistics.
        
        Returns:
            Dictionary with port usage information
        """
        total_ports = sum(port_range.size() for port_range in self.port_ranges)
        allocated_count = len(self.allocated_ports)
        reserved_count = len(self.reserved_ports)
        
        # Calculate available ports (approximate)
        available_count = total_ports - allocated_count - reserved_count
        
        return {
            "total_ports": total_ports,
            "allocated_ports": allocated_count,
            "reserved_ports": reserved_count,
            "available_ports": max(0, available_count),
            "allocation_percentage": (allocated_count / total_ports) * 100 if total_ports > 0 else 0,
            "cluster_count": len(self.cluster_allocations),
            "port_ranges": [{"start": pr.start, "end": pr.end, "size": pr.size()} for pr in self.port_ranges]
        }
    
    async def validate_allocation(self, allocation: PortAllocation) -> List[str]:
        """Validate a port allocation.
        
        Args:
            allocation: Port allocation to validate
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Check for duplicate ports within allocation
        all_ports = allocation.get_all_ports()
        if len(all_ports) != len(set(all_ports)):
            errors.append("Allocation contains duplicate ports")
        
        # Check each port
        for port in all_ports:
            # Check if port is in valid range
            if not any(port_range.contains(port) for port_range in self.port_ranges):
                errors.append(f"Port {port} is outside allowed ranges")
            
            # Check if port is reserved
            if port in self.reserved_ports:
                errors.append(f"Port {port} is reserved")
            
            # Check if port is already allocated
            if port in self.allocated_ports:
                errors.append(f"Port {port} is already allocated")
        
        return errors
    
    async def suggest_alternative_ports(self, failed_allocation: PortAllocation) -> Optional[PortAllocation]:
        """Suggest alternative ports when allocation fails.
        
        Args:
            failed_allocation: The allocation that failed
            
        Returns:
            Alternative PortAllocation or None if no alternatives available
        """
        try:
            # Try to find alternative ports in the same ranges
            kafka_port = await self._find_available_port_in_range(PortRange(9000, 9999))
            rest_proxy_port = await self._find_available_port_in_range(PortRange(8000, 8999))
            ui_port = await self._find_available_port_in_range(PortRange(8000, 8999), exclude={rest_proxy_port})
            
            if kafka_port and rest_proxy_port and ui_port:
                jmx_port = None
                if failed_allocation.jmx_port:
                    jmx_port = await self._find_available_port_in_range(PortRange(7000, 7999))
                
                return PortAllocation(
                    kafka_port=kafka_port,
                    rest_proxy_port=rest_proxy_port,
                    ui_port=ui_port,
                    jmx_port=jmx_port
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to suggest alternative ports: {e}")
            return None
    
    # Private helper methods
    
    async def _can_use_preferred_ports(self, preferred_ports: PortAllocation) -> bool:
        """Check if preferred ports can be used."""
        for port in preferred_ports.get_all_ports():
            if not await self.is_port_available(port):
                return False
        return True
    
    async def _allocate_new_ports(self, cluster_id: str) -> PortAllocation:
        """Allocate new ports for a cluster."""
        # Find available ports for each service
        kafka_port = await self._find_available_port_in_range(PortRange(9000, 9999))
        if not kafka_port:
            raise PortAllocationError("No available Kafka ports")
        
        rest_proxy_port = await self._find_available_port_in_range(
            PortRange(8000, 8999), 
            exclude={kafka_port}
        )
        if not rest_proxy_port:
            raise PortAllocationError("No available REST Proxy ports")
        
        ui_port = await self._find_available_port_in_range(
            PortRange(8000, 8999), 
            exclude={kafka_port, rest_proxy_port}
        )
        if not ui_port:
            raise PortAllocationError("No available UI ports")
        
        # JMX port is optional
        jmx_port = await self._find_available_port_in_range(
            PortRange(7000, 7999),
            exclude={kafka_port, rest_proxy_port, ui_port}
        )
        
        return PortAllocation(
            kafka_port=kafka_port,
            rest_proxy_port=rest_proxy_port,
            ui_port=ui_port,
            jmx_port=jmx_port
        )
    
    async def _find_available_port_in_range(self, port_range: PortRange, 
                                          exclude: Optional[Set[int]] = None) -> Optional[int]:
        """Find an available port within a specific range."""
        exclude = exclude or set()
        
        for port in port_range.to_list():
            if port in exclude:
                continue
            
            if await self.is_port_available(port):
                return port
        
        return None
    
    def _reserve_ports(self, allocation: PortAllocation) -> None:
        """Reserve ports in the allocation."""
        for port in allocation.get_all_ports():
            self.allocated_ports.add(port)
    
    async def _is_port_free_on_system(self, port: int) -> bool:
        """Check if port is actually free on the system."""
        try:
            # Try to bind to the port
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            result = sock.bind(('localhost', port))
            sock.close()
            return True
        except OSError:
            return False
    
    def _add_system_reserved_ports(self) -> None:
        """Add commonly reserved system ports."""
        # Common system ports that should not be allocated
        system_ports = {
            22,    # SSH
            25,    # SMTP
            53,    # DNS
            80,    # HTTP
            110,   # POP3
            143,   # IMAP
            443,   # HTTPS
            993,   # IMAPS
            995,   # POP3S
            3306,  # MySQL
            5432,  # PostgreSQL
            6379,  # Redis
            27017, # MongoDB
        }
        
        self.reserved_ports.update(system_ports)
        
        # Add ports that are commonly used by development tools
        dev_ports = {
            3000,  # React dev server
            3001,  # Alternative React
            4200,  # Angular dev server
            5000,  # Flask default
            5173,  # Vite dev server
            8080,  # Common HTTP alternate (but we might use this for Kafka UI)
            9000,  # Common application port
        }
        
        # Only add dev ports if they're not in our allocation ranges
        for port in dev_ports:
            if not any(port_range.contains(port) for port_range in self.port_ranges):
                self.reserved_ports.add(port)


class GlobalPortAllocator:
    """Global singleton port allocator for backward compatibility."""
    
    _instance: Optional[PortAllocator] = None
    _lock = asyncio.Lock()
    
    @classmethod
    async def get_instance(cls) -> PortAllocator:
        """Get the global port allocator instance."""
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = PortAllocator()
                    logger.info("Created global port allocator instance")
        return cls._instance
    
    @classmethod
    async def reset_instance(cls) -> None:
        """Reset the global instance (mainly for testing)."""
        async with cls._lock:
            cls._instance = None
            logger.info("Reset global port allocator instance")


async def allocate_cluster_ports(cluster_id: str, 
                               preferred_ports: Optional[PortAllocation] = None) -> PortAllocation:
    """Convenience function to allocate ports for a cluster.
    
    Args:
        cluster_id: Unique cluster identifier
        preferred_ports: Preferred port allocation
        
    Returns:
        PortAllocation with assigned ports
    """
    allocator = await GlobalPortAllocator.get_instance()
    return await allocator.allocate_ports(cluster_id, preferred_ports)


async def release_cluster_ports(cluster_id: str) -> bool:
    """Convenience function to release ports for a cluster.
    
    Args:
        cluster_id: Unique cluster identifier
        
    Returns:
        True if ports were released successfully
    """
    allocator = await GlobalPortAllocator.get_instance()
    return await allocator.release_ports(cluster_id)


class PortPool:
    """Pool of pre-allocated ports for faster allocation."""
    
    def __init__(self, allocator: PortAllocator, pool_size: int = 50):
        """Initialize port pool.
        
        Args:
            allocator: Port allocator instance
            pool_size: Number of ports to keep in pool
        """
        self.allocator = allocator
        self.pool_size = pool_size
        self.available_ports: List[int] = []
        self._lock = asyncio.Lock()
    
    async def initialize(self) -> None:
        """Initialize the port pool."""
        async with self._lock:
            self.available_ports = await self.allocator.get_available_ports(self.pool_size)
            logger.info(f"Port pool initialized with {len(self.available_ports)} ports")
    
    async def get_port(self) -> Optional[int]:
        """Get a port from the pool.
        
        Returns:
            Available port number or None if pool is empty
        """
        async with self._lock:
            if not self.available_ports:
                # Try to refill pool
                await self._refill_pool()
            
            if self.available_ports:
                port = self.available_ports.pop(0)
                # Verify port is still available
                if await self.allocator.is_port_available(port):
                    return port
                else:
                    # Port is no longer available, try next one
                    return await self.get_port()
            
            return None
    
    async def return_port(self, port: int) -> None:
        """Return a port to the pool.
        
        Args:
            port: Port number to return
        """
        async with self._lock:
            if port not in self.available_ports and await self.allocator.is_port_available(port):
                self.available_ports.append(port)
    
    async def _refill_pool(self) -> None:
        """Refill the port pool."""
        needed = self.pool_size - len(self.available_ports)
        if needed > 0:
            new_ports = await self.allocator.get_available_ports(needed)
            self.available_ports.extend(new_ports)
            logger.debug(f"Refilled port pool with {len(new_ports)} ports")