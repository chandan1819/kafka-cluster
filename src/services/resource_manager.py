"""
Resource management and optimization for multi-cluster Kafka deployments.

This module provides comprehensive resource monitoring, quota enforcement,
automatic cleanup, and scaling recommendations for multi-cluster environments.
"""

import asyncio
import logging
import psutil
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

from src.models.multi_cluster import ClusterDefinition
from src.services.multi_cluster_manager import MultiClusterManager
from src.exceptions import ResourceError, QuotaExceededError


class ResourceType(Enum):
    """Types of resources managed."""
    CPU = "cpu"
    MEMORY = "memory"
    DISK = "disk"
    NETWORK = "network"
    PORTS = "ports"


class ScalingRecommendation(Enum):
    """Scaling recommendations."""
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    MAINTAIN = "maintain"
    OPTIMIZE = "optimize"


@dataclass
class ResourceUsage:
    """Resource usage information."""
    resource_type: ResourceType
    cluster_id: Optional[str]
    current_usage: float
    limit: Optional[float]
    unit: str
    timestamp: datetime
    details: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def usage_percentage(self) -> Optional[float]:
        """Calculate usage percentage if limit is set."""
        if self.limit and self.limit > 0:
            return (self.current_usage / self.limit) * 100
        return None
    
    @property
    def is_over_limit(self) -> bool:
        """Check if usage exceeds limit."""
        if self.limit:
            return self.current_usage > self.limit
        return False


@dataclass
class ResourceQuota:
    """Resource quota configuration."""
    resource_type: ResourceType
    cluster_id: Optional[str]  # None for system-wide quotas
    soft_limit: float
    hard_limit: float
    unit: str
    enabled: bool = True
    
    def check_quota(self, usage: float) -> Tuple[bool, str]:
        """Check if usage violates quota."""
        if not self.enabled:
            return True, "Quota disabled"
        
        if usage > self.hard_limit:
            return False, f"Hard limit exceeded: {usage:.2f} > {self.hard_limit:.2f} {self.unit}"
        elif usage > self.soft_limit:
            return True, f"Soft limit exceeded: {usage:.2f} > {self.soft_limit:.2f} {self.unit}"
        
        return True, "Within quota"


@dataclass
class CleanupPolicy:
    """Cleanup policy configuration."""
    name: str
    enabled: bool
    max_age_days: int
    target_patterns: List[str]
    cluster_states: List[str]  # e.g., ["stopped", "failed"]
    dry_run: bool = False
    
    def should_cleanup(self, cluster: ClusterDefinition, last_activity: datetime) -> bool:
        """Check if cluster should be cleaned up."""
        if not self.enabled:
            return False
        
        # Check age
        age = datetime.now() - last_activity
        if age.days < self.max_age_days:
            return False
        
        # Check cluster state (if applicable)
        if self.cluster_states and hasattr(cluster, 'status'):
            if cluster.status not in self.cluster_states:
                return False
        
        return True


@dataclass
class ScalingRecommendationResult:
    """Scaling recommendation result."""
    cluster_id: str
    recommendation: ScalingRecommendation
    reason: str
    current_metrics: Dict[str, float]
    suggested_changes: Dict[str, Any]
    confidence: float  # 0.0 to 1.0
    timestamp: datetime


class ResourceManager:
    """
    Comprehensive resource management for multi-cluster deployments.
    
    Provides resource monitoring, quota enforcement, automatic cleanup,
    and intelligent scaling recommendations.
    """
    
    def __init__(
        self,
        multi_cluster_manager: MultiClusterManager,
        data_directory: Optional[Path] = None
    ):
        self.multi_cluster_manager = multi_cluster_manager
        self.data_directory = data_directory or Path("resource_data")
        self.data_directory.mkdir(parents=True, exist_ok=True)
        
        # Resource tracking
        self._resource_usage: Dict[str, List[ResourceUsage]] = {}
        self._resource_quotas: Dict[str, ResourceQuota] = {}
        self._cleanup_policies: Dict[str, CleanupPolicy] = {}
        
        # Monitoring state
        self._monitoring_active = False
        self._monitoring_tasks: List[asyncio.Task] = []
        
        # Configuration
        self.monitoring_interval = 60  # seconds
        self.cleanup_interval = 3600   # seconds (1 hour)
        self.retention_days = 30
        
        self.logger = logging.getLogger(__name__)
        
        # Initialize default quotas and policies
        self._initialize_default_configuration()
    
    def _initialize_default_configuration(self) -> None:
        """Initialize default resource quotas and cleanup policies."""
        # Default system-wide quotas
        self._resource_quotas["system_cpu"] = ResourceQuota(
            resource_type=ResourceType.CPU,
            cluster_id=None,
            soft_limit=80.0,
            hard_limit=95.0,
            unit="percent"
        )
        
        self._resource_quotas["system_memory"] = ResourceQuota(
            resource_type=ResourceType.MEMORY,
            cluster_id=None,
            soft_limit=85.0,
            hard_limit=95.0,
            unit="percent"
        )
        
        self._resource_quotas["system_disk"] = ResourceQuota(
            resource_type=ResourceType.DISK,
            cluster_id=None,
            soft_limit=90.0,
            hard_limit=98.0,
            unit="percent"
        )
        
        # Default cleanup policies
        self._cleanup_policies["stopped_clusters"] = CleanupPolicy(
            name="Stopped Clusters Cleanup",
            enabled=True,
            max_age_days=7,
            target_patterns=["*"],
            cluster_states=["stopped", "failed"]
        )
        
        self._cleanup_policies["old_logs"] = CleanupPolicy(
            name="Old Log Files Cleanup",
            enabled=True,
            max_age_days=30,
            target_patterns=["*.log", "*.log.*"],
            cluster_states=[]
        )
    
    async def start_monitoring(self) -> None:
        """Start resource monitoring."""
        if self._monitoring_active:
            self.logger.warning("Resource monitoring is already active")
            return
        
        self._monitoring_active = True
        self.logger.info("Starting resource monitoring")
        
        # Start monitoring tasks
        self._monitoring_tasks = [
            asyncio.create_task(self._resource_monitoring_loop()),
            asyncio.create_task(self._cleanup_monitoring_loop()),
            asyncio.create_task(self._quota_enforcement_loop())
        ]
    
    async def stop_monitoring(self) -> None:
        """Stop resource monitoring."""
        if not self._monitoring_active:
            return
        
        self.logger.info("Stopping resource monitoring")
        self._monitoring_active = False
        
        # Cancel monitoring tasks
        for task in self._monitoring_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        if self._monitoring_tasks:
            await asyncio.gather(*self._monitoring_tasks, return_exceptions=True)
        
        self._monitoring_tasks.clear()
    
    async def get_system_resource_usage(self) -> Dict[str, ResourceUsage]:
        """Get current system resource usage."""
        try:
            timestamp = datetime.now()
            usage = {}
            
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            usage["cpu"] = ResourceUsage(
                resource_type=ResourceType.CPU,
                cluster_id=None,
                current_usage=cpu_percent,
                limit=100.0,
                unit="percent",
                timestamp=timestamp,
                details={
                    "cpu_count": psutil.cpu_count(),
                    "load_average": list(psutil.getloadavg()) if hasattr(psutil, 'getloadavg') else None
                }
            )
            
            # Memory usage
            memory = psutil.virtual_memory()
            usage["memory"] = ResourceUsage(
                resource_type=ResourceType.MEMORY,
                cluster_id=None,
                current_usage=memory.percent,
                limit=100.0,
                unit="percent",
                timestamp=timestamp,
                details={
                    "total_bytes": memory.total,
                    "available_bytes": memory.available,
                    "used_bytes": memory.used
                }
            )
            
            # Disk usage
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            usage["disk"] = ResourceUsage(
                resource_type=ResourceType.DISK,
                cluster_id=None,
                current_usage=disk_percent,
                limit=100.0,
                unit="percent",
                timestamp=timestamp,
                details={
                    "total_bytes": disk.total,
                    "used_bytes": disk.used,
                    "free_bytes": disk.free
                }
            )
            
            # Network usage
            network = psutil.net_io_counters()
            usage["network"] = ResourceUsage(
                resource_type=ResourceType.NETWORK,
                cluster_id=None,
                current_usage=network.bytes_sent + network.bytes_recv,
                limit=None,
                unit="bytes",
                timestamp=timestamp,
                details={
                    "bytes_sent": network.bytes_sent,
                    "bytes_recv": network.bytes_recv,
                    "packets_sent": network.packets_sent,
                    "packets_recv": network.packets_recv
                }
            )
            
            return usage
            
        except Exception as e:
            self.logger.error(f"Error getting system resource usage: {e}")
            raise ResourceError(f"Failed to get system resource usage: {e}")
    
    async def get_cluster_resource_usage(self, cluster_id: str) -> Dict[str, ResourceUsage]:
        """Get resource usage for a specific cluster."""
        try:
            cluster = await self.multi_cluster_manager.registry.get_cluster(cluster_id)
            if not cluster:
                raise ResourceError(f"Cluster {cluster_id} not found")
            
            timestamp = datetime.now()
            usage = {}
            
            # Get cluster data directory size
            cluster_data_dir = Path(f"data/{cluster_id}")
            if cluster_data_dir.exists():
                disk_usage = self._get_directory_size(cluster_data_dir)
                usage["disk"] = ResourceUsage(
                    resource_type=ResourceType.DISK,
                    cluster_id=cluster_id,
                    current_usage=disk_usage,
                    limit=None,
                    unit="bytes",
                    timestamp=timestamp,
                    details={
                        "data_directory": str(cluster_data_dir),
                        "file_count": len(list(cluster_data_dir.rglob("*")))
                    }
                )
            
            # Port usage
            port_count = 0
            if cluster.port_allocation:
                ports = [
                    cluster.port_allocation.kafka_port,
                    cluster.port_allocation.rest_proxy_port,
                    cluster.port_allocation.ui_port
                ]
                if cluster.port_allocation.jmx_port:
                    ports.append(cluster.port_allocation.jmx_port)
                port_count = len(ports)
            
            usage["ports"] = ResourceUsage(
                resource_type=ResourceType.PORTS,
                cluster_id=cluster_id,
                current_usage=port_count,
                limit=None,
                unit="count",
                timestamp=timestamp,
                details={
                    "allocated_ports": ports if 'ports' in locals() else []
                }
            )
            
            return usage
            
        except Exception as e:
            self.logger.error(f"Error getting cluster resource usage for {cluster_id}: {e}")
            raise ResourceError(f"Failed to get cluster resource usage: {e}")
    
    async def get_all_clusters_resource_usage(self) -> Dict[str, Dict[str, ResourceUsage]]:
        """Get resource usage for all clusters."""
        try:
            clusters = await self.multi_cluster_manager.list_clusters()
            all_usage = {}
            
            for cluster in clusters:
                try:
                    cluster_usage = await self.get_cluster_resource_usage(cluster.id)
                    all_usage[cluster.id] = cluster_usage
                except Exception as e:
                    self.logger.warning(f"Failed to get usage for cluster {cluster.id}: {e}")
                    all_usage[cluster.id] = {}
            
            return all_usage
            
        except Exception as e:
            self.logger.error(f"Error getting all clusters resource usage: {e}")
            raise ResourceError(f"Failed to get all clusters resource usage: {e}")
    
    def set_resource_quota(self, quota: ResourceQuota) -> None:
        """Set a resource quota."""
        quota_key = f"{quota.resource_type.value}_{quota.cluster_id or 'system'}"
        self._resource_quotas[quota_key] = quota
        self.logger.info(f"Set resource quota: {quota_key}")
    
    def get_resource_quota(self, resource_type: ResourceType, cluster_id: Optional[str] = None) -> Optional[ResourceQuota]:
        """Get a resource quota."""
        quota_key = f"{resource_type.value}_{cluster_id or 'system'}"
        return self._resource_quotas.get(quota_key)
    
    def remove_resource_quota(self, resource_type: ResourceType, cluster_id: Optional[str] = None) -> bool:
        """Remove a resource quota."""
        quota_key = f"{resource_type.value}_{cluster_id or 'system'}"
        if quota_key in self._resource_quotas:
            del self._resource_quotas[quota_key]
            self.logger.info(f"Removed resource quota: {quota_key}")
            return True
        return False
    
    async def check_resource_quotas(self) -> Dict[str, Tuple[bool, str]]:
        """Check all resource quotas against current usage."""
        try:
            results = {}
            
            # Check system quotas
            system_usage = await self.get_system_resource_usage()
            
            for usage_key, usage in system_usage.items():
                quota = self.get_resource_quota(usage.resource_type)
                if quota:
                    is_valid, message = quota.check_quota(usage.current_usage)
                    results[f"system_{usage_key}"] = (is_valid, message)
            
            # Check cluster-specific quotas
            clusters_usage = await self.get_all_clusters_resource_usage()
            
            for cluster_id, cluster_usage in clusters_usage.items():
                for usage_key, usage in cluster_usage.items():
                    quota = self.get_resource_quota(usage.resource_type, cluster_id)
                    if quota:
                        is_valid, message = quota.check_quota(usage.current_usage)
                        results[f"{cluster_id}_{usage_key}"] = (is_valid, message)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error checking resource quotas: {e}")
            raise ResourceError(f"Failed to check resource quotas: {e}")
    
    def add_cleanup_policy(self, policy: CleanupPolicy) -> None:
        """Add a cleanup policy."""
        self._cleanup_policies[policy.name] = policy
        self.logger.info(f"Added cleanup policy: {policy.name}")
    
    def remove_cleanup_policy(self, policy_name: str) -> bool:
        """Remove a cleanup policy."""
        if policy_name in self._cleanup_policies:
            del self._cleanup_policies[policy_name]
            self.logger.info(f"Removed cleanup policy: {policy_name}")
            return True
        return False
    
    async def run_cleanup(self, policy_name: Optional[str] = None, dry_run: bool = False) -> Dict[str, Any]:
        """Run cleanup based on policies."""
        try:
            cleanup_results = {
                "timestamp": datetime.now().isoformat(),
                "dry_run": dry_run,
                "policies_executed": [],
                "clusters_cleaned": [],
                "files_cleaned": [],
                "space_freed_bytes": 0,
                "errors": []
            }
            
            policies_to_run = []
            if policy_name:
                if policy_name in self._cleanup_policies:
                    policies_to_run = [self._cleanup_policies[policy_name]]
                else:
                    raise ResourceError(f"Cleanup policy '{policy_name}' not found")
            else:
                policies_to_run = list(self._cleanup_policies.values())
            
            for policy in policies_to_run:
                if not policy.enabled:
                    continue
                
                try:
                    policy_results = await self._execute_cleanup_policy(policy, dry_run)
                    cleanup_results["policies_executed"].append(policy.name)
                    cleanup_results["clusters_cleaned"].extend(policy_results.get("clusters_cleaned", []))
                    cleanup_results["files_cleaned"].extend(policy_results.get("files_cleaned", []))
                    cleanup_results["space_freed_bytes"] += policy_results.get("space_freed_bytes", 0)
                    
                except Exception as e:
                    error_msg = f"Error executing policy '{policy.name}': {e}"
                    self.logger.error(error_msg)
                    cleanup_results["errors"].append(error_msg)
            
            return cleanup_results
            
        except Exception as e:
            self.logger.error(f"Error running cleanup: {e}")
            raise ResourceError(f"Failed to run cleanup: {e}")
    
    async def get_scaling_recommendations(self, cluster_id: Optional[str] = None) -> List[ScalingRecommendationResult]:
        """Get scaling recommendations for clusters."""
        try:
            recommendations = []
            
            if cluster_id:
                # Get recommendation for specific cluster
                cluster = await self.multi_cluster_manager.registry.get_cluster(cluster_id)
                if cluster:
                    recommendation = await self._analyze_cluster_scaling(cluster)
                    if recommendation:
                        recommendations.append(recommendation)
            else:
                # Get recommendations for all clusters
                clusters = await self.multi_cluster_manager.list_clusters()
                
                for cluster in clusters:
                    try:
                        recommendation = await self._analyze_cluster_scaling(cluster)
                        if recommendation:
                            recommendations.append(recommendation)
                    except Exception as e:
                        self.logger.warning(f"Failed to analyze scaling for cluster {cluster.id}: {e}")
            
            return recommendations
            
        except Exception as e:
            self.logger.error(f"Error getting scaling recommendations: {e}")
            raise ResourceError(f"Failed to get scaling recommendations: {e}")
    
    async def optimize_resource_allocation(self) -> Dict[str, Any]:
        """Optimize resource allocation across clusters."""
        try:
            optimization_results = {
                "timestamp": datetime.now().isoformat(),
                "system_optimization": {},
                "cluster_optimizations": [],
                "recommendations": [],
                "potential_savings": {}
            }
            
            # Analyze system-wide resource usage
            system_usage = await self.get_system_resource_usage()
            clusters_usage = await self.get_all_clusters_resource_usage()
            
            # System-level optimizations
            optimization_results["system_optimization"] = await self._optimize_system_resources(system_usage)
            
            # Cluster-level optimizations
            for cluster_id, cluster_usage in clusters_usage.items():
                cluster_optimization = await self._optimize_cluster_resources(cluster_id, cluster_usage)
                if cluster_optimization:
                    optimization_results["cluster_optimizations"].append(cluster_optimization)
            
            # Generate recommendations
            scaling_recommendations = await self.get_scaling_recommendations()
            optimization_results["recommendations"] = [
                {
                    "cluster_id": rec.cluster_id,
                    "recommendation": rec.recommendation.value,
                    "reason": rec.reason,
                    "confidence": rec.confidence
                }
                for rec in scaling_recommendations
            ]
            
            # Calculate potential savings
            optimization_results["potential_savings"] = self._calculate_potential_savings(
                system_usage, clusters_usage, scaling_recommendations
            )
            
            return optimization_results
            
        except Exception as e:
            self.logger.error(f"Error optimizing resource allocation: {e}")
            raise ResourceError(f"Failed to optimize resource allocation: {e}")
    
    def _get_directory_size(self, directory: Path) -> int:
        """Get total size of directory in bytes."""
        try:
            total_size = 0
            for file_path in directory.rglob("*"):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
            return total_size
        except Exception as e:
            self.logger.warning(f"Error calculating directory size for {directory}: {e}")
            return 0
    
    async def _resource_monitoring_loop(self) -> None:
        """Main resource monitoring loop."""
        while self._monitoring_active:
            try:
                # Collect resource usage
                await self._collect_resource_usage()
                
                # Store usage data
                await self._store_resource_usage()
                
                await asyncio.sleep(self.monitoring_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in resource monitoring loop: {e}")
                await asyncio.sleep(10)  # Brief pause before retrying
    
    async def _cleanup_monitoring_loop(self) -> None:
        """Cleanup monitoring loop."""
        while self._monitoring_active:
            try:
                # Run automatic cleanup
                await self.run_cleanup()
                
                # Clean up old monitoring data
                await self._cleanup_old_data()
                
                await asyncio.sleep(self.cleanup_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in cleanup monitoring loop: {e}")
                await asyncio.sleep(60)  # Brief pause before retrying
    
    async def _quota_enforcement_loop(self) -> None:
        """Quota enforcement loop."""
        while self._monitoring_active:
            try:
                # Check quotas
                quota_results = await self.check_resource_quotas()
                
                # Handle quota violations
                for quota_key, (is_valid, message) in quota_results.items():
                    if not is_valid:
                        self.logger.warning(f"Quota violation: {quota_key} - {message}")
                        # Could trigger alerts or automatic actions here
                
                await asyncio.sleep(self.monitoring_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in quota enforcement loop: {e}")
                await asyncio.sleep(30)  # Brief pause before retrying    
 
   async def _collect_resource_usage(self) -> None:
        """Collect current resource usage."""
        try:
            timestamp = datetime.now()
            
            # Collect system usage
            system_usage = await self.get_system_resource_usage()
            for usage_key, usage in system_usage.items():
                self._store_usage_data("system", usage)
            
            # Collect cluster usage
            clusters_usage = await self.get_all_clusters_resource_usage()
            for cluster_id, cluster_usage in clusters_usage.items():
                for usage_key, usage in cluster_usage.items():
                    self._store_usage_data(cluster_id, usage)
                    
        except Exception as e:
            self.logger.error(f"Error collecting resource usage: {e}")
    
    def _store_usage_data(self, key: str, usage: ResourceUsage) -> None:
        """Store usage data in memory."""
        if key not in self._resource_usage:
            self._resource_usage[key] = []
        
        self._resource_usage[key].append(usage)
        
        # Keep only recent data (last 24 hours)
        cutoff_time = datetime.now() - timedelta(hours=24)
        self._resource_usage[key] = [
            u for u in self._resource_usage[key]
            if u.timestamp > cutoff_time
        ]
    
    async def _store_resource_usage(self) -> None:
        """Store resource usage data to disk."""
        try:
            usage_file = self.data_directory / "resource_usage.json"
            
            # Convert usage data to serializable format
            usage_data = {}
            for key, usage_list in self._resource_usage.items():
                usage_data[key] = [
                    {
                        "resource_type": usage.resource_type.value,
                        "cluster_id": usage.cluster_id,
                        "current_usage": usage.current_usage,
                        "limit": usage.limit,
                        "unit": usage.unit,
                        "timestamp": usage.timestamp.isoformat(),
                        "details": usage.details
                    }
                    for usage in usage_list[-10:]  # Keep last 10 entries per key
                ]
            
            import json
            with open(usage_file, 'w') as f:
                json.dump(usage_data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Error storing resource usage data: {e}")
    
    async def _execute_cleanup_policy(self, policy: CleanupPolicy, dry_run: bool = False) -> Dict[str, Any]:
        """Execute a specific cleanup policy."""
        results = {
            "policy_name": policy.name,
            "clusters_cleaned": [],
            "files_cleaned": [],
            "space_freed_bytes": 0
        }
        
        try:
            if policy.name == "stopped_clusters":
                results.update(await self._cleanup_stopped_clusters(policy, dry_run))
            elif policy.name == "old_logs":
                results.update(await self._cleanup_old_files(policy, dry_run))
            else:
                # Generic file cleanup
                results.update(await self._cleanup_old_files(policy, dry_run))
                
        except Exception as e:
            self.logger.error(f"Error executing cleanup policy {policy.name}: {e}")
            raise
        
        return results
    
    async def _cleanup_stopped_clusters(self, policy: CleanupPolicy, dry_run: bool = False) -> Dict[str, Any]:
        """Clean up stopped clusters based on policy."""
        results = {
            "clusters_cleaned": [],
            "space_freed_bytes": 0
        }
        
        try:
            clusters = await self.multi_cluster_manager.list_clusters()
            
            for cluster in clusters:
                # Check if cluster should be cleaned up
                last_activity = cluster.last_stopped or cluster.created_at
                
                if policy.should_cleanup(cluster, last_activity):
                    # Check if cluster is actually stopped
                    try:
                        status = await self.multi_cluster_manager.get_cluster_status(cluster.id)
                        if status and hasattr(status, 'kafka_status'):
                            kafka_status = getattr(status, 'kafka_status')
                            if kafka_status in policy.cluster_states:
                                if not dry_run:
                                    # Calculate space before deletion
                                    cluster_data_dir = Path(f"data/{cluster.id}")
                                    space_freed = 0
                                    if cluster_data_dir.exists():
                                        space_freed = self._get_directory_size(cluster_data_dir)
                                    
                                    # Delete cluster
                                    await self.multi_cluster_manager.delete_cluster(cluster.id, force=True)
                                    results["space_freed_bytes"] += space_freed
                                
                                results["clusters_cleaned"].append({
                                    "cluster_id": cluster.id,
                                    "cluster_name": cluster.name,
                                    "last_activity": last_activity.isoformat(),
                                    "status": kafka_status
                                })
                                
                                self.logger.info(f"{'Would clean' if dry_run else 'Cleaned'} cluster {cluster.id}")
                    except Exception as e:
                        self.logger.warning(f"Error checking status for cluster {cluster.id}: {e}")
                        
        except Exception as e:
            self.logger.error(f"Error in stopped clusters cleanup: {e}")
            raise
        
        return results
    
    async def _cleanup_old_files(self, policy: CleanupPolicy, dry_run: bool = False) -> Dict[str, Any]:
        """Clean up old files based on policy."""
        results = {
            "files_cleaned": [],
            "space_freed_bytes": 0
        }
        
        try:
            cutoff_date = datetime.now() - timedelta(days=policy.max_age_days)
            
            # Search for files matching patterns
            for pattern in policy.target_patterns:
                for file_path in Path(".").rglob(pattern):
                    if file_path.is_file():
                        # Check file age
                        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        
                        if file_mtime < cutoff_date:
                            file_size = file_path.stat().st_size
                            
                            if not dry_run:
                                file_path.unlink()
                                results["space_freed_bytes"] += file_size
                            
                            results["files_cleaned"].append({
                                "file_path": str(file_path),
                                "size_bytes": file_size,
                                "modified_time": file_mtime.isoformat()
                            })
                            
                            self.logger.debug(f"{'Would clean' if dry_run else 'Cleaned'} file {file_path}")
                            
        except Exception as e:
            self.logger.error(f"Error in old files cleanup: {e}")
            raise
        
        return results
    
    async def _analyze_cluster_scaling(self, cluster: ClusterDefinition) -> Optional[ScalingRecommendationResult]:
        """Analyze scaling needs for a cluster."""
        try:
            # Get cluster resource usage history
            cluster_usage_history = self._resource_usage.get(cluster.id, [])
            
            if not cluster_usage_history:
                return None
            
            # Analyze recent usage patterns
            recent_usage = [u for u in cluster_usage_history if u.timestamp > datetime.now() - timedelta(hours=1)]
            
            if not recent_usage:
                return None
            
            # Calculate metrics
            current_metrics = {}
            for usage in recent_usage:
                metric_key = f"{usage.resource_type.value}_usage"
                if metric_key not in current_metrics:
                    current_metrics[metric_key] = []
                current_metrics[metric_key].append(usage.current_usage)
            
            # Average the metrics
            avg_metrics = {
                key: sum(values) / len(values)
                for key, values in current_metrics.items()
            }
            
            # Determine recommendation
            recommendation, reason, suggested_changes, confidence = self._determine_scaling_recommendation(
                cluster, avg_metrics
            )
            
            return ScalingRecommendationResult(
                cluster_id=cluster.id,
                recommendation=recommendation,
                reason=reason,
                current_metrics=avg_metrics,
                suggested_changes=suggested_changes,
                confidence=confidence,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"Error analyzing cluster scaling for {cluster.id}: {e}")
            return None
    
    def _determine_scaling_recommendation(
        self,
        cluster: ClusterDefinition,
        metrics: Dict[str, float]
    ) -> Tuple[ScalingRecommendation, str, Dict[str, Any], float]:
        """Determine scaling recommendation based on metrics."""
        
        # Get disk usage if available
        disk_usage = metrics.get("disk_usage", 0)
        
        # Simple heuristics for scaling recommendations
        if disk_usage > 1_000_000_000:  # > 1GB
            if disk_usage > 10_000_000_000:  # > 10GB
                return (
                    ScalingRecommendation.OPTIMIZE,
                    f"High disk usage ({disk_usage / 1_000_000_000:.1f}GB). Consider data retention policies.",
                    {"suggested_retention_days": 7, "compress_logs": True},
                    0.8
                )
            else:
                return (
                    ScalingRecommendation.MAINTAIN,
                    f"Moderate disk usage ({disk_usage / 1_000_000_000:.1f}GB). Monitor growth.",
                    {"monitor_growth": True},
                    0.6
                )
        
        # Check if cluster has been inactive
        try:
            status = asyncio.create_task(self.multi_cluster_manager.get_cluster_status(cluster.id))
            # This is a simplified check - in practice, you'd want more sophisticated analysis
            return (
                ScalingRecommendation.MAINTAIN,
                "Cluster appears to be operating normally.",
                {},
                0.5
            )
        except Exception:
            return (
                ScalingRecommendation.SCALE_DOWN,
                "Cluster appears to be inactive or unreachable.",
                {"consider_shutdown": True},
                0.7
            )
    
    async def _optimize_system_resources(self, system_usage: Dict[str, ResourceUsage]) -> Dict[str, Any]:
        """Optimize system-level resources."""
        optimizations = {
            "cpu_optimization": {},
            "memory_optimization": {},
            "disk_optimization": {},
            "recommendations": []
        }
        
        try:
            # CPU optimization
            cpu_usage = system_usage.get("cpu")
            if cpu_usage and cpu_usage.current_usage > 80:
                optimizations["cpu_optimization"] = {
                    "current_usage": cpu_usage.current_usage,
                    "recommendation": "Consider reducing cluster count or optimizing configurations"
                }
                optimizations["recommendations"].append("High CPU usage detected")
            
            # Memory optimization
            memory_usage = system_usage.get("memory")
            if memory_usage and memory_usage.current_usage > 85:
                optimizations["memory_optimization"] = {
                    "current_usage": memory_usage.current_usage,
                    "recommendation": "Consider increasing system memory or reducing cluster memory allocations"
                }
                optimizations["recommendations"].append("High memory usage detected")
            
            # Disk optimization
            disk_usage = system_usage.get("disk")
            if disk_usage and disk_usage.current_usage > 90:
                optimizations["disk_optimization"] = {
                    "current_usage": disk_usage.current_usage,
                    "recommendation": "Run cleanup policies or increase disk space"
                }
                optimizations["recommendations"].append("High disk usage detected")
            
        except Exception as e:
            self.logger.error(f"Error optimizing system resources: {e}")
        
        return optimizations
    
    async def _optimize_cluster_resources(self, cluster_id: str, cluster_usage: Dict[str, ResourceUsage]) -> Optional[Dict[str, Any]]:
        """Optimize resources for a specific cluster."""
        try:
            optimization = {
                "cluster_id": cluster_id,
                "optimizations": [],
                "potential_savings": {}
            }
            
            # Disk optimization
            disk_usage = cluster_usage.get("disk")
            if disk_usage and disk_usage.current_usage > 5_000_000_000:  # > 5GB
                optimization["optimizations"].append({
                    "type": "disk_cleanup",
                    "current_usage_gb": disk_usage.current_usage / 1_000_000_000,
                    "recommendation": "Consider running log cleanup or data retention policies"
                })
                
                # Estimate potential savings
                optimization["potential_savings"]["disk_gb"] = disk_usage.current_usage * 0.3 / 1_000_000_000  # 30% savings estimate
            
            # Port optimization
            port_usage = cluster_usage.get("ports")
            if port_usage and port_usage.current_usage > 4:  # More than standard ports
                optimization["optimizations"].append({
                    "type": "port_optimization",
                    "current_ports": port_usage.current_usage,
                    "recommendation": "Review port allocation for unused services"
                })
            
            return optimization if optimization["optimizations"] else None
            
        except Exception as e:
            self.logger.error(f"Error optimizing cluster resources for {cluster_id}: {e}")
            return None
    
    def _calculate_potential_savings(
        self,
        system_usage: Dict[str, ResourceUsage],
        clusters_usage: Dict[str, Dict[str, ResourceUsage]],
        scaling_recommendations: List[ScalingRecommendationResult]
    ) -> Dict[str, Any]:
        """Calculate potential resource savings."""
        savings = {
            "disk_space_gb": 0,
            "memory_mb": 0,
            "cpu_percent": 0,
            "clusters_to_optimize": 0
        }
        
        try:
            # Calculate savings from scaling recommendations
            for recommendation in scaling_recommendations:
                if recommendation.recommendation in [ScalingRecommendation.SCALE_DOWN, ScalingRecommendation.OPTIMIZE]:
                    savings["clusters_to_optimize"] += 1
                    
                    # Estimate disk savings (simplified)
                    if "disk_usage" in recommendation.current_metrics:
                        disk_usage_gb = recommendation.current_metrics["disk_usage"] / 1_000_000_000
                        savings["disk_space_gb"] += disk_usage_gb * 0.2  # 20% savings estimate
            
            # Calculate total cluster disk usage
            total_cluster_disk = 0
            for cluster_usage in clusters_usage.values():
                disk_usage = cluster_usage.get("disk")
                if disk_usage:
                    total_cluster_disk += disk_usage.current_usage
            
            if total_cluster_disk > 0:
                savings["total_cluster_disk_gb"] = total_cluster_disk / 1_000_000_000
            
        except Exception as e:
            self.logger.error(f"Error calculating potential savings: {e}")
        
        return savings
    
    async def _cleanup_old_data(self) -> None:
        """Clean up old monitoring data."""
        try:
            cutoff_time = datetime.now() - timedelta(days=self.retention_days)
            
            # Clean up in-memory usage data
            for key in self._resource_usage:
                self._resource_usage[key] = [
                    usage for usage in self._resource_usage[key]
                    if usage.timestamp > cutoff_time
                ]
            
            # Clean up old data files
            for data_file in self.data_directory.glob("*.json"):
                if data_file.stat().st_mtime < cutoff_time.timestamp():
                    data_file.unlink()
                    self.logger.debug(f"Cleaned up old data file: {data_file}")
                    
        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {e}")


# Utility functions for resource management
def bytes_to_human_readable(bytes_value: int) -> str:
    """Convert bytes to human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"


def calculate_resource_efficiency(usage: ResourceUsage) -> float:
    """Calculate resource efficiency score (0.0 to 1.0)."""
    if not usage.limit or usage.limit == 0:
        return 1.0  # No limit means fully efficient
    
    utilization = usage.current_usage / usage.limit
    
    # Optimal utilization is around 70-80%
    if 0.7 <= utilization <= 0.8:
        return 1.0
    elif utilization < 0.7:
        return utilization / 0.7  # Underutilized
    else:
        return max(0.0, 1.0 - (utilization - 0.8) / 0.2)  # Overutilized


def get_resource_health_status(usage: ResourceUsage) -> str:
    """Get health status based on resource usage."""
    if not usage.limit:
        return "unknown"
    
    utilization = usage.current_usage / usage.limit
    
    if utilization < 0.7:
        return "healthy"
    elif utilization < 0.85:
        return "warning"
    elif utilization < 0.95:
        return "critical"
    else:
        return "emergency"