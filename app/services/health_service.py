from dataclasses import dataclass, asdict
from datetime import datetime
import psutil
import asyncio
from typing import Dict, Any, Optional
import gc
import logging
from app.models.health import ServiceHealth, DetailedMetrics
from app.services.llm_service import LLMService
from app.services.db_service import DatabaseManager

logger = logging.getLogger(__name__)

@dataclass
class DetailedMetrics:
    system: Dict[str, Any]
    application: Dict[str, Any]
    database: Dict[str, Any]
    api: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the metrics to a dictionary."""
        return asdict(self)

@dataclass
class ServiceHealth:
    status: str
    last_check: datetime
    metrics: DetailedMetrics
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the health status to a dictionary."""
        return {
            "status": self.status,
            "last_check": self.last_check.isoformat(),
            "metrics": self.metrics.to_dict()
        }

class HealthService:
    def __init__(self, llm_service: LLMService, db_manager: DatabaseManager):
        self.last_health_check = None
        self.health_cache_ttl = 60
        self.llm_service = llm_service
        self.db_manager = db_manager
        # Initialize default metrics
        self._default_metrics = {
            "requests_per_minute": 0.0,
            "average_response_time": 0.0,
            "successful_requests": 0,
            "total_requests": 0,
            "failed_requests": 0
        }
        
        # Start background health monitor
        self.monitor_task = asyncio.create_task(self._monitor_health())
    
    async def _monitor_health(self):
        """Continuous health monitoring."""
        while True:
            try:
                health = await self.check_system_health()
                if health.status == "degraded":
                    logger.warning("System health degraded", extra=health.metrics.__dict__)
                elif health.status == "unhealthy":
                    logger.error("System unhealthy", extra=health.metrics.__dict__)
                    
                await asyncio.sleep(self.health_cache_ttl)
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                await asyncio.sleep(5)
    
    async def check_system_health(self) -> ServiceHealth:
        """Enhanced health check with detailed metrics."""
        if (self.last_health_check and 
            (datetime.now() - self.last_health_check.last_check).seconds < self.health_cache_ttl):
            return self.last_health_check
        
        try:
            # Get metrics with fallback to defaults
            metrics = getattr(self.llm_service, 'metrics', self._default_metrics)
            
            # Calculate success rate safely
            total_requests = metrics.get("total_requests", 0)
            success_rate = (metrics.get("successful_requests", 0) / total_requests * 100) if total_requests > 0 else 100
            
            app_metrics = {
                "success_rate": success_rate,
                "total_requests": total_requests,
                "requests_per_minute": metrics.get("requests_per_minute", 0.0),
                "average_response_time": metrics.get("average_response_time", 0.0)
            }
            
            # System metrics
            system_metrics = {
                "cpu_percent": psutil.cpu_percent(interval=1),
                "memory_percent": psutil.virtual_memory().percent,
                "swap_percent": psutil.swap_memory().percent,
                "disk_usage": psutil.disk_usage('/').percent,
                "open_files": len(psutil.Process().open_files()),
                "threads": len(psutil.Process().threads()),
                "connections": len(psutil.Process().connections()),
                "gc_counts": gc.get_count(),
                "gc_threshold": gc.get_threshold()
            }
            
            # Database metrics
            db_metrics = {
                "pool_size": len(self.db_manager.pool._pool),
                "circuit_breaker_state": self.llm_service.db_circuit_breaker.state,
                "cache_size": len(self.db_manager._cache)
            }
            
            # API metrics
            api_metrics = {
                "circuit_breaker_state": self.llm_service.api_circuit_breaker.state,
                "current_requests": self.llm_service.max_concurrent_requests - self.llm_service.rate_limiter._value,
                "batch_queue_size": len(self.llm_service.background_queue._queue)
            }
            
            detailed_metrics = DetailedMetrics(
                system=system_metrics,
                application=app_metrics,
                database=db_metrics,
                api=api_metrics
            )
            
            # Updated health status determination logic with more conservative thresholds
            status = "healthy"
            
            # Check for degraded state
            if any([
                system_metrics["cpu_percent"] > 80,  # Reduced from 90
                system_metrics["memory_percent"] > 80,  # Reduced from 90
                system_metrics["disk_usage"] > 85,  # Reduced from 90
                app_metrics["success_rate"] < 97 and app_metrics["total_requests"] > 0  # Increased from 95
            ]):
                status = "degraded"
                logger.warning(f"System degraded: Memory {system_metrics['memory_percent']}%, CPU {system_metrics['cpu_percent']}%")
            
            # Check for unhealthy state
            if any([
                system_metrics["cpu_percent"] > 90,  # Reduced from 95
                system_metrics["memory_percent"] > 90,  # Reduced from 95
                system_metrics["disk_usage"] > 90,  # Reduced from 95
                app_metrics["success_rate"] < 95 and app_metrics["total_requests"] > 0,  # Increased from 90
                db_metrics["circuit_breaker_state"] == "open",
                api_metrics["circuit_breaker_state"] == "open"
            ]):
                status = "unhealthy"
                logger.error(f"System unhealthy: Memory {system_metrics['memory_percent']}%, CPU {system_metrics['cpu_percent']}%")
                
                # Trigger emergency cleanup if memory is critically high
                if system_metrics["memory_percent"] > 90:
                    logger.error("Memory critically high, triggering emergency cleanup")
                    gc.collect()
            
            # For startup phase: if no requests yet, consider system healthy
            if app_metrics["total_requests"] == 0:
                status = "healthy"
            
            self.last_health_check = ServiceHealth(
                status=status,
                last_check=datetime.now(),
                metrics=detailed_metrics
            )
            
            return self.last_health_check
            
        except Exception as e:
            logger.error(f"Health check error: {e}")
            return ServiceHealth(
                status="unhealthy",
                last_check=datetime.now(),
                metrics=DetailedMetrics(
                    system={"error": str(e)},
                    application={},
                    database={},
                    api={}
                )
            ) 