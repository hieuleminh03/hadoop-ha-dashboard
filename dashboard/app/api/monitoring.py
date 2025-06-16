import asyncio
import logging
from typing import Dict, List, Any
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)

class ClusterMonitor:
    """Background monitoring service for Hadoop cluster"""
    
    def __init__(self, hadoop_client):
        self.hadoop_client = hadoop_client
        self.monitoring = False
        self.current_metrics = {}
        self.metrics_history = []
        self.logs = []
        self.max_history_size = 100
        self.max_logs_size = 200
        
    async def start_monitoring(self):
        """Start background monitoring task"""
        if not self.monitoring:
            self.monitoring = True
            logger.info("Starting cluster monitoring...")
            asyncio.create_task(self._monitoring_loop())
    
    async def stop_monitoring(self):
        """Stop background monitoring"""
        self.monitoring = False
        logger.info("Stopping cluster monitoring...")
    
    async def _monitoring_loop(self):
        """Main monitoring loop"""
        while self.monitoring:
            try:
                # Collect metrics
                metrics = await self._collect_metrics()
                
                # Update current metrics
                self.current_metrics = metrics
                
                # Add to history
                self._add_to_history(metrics)
                
                # Generate log entries for significant events
                await self._check_for_events(metrics)
                
                # Wait before next collection
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(10)  # Wait longer on error
    
    async def _collect_metrics(self) -> Dict[str, Any]:
        """Collect comprehensive cluster metrics"""
        timestamp = datetime.now().isoformat()
        
        metrics = {
            "timestamp": timestamp,
            "cluster_health": {},
            "namenode_metrics": {},
            "resourcemanager_metrics": {},
            "node_metrics": {},
            "service_metrics": {},
            "performance_metrics": {}
        }
        
        try:
            # Get cluster status
            cluster_status = await self.hadoop_client.get_cluster_status()
            
            # Extract NameNode metrics
            nn_status = cluster_status.get("namenode", {})
            metrics["namenode_metrics"] = {
                "active_healthy": nn_status.get("active", {}).get("healthy", False),
                "standby_healthy": nn_status.get("standby", {}).get("healthy", False),
                "active_state": nn_status.get("active", {}).get("ha_state", "unknown"),
                "standby_state": nn_status.get("standby", {}).get("ha_state", "unknown"),
                "ha_enabled": nn_status.get("ha_enabled", False),
                "active_response_time": nn_status.get("active", {}).get("response_time", 0),
                "standby_response_time": nn_status.get("standby", {}).get("response_time", 0)
            }
            
            # Extract ResourceManager metrics
            rm_status = cluster_status.get("resourcemanager", {})
            metrics["resourcemanager_metrics"] = {
                "active_healthy": rm_status.get("active", {}).get("healthy", False),
                "standby_healthy": rm_status.get("standby", {}).get("healthy", False),
                "active_state": rm_status.get("active", {}).get("ha_state", "unknown"),
                "standby_state": rm_status.get("standby", {}).get("ha_state", "unknown"),
                "ha_enabled": rm_status.get("ha_enabled", False),
                "active_response_time": rm_status.get("active", {}).get("response_time", 0),
                "standby_response_time": rm_status.get("standby", {}).get("response_time", 0)
            }
            
            # Extract cluster metrics from ResourceManager
            if rm_status.get("active", {}).get("metrics"):
                rm_metrics = rm_status["active"]["metrics"]
                metrics["performance_metrics"] = {
                    "total_memory": rm_metrics.get("totalMB", 0),
                    "available_memory": rm_metrics.get("availableMB", 0),
                    "allocated_memory": rm_metrics.get("allocatedMB", 0),
                    "total_vcores": rm_metrics.get("totalVirtualCores", 0),
                    "available_vcores": rm_metrics.get("availableVirtualCores", 0),
                    "allocated_vcores": rm_metrics.get("allocatedVirtualCores", 0),
                    "active_nodes": rm_metrics.get("activeNodes", 0),
                    "decommissioned_nodes": rm_metrics.get("decommissionedNodes", 0),
                    "lost_nodes": rm_metrics.get("lostNodes", 0),
                    "unhealthy_nodes": rm_metrics.get("unhealthyNodes", 0),
                    "running_apps": rm_metrics.get("appsPending", 0) + rm_metrics.get("appsRunning", 0),
                    "pending_apps": rm_metrics.get("appsPending", 0)
                }
            
            # Extract node health metrics
            nodes_health = cluster_status.get("nodes", {})
            metrics["node_metrics"] = {
                "total_datanodes": len(nodes_health.get("datanodes", [])),
                "healthy_datanodes": len([dn for dn in nodes_health.get("datanodes", []) if dn.get("state") == "NORMAL"]),
                "total_nodemanagers": len(nodes_health.get("nodemanagers", [])),
                "healthy_nodemanagers": len([nm for nm in nodes_health.get("nodemanagers", []) if nm.get("state") == "RUNNING"]),
                "total_journalnodes": len(nodes_health.get("journalnodes", [])),
                "healthy_journalnodes": len([jn for jn in nodes_health.get("journalnodes", []) if jn.get("healthy", False)])
            }
            
            # Extract service metrics
            services = cluster_status.get("services", {})
            metrics["service_metrics"] = {
                "historyserver_healthy": services.get("historyserver", {}).get("healthy", False),
                "hive_healthy": services.get("hive", {}).get("healthy", False),
                "historyserver_response_time": services.get("historyserver", {}).get("response_time", 0),
                "hive_response_time": services.get("hive", {}).get("response_time", 0)
            }
            
            # Calculate overall cluster health
            metrics["cluster_health"] = self._calculate_cluster_health(metrics)
            
        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
            metrics["error"] = str(e)
        
        return metrics
    
    def _calculate_cluster_health(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate overall cluster health score"""
        health_score = 0
        max_score = 100
        
        # NameNode health (25 points)
        nn_metrics = metrics.get("namenode_metrics", {})
        if nn_metrics.get("active_healthy") and nn_metrics.get("active_state") == "active":
            health_score += 15
        if nn_metrics.get("standby_healthy") and nn_metrics.get("standby_state") == "standby":
            health_score += 10
        
        # ResourceManager health (25 points)
        rm_metrics = metrics.get("resourcemanager_metrics", {})
        if rm_metrics.get("active_healthy") and rm_metrics.get("active_state") == "active":
            health_score += 15
        if rm_metrics.get("standby_healthy") and rm_metrics.get("standby_state") == "standby":
            health_score += 10
        
        # Node health (30 points)
        node_metrics = metrics.get("node_metrics", {})
        total_datanodes = node_metrics.get("total_datanodes", 1)
        healthy_datanodes = node_metrics.get("healthy_datanodes", 0)
        if total_datanodes > 0:
            health_score += int((healthy_datanodes / total_datanodes) * 15)
        
        total_nodemanagers = node_metrics.get("total_nodemanagers", 1)
        healthy_nodemanagers = node_metrics.get("healthy_nodemanagers", 0)
        if total_nodemanagers > 0:
            health_score += int((healthy_nodemanagers / total_nodemanagers) * 15)
        
        # Service health (20 points)
        service_metrics = metrics.get("service_metrics", {})
        if service_metrics.get("historyserver_healthy"):
            health_score += 10
        if service_metrics.get("hive_healthy"):
            health_score += 10
        
        # Determine health status
        health_percentage = (health_score / max_score) * 100
        
        if health_percentage >= 90:
            status = "excellent"
        elif health_percentage >= 75:
            status = "good"
        elif health_percentage >= 50:
            status = "warning"
        else:
            status = "critical"
        
        return {
            "score": health_score,
            "max_score": max_score,
            "percentage": health_percentage,
            "status": status,
            "issues": self._identify_issues(metrics)
        }
    
    def _identify_issues(self, metrics: Dict[str, Any]) -> List[str]:
        """Identify specific cluster issues"""
        issues = []
        
        # Check NameNode issues
        nn_metrics = metrics.get("namenode_metrics", {})
        if not nn_metrics.get("active_healthy"):
            issues.append("Active NameNode is unhealthy")
        if not nn_metrics.get("standby_healthy"):
            issues.append("Standby NameNode is unhealthy")
        if nn_metrics.get("active_state") != "active":
            issues.append(f"Active NameNode is in '{nn_metrics.get('active_state')}' state")
        if nn_metrics.get("standby_state") != "standby":
            issues.append(f"Standby NameNode is in '{nn_metrics.get('standby_state')}' state")
        
        # Check ResourceManager issues
        rm_metrics = metrics.get("resourcemanager_metrics", {})
        if not rm_metrics.get("active_healthy"):
            issues.append("Active ResourceManager is unhealthy")
        if not rm_metrics.get("standby_healthy"):
            issues.append("Standby ResourceManager is unhealthy")
        if rm_metrics.get("active_state") != "active":
            issues.append(f"Active ResourceManager is in '{rm_metrics.get('active_state')}' state")
        if rm_metrics.get("standby_state") != "standby":
            issues.append(f"Standby ResourceManager is in '{rm_metrics.get('standby_state')}' state")
        
        # Check node issues
        node_metrics = metrics.get("node_metrics", {})
        unhealthy_datanodes = node_metrics.get("total_datanodes", 0) - node_metrics.get("healthy_datanodes", 0)
        if unhealthy_datanodes > 0:
            issues.append(f"{unhealthy_datanodes} DataNode(s) are unhealthy")
        
        unhealthy_nodemanagers = node_metrics.get("total_nodemanagers", 0) - node_metrics.get("healthy_nodemanagers", 0)
        if unhealthy_nodemanagers > 0:
            issues.append(f"{unhealthy_nodemanagers} NodeManager(s) are unhealthy")
        
        unhealthy_journalnodes = node_metrics.get("total_journalnodes", 0) - node_metrics.get("healthy_journalnodes", 0)
        if unhealthy_journalnodes > 0:
            issues.append(f"{unhealthy_journalnodes} JournalNode(s) are unhealthy")
        
        # Check service issues
        service_metrics = metrics.get("service_metrics", {})
        if not service_metrics.get("historyserver_healthy"):
            issues.append("History Server is unhealthy")
        if not service_metrics.get("hive_healthy"):
            issues.append("Hive WebUI is unhealthy")
        
        # Check performance issues
        perf_metrics = metrics.get("performance_metrics", {})
        if perf_metrics.get("total_memory", 0) > 0:
            memory_usage = (perf_metrics.get("allocated_memory", 0) / perf_metrics.get("total_memory", 1)) * 100
            if memory_usage > 90:
                issues.append(f"High memory usage: {memory_usage:.1f}%")
        
        if perf_metrics.get("pending_apps", 0) > 5:
            issues.append(f"{perf_metrics.get('pending_apps')} applications are pending")
        
        return issues
    
    def _add_to_history(self, metrics: Dict[str, Any]):
        """Add metrics to history with size limit"""
        self.metrics_history.append(metrics)
        
        # Keep only recent history
        if len(self.metrics_history) > self.max_history_size:
            self.metrics_history = self.metrics_history[-self.max_history_size:]
    
    async def _check_for_events(self, metrics: Dict[str, Any]):
        """Check for significant events and log them"""
        if len(self.metrics_history) < 2:
            return
        
        current = metrics
        previous = self.metrics_history[-2]
        timestamp = datetime.now().isoformat()
        
        # Check for HA state changes
        await self._check_ha_state_changes(current, previous, timestamp)
        
        # Check for health changes
        await self._check_health_changes(current, previous, timestamp)
        
        # Check for performance issues
        await self._check_performance_issues(current, timestamp)
    
    async def _check_ha_state_changes(self, current: Dict, previous: Dict, timestamp: str):
        """Check for HA state changes"""
        # NameNode state changes
        curr_nn = current.get("namenode_metrics", {})
        prev_nn = previous.get("namenode_metrics", {})
        
        if curr_nn.get("active_state") != prev_nn.get("active_state"):
            self._add_log("warning", f"NameNode active state changed: {prev_nn.get('active_state')} → {curr_nn.get('active_state')}", timestamp)
        
        if curr_nn.get("standby_state") != prev_nn.get("standby_state"):
            self._add_log("warning", f"NameNode standby state changed: {prev_nn.get('standby_state')} → {curr_nn.get('standby_state')}", timestamp)
        
        # ResourceManager state changes
        curr_rm = current.get("resourcemanager_metrics", {})
        prev_rm = previous.get("resourcemanager_metrics", {})
        
        if curr_rm.get("active_state") != prev_rm.get("active_state"):
            self._add_log("warning", f"ResourceManager active state changed: {prev_rm.get('active_state')} → {curr_rm.get('active_state')}", timestamp)
        
        if curr_rm.get("standby_state") != prev_rm.get("standby_state"):
            self._add_log("warning", f"ResourceManager standby state changed: {prev_rm.get('standby_state')} → {curr_rm.get('standby_state')}", timestamp)
    
    async def _check_health_changes(self, current: Dict, previous: Dict, timestamp: str):
        """Check for health status changes"""
        curr_health = current.get("cluster_health", {})
        prev_health = previous.get("cluster_health", {})
        
        curr_status = curr_health.get("status")
        prev_status = prev_health.get("status")
        
        if curr_status != prev_status:
            level = "error" if curr_status in ["warning", "critical"] else "info"
            self._add_log(level, f"Cluster health status changed: {prev_status} → {curr_status}", timestamp)
        
        # Check for new issues
        curr_issues = set(curr_health.get("issues", []))
        prev_issues = set(prev_health.get("issues", []))
        
        new_issues = curr_issues - prev_issues
        resolved_issues = prev_issues - curr_issues
        
        for issue in new_issues:
            self._add_log("warning", f"New issue detected: {issue}", timestamp)
        
        for issue in resolved_issues:
            self._add_log("info", f"Issue resolved: {issue}", timestamp)
    
    async def _check_performance_issues(self, current: Dict, timestamp: str):
        """Check for performance issues"""
        perf_metrics = current.get("performance_metrics", {})
        
        # High memory usage
        if perf_metrics.get("total_memory", 0) > 0:
            memory_usage = (perf_metrics.get("allocated_memory", 0) / perf_metrics.get("total_memory", 1)) * 100
            if memory_usage > 95:
                self._add_log("error", f"Critical memory usage: {memory_usage:.1f}%", timestamp)
            elif memory_usage > 85:
                self._add_log("warning", f"High memory usage: {memory_usage:.1f}%", timestamp)
        
        # High pending applications
        pending_apps = perf_metrics.get("pending_apps", 0)
        if pending_apps > 10:
            self._add_log("warning", f"High number of pending applications: {pending_apps}", timestamp)
    
    def _add_log(self, level: str, message: str, timestamp: str):
        """Add log entry"""
        log_entry = {
            "timestamp": timestamp,
            "level": level,
            "message": message
        }
        
        self.logs.append(log_entry)
        
        # Keep only recent logs
        if len(self.logs) > self.max_logs_size:
            self.logs = self.logs[-self.max_logs_size:]
        
        # Also log to system logger
        if level == "error":
            logger.error(message)
        elif level == "warning":
            logger.warning(message)
        else:
            logger.info(message)
    
    async def get_current_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        return self.current_metrics.copy() if self.current_metrics else {}
    
    async def get_metrics_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get metrics history"""
        return self.metrics_history[-limit:] if self.metrics_history else []
    
    async def get_recent_logs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent logs"""
        return self.logs[-limit:] if self.logs else []