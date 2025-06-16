import httpx
import json
import logging
from typing import Dict, List, Optional, Any
from fastapi import UploadFile
import asyncio
from pathlib import Path

logger = logging.getLogger(__name__)

class HadoopClient:
    """Client for interacting with Hadoop cluster APIs"""
    
    def __init__(self):
        self.config = self._load_config()
        self.client = httpx.AsyncClient(timeout=30.0)
        
    def _load_config(self) -> Dict:
        """Load Hadoop endpoints configuration"""
        config_path = Path("config/hadoop_endpoints.json")
        with open(config_path, 'r') as f:
            return json.load(f)
    
    async def get_cluster_status(self) -> Dict[str, Any]:
        """Get overall cluster status"""
        status = {
            "timestamp": asyncio.get_event_loop().time(),
            "namenode": await self.get_namenode_status(),
            "resourcemanager": await self.get_resourcemanager_status(),
            "nodes": await self.get_nodes_health(),
            "services": await self.get_services_status()
        }
        return status
    
    async def get_namenode_status(self) -> Dict[str, Any]:
        """Get NameNode HA status"""
        namenode_config = self.config["namenode"]
        active_url = f"http://{namenode_config['active']['host']}:{namenode_config['active']['port']}"
        standby_url = f"http://{namenode_config['standby']['host']}:{namenode_config['standby']['port']}"
        
        # Check active NameNode
        active_status = await self._check_namenode_health(active_url, "active")
        
        # Check standby NameNode
        standby_status = await self._check_namenode_health(standby_url, "standby")
        
        return {
            "active": active_status,
            "standby": standby_status,
            "ha_enabled": True
        }
    
    async def _check_namenode_health(self, base_url: str, expected_role: str) -> Dict[str, Any]:
        """Check health of a specific NameNode"""
        try:
            # Get JMX metrics
            jmx_response = await self.client.get(f"{base_url}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus")
            jmx_data = jmx_response.json()
            
            # Get basic info
            info_response = await self.client.get(f"{base_url}/ws/v1/cluster/info")
            info_data = info_response.json()
            
            # Extract HA state
            ha_state = "unknown"
            if jmx_data.get("beans"):
                for bean in jmx_data["beans"]:
                    if "State" in bean:
                        ha_state = bean["State"].lower()
                        break
            
            return {
                "url": base_url,
                "healthy": True,
                "ha_state": ha_state,
                "is_active": ha_state == "active",
                "expected_role": expected_role,
                "role_correct": ha_state == expected_role,
                "response_time": jmx_response.elapsed.total_seconds(),
                "cluster_info": info_data.get("clusterInfo", {})
            }
            
        except Exception as e:
            logger.error(f"Error checking NameNode health at {base_url}: {e}")
            return {
                "url": base_url,
                "healthy": False,
                "ha_state": "unreachable",
                "is_active": False,
                "expected_role": expected_role,
                "role_correct": False,
                "error": str(e)
            }
    
    async def get_resourcemanager_status(self) -> Dict[str, Any]:
        """Get ResourceManager HA status"""
        rm_config = self.config["resourcemanager"]
        active_url = f"http://{rm_config['active']['host']}:{rm_config['active']['port']}"
        standby_url = f"http://{rm_config['standby']['host']}:{rm_config['standby']['port']}"
        
        # Check active ResourceManager
        active_status = await self._check_resourcemanager_health(active_url, "active")
        
        # Check standby ResourceManager  
        standby_status = await self._check_resourcemanager_health(standby_url, "standby")
        
        return {
            "active": active_status,
            "standby": standby_status,
            "ha_enabled": True
        }
    
    async def _check_resourcemanager_health(self, base_url: str, expected_role: str) -> Dict[str, Any]:
        """Check health of a specific ResourceManager"""
        try:
            # Get cluster info
            info_response = await self.client.get(f"{base_url}/ws/v1/cluster/info")
            info_data = info_response.json()
            
            # Get cluster metrics
            metrics_response = await self.client.get(f"{base_url}/ws/v1/cluster/metrics")
            metrics_data = metrics_response.json()
            
            ha_state = info_data.get("clusterInfo", {}).get("haState", "unknown").lower()
            
            return {
                "url": base_url,
                "healthy": True,
                "ha_state": ha_state,
                "is_active": ha_state == "active",
                "expected_role": expected_role,
                "role_correct": ha_state == expected_role,
                "response_time": info_response.elapsed.total_seconds(),
                "cluster_info": info_data.get("clusterInfo", {}),
                "metrics": metrics_data.get("clusterMetrics", {})
            }
            
        except Exception as e:
            logger.error(f"Error checking ResourceManager health at {base_url}: {e}")
            return {
                "url": base_url,
                "healthy": False,
                "ha_state": "unreachable",
                "is_active": False,
                "expected_role": expected_role,
                "role_correct": False,
                "error": str(e)
            }
    
    async def get_nodes_health(self) -> Dict[str, Any]:
        """Get health status of all cluster nodes"""
        nodes_health = {
            "datanodes": [],
            "nodemanagers": [],
            "journalnodes": []
        }
        
        # Check DataNodes via active NameNode
        try:
            namenode_config = self.config["namenode"]["active"]
            nn_url = f"http://{namenode_config['host']}:{namenode_config['port']}"
            
            # Get DataNode info
            dn_response = await self.client.get(f"{nn_url}/ws/v1/cluster/nodes")
            if dn_response.status_code == 200:
                dn_data = dn_response.json()
                nodes_health["datanodes"] = dn_data.get("nodes", {}).get("node", [])
        except Exception as e:
            logger.error(f"Error getting DataNode health: {e}")
        
        # Check NodeManagers via active ResourceManager
        try:
            rm_config = self.config["resourcemanager"]["active"]
            rm_url = f"http://{rm_config['host']}:{rm_config['port']}"
            
            # Get NodeManager info
            nm_response = await self.client.get(f"{rm_url}/ws/v1/cluster/nodes")
            if nm_response.status_code == 200:
                nm_data = nm_response.json()
                nodes_health["nodemanagers"] = nm_data.get("nodes", {}).get("node", [])
        except Exception as e:
            logger.error(f"Error getting NodeManager health: {e}")
        
        # Check JournalNodes
        for jn_config in self.config["journalnodes"]:
            try:
                jn_url = f"http://{jn_config['host']}:{jn_config['port']}"
                jn_response = await self.client.get(f"{jn_url}/jmx")
                
                jn_health = {
                    "host": jn_config["host"],
                    "port": jn_config["port"],
                    "healthy": jn_response.status_code == 200,
                    "response_time": jn_response.elapsed.total_seconds()
                }
                
                if jn_response.status_code == 200:
                    jn_health["metrics"] = jn_response.json()
                    
                nodes_health["journalnodes"].append(jn_health)
                
            except Exception as e:
                logger.error(f"Error checking JournalNode {jn_config['host']}: {e}")
                nodes_health["journalnodes"].append({
                    "host": jn_config["host"],
                    "port": jn_config["port"],
                    "healthy": False,
                    "error": str(e)
                })
        
        return nodes_health
    
    async def get_services_status(self) -> Dict[str, Any]:
        """Get status of additional services"""
        services = {}
        
        # Check History Server
        try:
            hs_config = self.config["historyserver"]
            hs_url = f"http://{hs_config['host']}:{hs_config['port']}"
            
            hs_response = await self.client.get(f"{hs_url}/ws/v1/history/info")
            services["historyserver"] = {
                "healthy": hs_response.status_code == 200,
                "url": hs_url,
                "response_time": hs_response.elapsed.total_seconds()
            }
            
            if hs_response.status_code == 200:
                services["historyserver"]["info"] = hs_response.json()
                
        except Exception as e:
            logger.error(f"Error checking History Server: {e}")
            services["historyserver"] = {
                "healthy": False,
                "error": str(e)
            }
        
        # Check Hive WebUI
        try:
            hive_config = self.config["hive"]
            hive_url = f"http://{hive_config['host']}:{hive_config['port']}"
            
            hive_response = await self.client.get(hive_url)
            services["hive"] = {
                "healthy": hive_response.status_code == 200,
                "url": hive_url,
                "response_time": hive_response.elapsed.total_seconds()
            }
            
        except Exception as e:
            logger.error(f"Error checking Hive WebUI: {e}")
            services["hive"] = {
                "healthy": False,
                "error": str(e)
            }
        
        return services
    
    async def browse_hdfs(self, path: str = "/") -> Dict[str, Any]:
        """Browse HDFS directory using WebHDFS API"""
        try:
            namenode_config = self.config["namenode"]["active"]
            webhdfs_url = f"http://{namenode_config['host']}:{namenode_config['port']}/webhdfs/v1"
            
            # List directory contents
            response = await self.client.get(
                f"{webhdfs_url}{path}",
                params={"op": "LISTSTATUS"}
            )
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "path": path,
                    "contents": data.get("FileStatuses", {}).get("FileStatus", [])
                }
            else:
                raise Exception(f"WebHDFS error: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Error browsing HDFS path {path}: {e}")
            raise e
    
    async def upload_file(self, file: UploadFile, hdfs_path: str) -> Dict[str, Any]:
        """Upload file to HDFS using WebHDFS API"""
        try:
            namenode_config = self.config["namenode"]["active"]
            webhdfs_url = f"http://{namenode_config['host']}:{namenode_config['port']}/webhdfs/v1"
            
            # Create file path
            full_path = f"{hdfs_path.rstrip('/')}/{file.filename}"
            
            # Step 1: Create file (get redirect URL)
            create_response = await self.client.put(
                f"{webhdfs_url}{full_path}",
                params={"op": "CREATE", "overwrite": "true"},
                allow_redirects=False
            )
            
            if create_response.status_code != 307:
                raise Exception(f"Failed to get redirect URL: {create_response.status_code}")
            
            # Step 2: Upload to DataNode
            redirect_url = create_response.headers["Location"]
            file_content = await file.read()
            
            upload_response = await self.client.put(
                redirect_url,
                content=file_content,
                headers={"Content-Type": "application/octet-stream"}
            )
            
            if upload_response.status_code == 201:
                return {
                    "success": True,
                    "path": full_path,
                    "size": len(file_content),
                    "filename": file.filename
                }
            else:
                raise Exception(f"Upload failed: {upload_response.status_code}")
                
        except Exception as e:
            logger.error(f"Error uploading file to HDFS: {e}")
            raise e
    
    async def download_url_to_hdfs(self, url: str, hdfs_path: str) -> Dict[str, Any]:
        """Download file from URL and save to HDFS"""
        try:
            # Download file from URL
            download_response = await self.client.get(url)
            download_response.raise_for_status()
            
            # Extract filename from URL
            filename = url.split("/")[-1] or "downloaded_file"
            
            namenode_config = self.config["namenode"]["active"]
            webhdfs_url = f"http://{namenode_config['host']}:{namenode_config['port']}/webhdfs/v1"
            
            # Create full HDFS path
            full_path = f"{hdfs_path.rstrip('/')}/{filename}"
            
            # Step 1: Create file
            create_response = await self.client.put(
                f"{webhdfs_url}{full_path}",
                params={"op": "CREATE", "overwrite": "true"},
                allow_redirects=False
            )
            
            if create_response.status_code != 307:
                raise Exception(f"Failed to get redirect URL: {create_response.status_code}")
            
            # Step 2: Upload content
            redirect_url = create_response.headers["Location"]
            upload_response = await self.client.put(
                redirect_url,
                content=download_response.content,
                headers={"Content-Type": "application/octet-stream"}
            )
            
            if upload_response.status_code == 201:
                return {
                    "success": True,
                    "source_url": url,
                    "hdfs_path": full_path,
                    "size": len(download_response.content),
                    "filename": filename
                }
            else:
                raise Exception(f"Upload to HDFS failed: {upload_response.status_code}")
                
        except Exception as e:
            logger.error(f"Error downloading URL to HDFS: {e}")
            raise e
    
    async def get_yarn_applications(self) -> Dict[str, Any]:
        """Get YARN applications"""
        try:
            rm_config = self.config["resourcemanager"]["active"]
            rm_url = f"http://{rm_config['host']}:{rm_config['port']}"
            
            # Get applications
            apps_response = await self.client.get(f"{rm_url}/ws/v1/cluster/apps")
            
            if apps_response.status_code == 200:
                return apps_response.json()
            else:
                raise Exception(f"Failed to get YARN applications: {apps_response.status_code}")
                
        except Exception as e:
            logger.error(f"Error getting YARN applications: {e}")
            raise e
    
    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()