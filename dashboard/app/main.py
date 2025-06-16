from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from sse_starlette.sse import EventSourceResponse
import json
import asyncio
from typing import Optional
import logging

from api.hadoop_client import HadoopClient
from api.monitoring import ClusterMonitor
from api.failover import FailoverManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Hadoop HA Dashboard",
    description="Real-time monitoring and failover testing for Hadoop High Availability cluster",
    version="1.0.0"
)

# Mount static files and templates
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# Initialize services
hadoop_client = HadoopClient()
cluster_monitor = ClusterMonitor(hadoop_client)
failover_manager = FailoverManager(hadoop_client)

# Global variables for SSE
connected_clients = set()

@app.on_event("startup")
async def startup_event():
    """Initialize monitoring on startup"""
    logger.info("Starting Hadoop HA Dashboard...")
    # Start background monitoring task
    asyncio.create_task(cluster_monitor.start_monitoring())

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down Hadoop HA Dashboard...")
    await cluster_monitor.stop_monitoring()

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard page"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "hadoop-dashboard"}

# API Routes
@app.get("/api/cluster/status")
async def get_cluster_status():
    """Get overall cluster status"""
    try:
        status = await hadoop_client.get_cluster_status()
        return status
    except Exception as e:
        logger.error(f"Error getting cluster status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/namenode/status")
async def get_namenode_status():
    """Get NameNode HA status"""
    try:
        status = await hadoop_client.get_namenode_status()
        return status
    except Exception as e:
        logger.error(f"Error getting NameNode status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/resourcemanager/status")
async def get_resourcemanager_status():
    """Get ResourceManager HA status"""
    try:
        status = await hadoop_client.get_resourcemanager_status()
        return status
    except Exception as e:
        logger.error(f"Error getting ResourceManager status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/nodes/health")
async def get_nodes_health():
    """Get health status of all nodes"""
    try:
        health = await hadoop_client.get_nodes_health()
        return health
    except Exception as e:
        logger.error(f"Error getting nodes health: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/ha/namenode/failover")
async def namenode_failover():
    """Trigger NameNode failover"""
    try:
        result = await failover_manager.namenode_failover()
        return result
    except Exception as e:
        logger.error(f"Error during NameNode failover: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/ha/resourcemanager/failover")
async def resourcemanager_failover():
    """Trigger ResourceManager failover"""
    try:
        result = await failover_manager.resourcemanager_failover()
        return result
    except Exception as e:
        logger.error(f"Error during ResourceManager failover: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/hdfs/browse")
async def browse_hdfs(path: str = "/"):
    """Browse HDFS directory"""
    try:
        contents = await hadoop_client.browse_hdfs(path)
        return contents
    except Exception as e:
        logger.error(f"Error browsing HDFS path {path}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/hdfs/upload")
async def upload_file(file: UploadFile = File(...), path: str = "/tmp"):
    """Upload file to HDFS"""
    try:
        result = await hadoop_client.upload_file(file, path)
        return result
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/hdfs/download-url")
async def download_url_to_hdfs(url: str, hdfs_path: str):
    """Download file from URL to HDFS"""
    try:
        result = await hadoop_client.download_url_to_hdfs(url, hdfs_path)
        return result
    except Exception as e:
        logger.error(f"Error downloading URL to HDFS: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/yarn/applications")
async def get_yarn_applications():
    """Get YARN applications"""
    try:
        apps = await hadoop_client.get_yarn_applications()
        return apps
    except Exception as e:
        logger.error(f"Error getting YARN applications: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stream/metrics")
async def stream_metrics(request: Request):
    """Server-Sent Events stream for real-time metrics"""
    async def event_generator():
        try:
            # Add client to connected clients
            client_id = id(request)
            connected_clients.add(client_id)
            
            while True:
                # Check if client is still connected
                if await request.is_disconnected():
                    break
                
                # Get latest metrics
                metrics = await cluster_monitor.get_current_metrics()
                
                # Send metrics as SSE
                yield {
                    "event": "metrics",
                    "data": json.dumps(metrics)
                }
                
                # Wait before next update
                await asyncio.sleep(5)
                
        except asyncio.CancelledError:
            logger.info(f"Client {client_id} disconnected")
        finally:
            # Remove client from connected clients
            connected_clients.discard(client_id)
    
    return EventSourceResponse(event_generator())

@app.get("/api/stream/logs")
async def stream_logs(request: Request):
    """Server-Sent Events stream for real-time logs"""
    async def log_generator():
        try:
            client_id = id(request)
            connected_clients.add(client_id)
            
            while True:
                if await request.is_disconnected():
                    break
                
                # Get latest logs
                logs = await cluster_monitor.get_recent_logs()
                
                yield {
                    "event": "logs",
                    "data": json.dumps(logs)
                }
                
                await asyncio.sleep(2)
                
        except asyncio.CancelledError:
            logger.info(f"Log client {client_id} disconnected")
        finally:
            connected_clients.discard(client_id)
    
    return EventSourceResponse(log_generator())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000)