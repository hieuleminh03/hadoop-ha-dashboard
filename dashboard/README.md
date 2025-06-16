# Hadoop HA Dashboard

A comprehensive real-time monitoring and management dashboard for Hadoop High Availability clusters.

## Features

- **Real-time Monitoring**: Live cluster health monitoring with Server-Sent Events
- **HA Management**: Manual and automatic failover testing for NameNode and ResourceManager
- **File Management**: HDFS file browser with upload/download capabilities
- **Job Monitoring**: YARN application tracking and management
- **Performance Metrics**: Real-time charts and visualizations
- **Responsive Design**: Works on desktop, tablet, and mobile devices
- **Dark/Light Theme**: Toggle between themes

## Architecture

The dashboard consists of:
- **FastAPI Backend**: REST API and real-time data streaming
- **Vanilla JavaScript Frontend**: Responsive web interface
- **Real-time Updates**: Server-Sent Events for live monitoring
- **Hadoop Integration**: Direct integration with Hadoop REST APIs

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Hadoop cluster (configured with the main project)

### Installation

1. Build all images including the dashboard:
```bash
./Build.sh
```

2. Start the entire cluster with dashboard:
```bash
docker-compose up -d
```

3. Initialize the Hadoop cluster:
```bash
./Start.sh
```

4. Access the dashboard:
```
http://localhost:3000
```

## Dashboard Sections

### 1. Overview
- Cluster health status and score
- NameNode and ResourceManager HA status
- Resource utilization (Memory, vCores)
- Node health (DataNodes, NodeManagers, JournalNodes)
- Service status (History Server, Hive)
- Real-time performance charts

### 2. HA Controls
- Manual NameNode failover
- Manual ResourceManager failover
- Force failover options
- Failover history tracking
- Real-time status updates

### 3. File Manager
- Browse HDFS directory structure
- Upload files via drag & drop
- Download files from URLs to HDFS
- File operations (view, delete)
- Breadcrumb navigation

### 4. Job Monitor
- View running and completed YARN applications
- Application details and status
- Job execution timeline
- Resource allocation tracking

### 5. Logs
- Real-time cluster event logs
- Failover notifications
- Health status changes
- Auto-scroll functionality

## API Endpoints

### Cluster Status
- `GET /api/cluster/status` - Overall cluster status
- `GET /api/namenode/status` - NameNode HA status
- `GET /api/resourcemanager/status` - ResourceManager HA status
- `GET /api/nodes/health` - Node health status

### HA Operations
- `POST /api/ha/namenode/failover` - Trigger NameNode failover
- `POST /api/ha/resourcemanager/failover` - Trigger ResourceManager failover

### File Operations
- `GET /api/hdfs/browse?path=<path>` - Browse HDFS directory
- `POST /api/hdfs/upload` - Upload file to HDFS
- `POST /api/hdfs/download-url` - Download from URL to HDFS

### Monitoring
- `GET /api/yarn/applications` - Get YARN applications
- `GET /api/stream/metrics` - SSE metrics stream
- `GET /api/stream/logs` - SSE logs stream

## Configuration

### Environment Variables

The dashboard supports these environment variables:

```bash
HADOOP_NAMENODE_ACTIVE=active-nn:9870
HADOOP_NAMENODE_STANDBY=standby-nn:9870
HADOOP_RM_ACTIVE=active-rm:8088
HADOOP_RM_STANDBY=standby-rm:8088
HADOOP_HISTORYSERVER=historyserver:19888
HADOOP_HIVE=active-nn:10002
```

### Hadoop Endpoints Configuration

Edit `/config/hadoop_endpoints.json` to customize cluster endpoints:

```json
{
  "namenode": {
    "active": {"host": "active-nn", "port": 9870},
    "standby": {"host": "standby-nn", "port": 9870}
  },
  "resourcemanager": {
    "active": {"host": "active-rm", "port": 8088},
    "standby": {"host": "standby-rm", "port": 8088}
  }
}
```

## Development

### Local Development

1. Install dependencies:
```bash
cd dashboard
pip install -r requirements.txt
```

2. Run development server:
```bash
uvicorn app.main:app --host 0.0.0.0 --port 3000 --reload
```

3. Access at `http://localhost:3000`

### Project Structure

```
dashboard/
├── Dockerfile                  # Container configuration
├── requirements.txt           # Python dependencies
├── config/
│   └── hadoop_endpoints.json  # Cluster endpoints
├── app/
│   ├── main.py               # FastAPI application
│   ├── api/                  # API modules
│   │   ├── hadoop_client.py  # Hadoop REST client
│   │   ├── monitoring.py     # Background monitoring
│   │   └── failover.py       # Failover management
│   ├── static/               # Frontend assets
│   │   ├── css/
│   │   └── js/
│   └── templates/            # HTML templates
│       └── index.html
```

## Troubleshooting

### Common Issues

1. **Dashboard not accessible**
   - Check if container is running: `docker ps`
   - Verify port mapping: `docker-compose logs hadoop-dashboard`

2. **Connection errors to Hadoop services**
   - Ensure Hadoop cluster is fully started
   - Check network connectivity between containers
   - Verify Hadoop service endpoints

3. **Real-time updates not working**
   - Check browser console for SSE connection errors
   - Verify firewall/proxy settings
   - Refresh the page to reconnect

### Logs

View dashboard logs:
```bash
docker-compose logs -f hadoop-dashboard
```

## Demo Scenarios

### HA Failover Testing

1. **NameNode Failover**:
   - Navigate to HA Controls tab
   - Click "Trigger Failover" for NameNode
   - Watch real-time status changes in Overview tab
   - Verify service continuity

2. **ResourceManager Failover**:
   - Navigate to HA Controls tab
   - Click "Trigger Failover" for ResourceManager
   - Monitor application continuity in Job Monitor
   - Check failover history

### File Operations

1. **Upload Test Data**:
   - Go to File Manager tab
   - Upload sample files via drag & drop
   - Browse uploaded files
   - Verify replication across DataNodes

2. **Download from URL**:
   - Use "Download from URL" feature
   - Test with public datasets
   - Monitor download progress

## Security Notes

This dashboard is designed for development and demo purposes. For production use:

- Enable authentication and authorization
- Use HTTPS with proper certificates
- Restrict network access
- Enable audit logging
- Regular security updates

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is part of the Hadoop HA Cluster demo environment.