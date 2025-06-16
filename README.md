# Hadoop HA Dashboard

Real-time monitoring dashboard for Hadoop High Availability clusters.

## Features

- Real-time HA monitoring
- Interactive failover testing
- HDFS file management
- YARN job tracking
- Performance metrics

## Quick Start

```bash
./Build.sh
docker-compose up -d
./Start.sh
```

Access: http://localhost:3000

## Tech Stack

- FastAPI backend
- Vanilla JavaScript frontend
- Server-Sent Events
- Hadoop REST APIs
- Docker Compose

## Services

| Component | Port |
|-----------|------|
| Dashboard | 3000 |
| Active NameNode | 9870 |
| Standby NameNode | 9871 |
| Active ResourceManager | 8088 |
| Standby ResourceManager | 8089 |
| History Server | 19888 |

## Development

```bash
cd dashboard
pip install -r requirements.txt
uvicorn app.main:app --reload --port 3000
```