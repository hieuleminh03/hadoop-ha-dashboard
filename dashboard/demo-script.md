# Hadoop HA Dashboard Demo Script

This script provides a step-by-step demonstration of the Hadoop HA Dashboard features.

## Pre-Demo Setup

1. **Start the cluster**:
```bash
# Build all images
./Build.sh

# Start containers
docker-compose up -d

# Initialize Hadoop cluster
./Start.sh
```

2. **Verify services are running**:
```bash
# Check container status
docker ps

# Verify Hadoop services
curl -s http://localhost:9870/jmx | grep -o '"State":"[^"]*"'
curl -s http://localhost:8088/ws/v1/cluster/info | grep -o '"haState":"[^"]*"'
```

3. **Access the dashboard**:
   - Open browser: `http://localhost:3000`
   - Allow 30-60 seconds for initial data loading

## Demo Flow

### 1. Overview Dashboard (5 minutes)

**Talking Points:**
- "This is the main overview showing our Hadoop HA cluster status"
- "We can see cluster health score and real-time metrics"

**Demo Actions:**
1. **Cluster Health**: Point out the overall health score in top-right
2. **NameNode HA**: Show active/standby states with green/yellow indicators
3. **ResourceManager HA**: Demonstrate dual RM setup
4. **Resource Utilization**: Explain memory and vCore usage bars
5. **Node Health**: Show DataNodes, NodeManagers, JournalNodes status
6. **Performance Chart**: Highlight real-time metrics streaming
7. **Theme Toggle**: Switch between light/dark themes

**Key Features to Highlight:**
- Real-time updates (point out changing timestamps)
- Color-coded status indicators
- Responsive design

### 2. HA Failover Demo (10 minutes)

**Talking Points:**
- "Now let's demonstrate the High Availability failover capabilities"
- "This is the core feature - automatic failover with zero downtime"

**Demo Actions:**

#### NameNode Failover:
1. Navigate to **HA Controls** tab
2. Show current active/standby configuration
3. Click **"Trigger Failover"** for NameNode
4. **Watch the process**:
   - Progress bar shows failover steps
   - Status indicators change in real-time
   - Overview tab updates automatically
5. **Verify failover**:
   - Switch back to Overview tab
   - Point out role reversal
   - Show service continuity

#### ResourceManager Failover:
1. Return to **HA Controls** tab
2. Trigger ResourceManager failover
3. **Monitor the process**:
   - Real-time status updates
   - Failover history gets updated
4. **Show job continuity** (if any jobs are running)

**Key Features to Highlight:**
- Zero downtime failover
- Real-time status monitoring
- Automatic role switching
- Failover history tracking

### 3. File Management Demo (8 minutes)

**Talking Points:**
- "Let's explore the HDFS file management capabilities"
- "This shows the distributed file system in action"

**Demo Actions:**

#### File Upload:
1. Navigate to **File Manager** tab
2. Browse root directory structure
3. **Upload files**:
   - Click "Upload File"
   - Drag & drop demo files
   - Show upload progress
   - Verify files appear in browser

#### Browse HDFS:
1. **Navigate directories**:
   - Click on directories to explore
   - Show breadcrumb navigation
   - Demonstrate file/folder icons
2. **File details**:
   - Point out permissions, ownership
   - Show file sizes and timestamps
   - Explain replication information

#### Download from URL:
1. Click **"Download from URL"**
2. Use a sample URL (e.g., `https://raw.githubusercontent.com/apache/spark/master/README.md`)
3. Show download progress
4. Verify file appears in HDFS

**Key Features to Highlight:**
- Drag & drop file upload
- Real-time directory browsing
- URL-to-HDFS download
- File metadata display

### 4. Job Monitoring Demo (7 minutes)

**Talking Points:**
- "Now let's look at job monitoring and resource management"
- "This shows how YARN manages applications across the cluster"

**Demo Actions:**

#### YARN Applications:
1. Navigate to **Job Monitor** tab
2. Show current applications (if any)
3. **Submit a test job** (optional):
   ```bash
   # From host machine
   docker exec active-nn /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.1.jar pi 2 100
   ```
4. **Monitor job execution**:
   - Refresh to see new applications
   - Show job states (RUNNING, FINISHED)
   - Point out resource allocation

#### Resource Monitoring:
1. Return to **Overview** tab
2. Show real-time resource usage
3. Point out memory/vCore allocation
4. Explain active nodes count

**Key Features to Highlight:**
- Real-time application tracking
- Resource utilization monitoring
- Job state management
- Cluster capacity planning

### 5. Real-time Monitoring Demo (5 minutes)

**Talking Points:**
- "Let's explore the real-time monitoring capabilities"
- "Everything updates automatically without page refresh"

**Demo Actions:**

#### Live Updates:
1. Navigate to **Logs** tab
2. Show real-time log streaming
3. **Trigger events**:
   - Perform another failover
   - Upload more files
   - Watch logs update in real-time

#### Performance Charts:
1. Return to **Overview** tab
2. Watch performance chart updates
3. Point out historical trends
4. Explain different metrics

#### Auto-refresh Features:
1. Demonstrate Server-Sent Events
2. Show automatic data updates
3. Point out timestamp changes

**Key Features to Highlight:**
- Server-Sent Events for real-time updates
- No manual refresh needed
- Historical data retention
- Live event logging

### 6. Advanced Features Demo (5 minutes)

**Talking Points:**
- "Let's look at some advanced monitoring features"
- "These help in troubleshooting and cluster management"

**Demo Actions:**

#### Force Failover:
1. Go to **HA Controls**
2. Demonstrate **"Force Failover"** option
3. Explain use cases for forced failover
4. Show warning indicators

#### Error Simulation:
1. **Simulate service issue** (optional):
   ```bash
   # Stop a DataNode temporarily
   docker exec worker1 /usr/local/hadoop/bin/hdfs --daemon stop datanode
   ```
2. Watch dashboard detect the issue
3. Show health score impact
4. Restart service and show recovery

#### Toast Notifications:
1. Trigger various actions
2. Point out toast notifications
3. Show different notification types

**Key Features to Highlight:**
- Error detection and alerting
- Health impact visualization
- Recovery monitoring
- User feedback system

## Demo Wrap-up (5 minutes)

### Summary Points:
1. **High Availability**: Zero-downtime failover capabilities
2. **Real-time Monitoring**: Live updates without manual refresh
3. **File Management**: Easy HDFS operations with web interface
4. **Job Tracking**: Complete YARN application monitoring
5. **User Experience**: Modern, responsive dashboard design

### Technical Highlights:
- **Architecture**: FastAPI backend + vanilla JavaScript frontend
- **Real-time**: Server-Sent Events for live updates
- **Integration**: Direct Hadoop REST API integration
- **Scalability**: Handles large clusters and high update frequencies
- **Accessibility**: Works on desktop, tablet, and mobile

### Use Cases:
- **DevOps**: Cluster monitoring and management
- **Development**: Testing HA scenarios
- **Training**: Learning Hadoop HA concepts
- **Debugging**: Troubleshooting cluster issues
- **Demos**: Showcasing Hadoop capabilities

## Troubleshooting During Demo

### Common Issues:

1. **Dashboard not loading**:
   ```bash
   docker-compose logs hadoop-dashboard
   docker-compose restart hadoop-dashboard
   ```

2. **Hadoop services not ready**:
   ```bash
   # Check service status
   curl http://localhost:9870/jmx
   curl http://localhost:8088/ws/v1/cluster/info
   ```

3. **Failover not working**:
   - Ensure all JournalNodes are running
   - Check Zookeeper connectivity
   - Verify HDFS is properly initialized

4. **Real-time updates stopped**:
   - Refresh browser page
   - Check browser console for errors
   - Verify SSE connection

### Emergency Recovery:

```bash
# Reset everything
docker-compose down
docker-compose up -d
./Start.sh
```

## Post-Demo

### Questions to Address:
- How does this compare to native Hadoop UIs?
- Can this be extended for production use?
- What about security and authentication?
- How does it handle larger clusters?
- Integration with other monitoring tools?

### Next Steps:
- Explore individual Hadoop service UIs
- Discuss production deployment considerations
- Show integration possibilities
- Explain customization options

---

**Total Demo Time: ~45 minutes**
**Recommended Audience: Technical stakeholders, DevOps teams, Hadoop users**
**Prerequisites: Basic understanding of Hadoop concepts**