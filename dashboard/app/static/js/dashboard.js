// Dashboard Main JavaScript
class HadoopDashboard {
    constructor() {
        this.eventSource = null;
        this.logsEventSource = null;
        this.currentMetrics = {};
        this.failoverHistory = [];
        this.autoScroll = true;
        
        this.init();
    }
    
    init() {
        this.setupEventListeners();
        this.setupSSE();
        this.setupTheme();
        this.loadInitialData();
        this.startPeriodicUpdates();
        
        // Show loading overlay initially
        this.showLoading();
    }
    
    setupEventListeners() {
        // Navigation tabs
        document.querySelectorAll('.nav-tab').forEach(tab => {
            tab.addEventListener('click', (e) => {
                this.switchTab(e.target.dataset.tab);
            });
        });
        
        // Theme toggle
        document.getElementById('theme-toggle').addEventListener('click', () => {
            this.toggleTheme();
        });
        
        // HA Controls
        document.getElementById('nn-failover-btn').addEventListener('click', () => {
            this.triggerNameNodeFailover(false);
        });
        
        document.getElementById('nn-force-failover-btn').addEventListener('click', () => {
            this.triggerNameNodeFailover(true);
        });
        
        document.getElementById('rm-failover-btn').addEventListener('click', () => {
            this.triggerResourceManagerFailover(false);
        });
        
        document.getElementById('rm-force-failover-btn').addEventListener('click', () => {
            this.triggerResourceManagerFailover(true);
        });
        
        // Clear history
        document.getElementById('clear-history-btn').addEventListener('click', () => {
            this.clearFailoverHistory();
        });
        
        // Refresh buttons
        document.getElementById('refresh-jobs-btn').addEventListener('click', () => {
            this.loadYarnApplications();
        });
        
        // Logs controls
        document.getElementById('clear-logs-btn').addEventListener('click', () => {
            this.clearLogs();
        });
        
        document.getElementById('auto-scroll-toggle').addEventListener('click', (e) => {
            this.toggleAutoScroll(e.target);
        });
        
        // Modal controls
        this.setupModalListeners();
    }
    
    setupModalListeners() {
        // Close modals
        document.querySelectorAll('.modal-close').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const modal = e.target.closest('.modal');
                this.closeModal(modal);
            });
        });
        
        // Click outside modal
        document.querySelectorAll('.modal').forEach(modal => {
            modal.addEventListener('click', (e) => {
                if (e.target === modal) {
                    this.closeModal(modal);
                }
            });
        });
    }
    
    setupSSE() {
        // Setup metrics stream
        this.eventSource = new EventSource('/api/stream/metrics');
        
        this.eventSource.onmessage = (event) => {
            try {
                const metrics = JSON.parse(event.data);
                this.updateDashboard(metrics);
            } catch (error) {
                console.error('Error parsing metrics:', error);
            }
        };
        
        this.eventSource.onerror = (error) => {
            console.error('SSE connection error:', error);
            // Attempt to reconnect after 5 seconds
            setTimeout(() => {
                if (this.eventSource.readyState === EventSource.CLOSED) {
                    this.setupSSE();
                }
            }, 5000);
        };
        
        // Setup logs stream
        this.logsEventSource = new EventSource('/api/stream/logs');
        
        this.logsEventSource.onmessage = (event) => {
            try {
                const logs = JSON.parse(event.data);
                this.updateLogs(logs);
            } catch (error) {
                console.error('Error parsing logs:', error);
            }
        };
    }
    
    setupTheme() {
        // Load saved theme
        const savedTheme = localStorage.getItem('dashboard-theme') || 'light';
        document.documentElement.setAttribute('data-theme', savedTheme);
        
        const themeIcon = document.querySelector('#theme-toggle i');
        themeIcon.className = savedTheme === 'dark' ? 'fas fa-sun' : 'fas fa-moon';
    }
    
    toggleTheme() {
        const currentTheme = document.documentElement.getAttribute('data-theme');
        const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
        
        document.documentElement.setAttribute('data-theme', newTheme);
        localStorage.setItem('dashboard-theme', newTheme);
        
        const themeIcon = document.querySelector('#theme-toggle i');
        themeIcon.className = newTheme === 'dark' ? 'fas fa-sun' : 'fas fa-moon';
    }
    
    async loadInitialData() {
        try {
            // Load cluster status
            await this.loadClusterStatus();
            
            // Load YARN applications
            await this.loadYarnApplications();
            
            // Hide loading overlay
            this.hideLoading();
            
        } catch (error) {
            console.error('Error loading initial data:', error);
            this.showToast('Error', 'Failed to load initial data', 'error');
            this.hideLoading();
        }
    }
    
    async loadClusterStatus() {
        try {
            const response = await fetch('/api/cluster/status');
            const status = await response.json();
            this.updateDashboard(status);
        } catch (error) {
            console.error('Error loading cluster status:', error);
        }
    }
    
    updateDashboard(metrics) {
        this.currentMetrics = metrics;
        
        // Update cluster health
        this.updateClusterHealth(metrics.cluster_health || {});
        
        // Update NameNode status
        this.updateNameNodeStatus(metrics.namenode_metrics || {});
        
        // Update ResourceManager status
        this.updateResourceManagerStatus(metrics.resourcemanager_metrics || {});
        
        // Update performance metrics
        this.updatePerformanceMetrics(metrics.performance_metrics || {});
        
        // Update node metrics
        this.updateNodeMetrics(metrics.node_metrics || {});
        
        // Update service metrics
        this.updateServiceMetrics(metrics.service_metrics || {});
        
        // Update HA controls
        this.updateHAControls(metrics);
    }
    
    updateClusterHealth(health) {
        const statusElement = document.getElementById('cluster-health-status');
        const scoreElement = document.getElementById('cluster-health-score');
        
        if (health.status) {
            statusElement.textContent = health.status.charAt(0).toUpperCase() + health.status.slice(1);
            statusElement.className = `health-status ${health.status}`;
        }
        
        if (health.percentage !== undefined) {
            scoreElement.textContent = `${Math.round(health.percentage)}%`;
        }
    }
    
    updateNameNodeStatus(metrics) {
        // Update card status
        const cardStatus = document.querySelector('#namenode-status');
        const isHealthy = metrics.active_healthy && metrics.standby_healthy;
        
        this.updateCardStatus(cardStatus, isHealthy, 'NameNode HA Healthy', 'NameNode Issues');
        
        // Update active NameNode
        const activeNode = document.getElementById('nn-active');
        this.updateHANode(activeNode, {
            healthy: metrics.active_healthy,
            state: metrics.active_state,
            responseTime: metrics.active_response_time,
            isActive: metrics.active_state === 'active'
        });
        
        // Update standby NameNode
        const standbyNode = document.getElementById('nn-standby');
        this.updateHANode(standbyNode, {
            healthy: metrics.standby_healthy,
            state: metrics.standby_state,
            responseTime: metrics.standby_response_time,
            isActive: metrics.standby_state === 'active'
        });
    }
    
    updateResourceManagerStatus(metrics) {
        // Update card status
        const cardStatus = document.querySelector('#resourcemanager-status');
        const isHealthy = metrics.active_healthy && metrics.standby_healthy;
        
        this.updateCardStatus(cardStatus, isHealthy, 'ResourceManager HA Healthy', 'ResourceManager Issues');
        
        // Update active ResourceManager
        const activeNode = document.getElementById('rm-active');
        this.updateHANode(activeNode, {
            healthy: metrics.active_healthy,
            state: metrics.active_state,
            responseTime: metrics.active_response_time,
            isActive: metrics.active_state === 'active'
        });
        
        // Update standby ResourceManager
        const standbyNode = document.getElementById('rm-standby');
        this.updateHANode(standbyNode, {
            healthy: metrics.standby_healthy,
            state: metrics.standby_state,
            responseTime: metrics.standby_response_time,
            isActive: metrics.standby_state === 'active'
        });
    }
    
    updateHANode(nodeElement, nodeData) {
        const statusIndicator = nodeElement.querySelector('.status-indicator');
        const responseTime = nodeElement.querySelector('.response-time');
        
        // Update status indicator
        statusIndicator.className = 'status-indicator';
        if (nodeData.healthy) {
            statusIndicator.classList.add('healthy');
        } else {
            statusIndicator.classList.add('error');
        }
        
        // Update response time
        if (nodeData.responseTime !== undefined) {
            responseTime.textContent = `${Math.round(nodeData.responseTime * 1000)}ms`;
        }
        
        // Update node class based on state
        nodeElement.className = 'ha-node';
        if (nodeData.isActive) {
            nodeElement.classList.add('active');
        } else if (nodeData.state === 'standby') {
            nodeElement.classList.add('standby');
        }
    }
    
    updatePerformanceMetrics(metrics) {
        // Update memory usage
        if (metrics.total_memory && metrics.allocated_memory) {
            const memoryUsage = (metrics.allocated_memory / metrics.total_memory) * 100;
            const memoryFill = document.getElementById('memory-usage');
            const memoryText = document.getElementById('memory-text');
            
            memoryFill.style.width = `${memoryUsage}%`;
            memoryFill.className = 'metric-fill';
            if (memoryUsage > 90) memoryFill.classList.add('error');
            else if (memoryUsage > 75) memoryFill.classList.add('warning');
            
            memoryText.textContent = `${this.formatBytes(metrics.allocated_memory * 1024 * 1024)} / ${this.formatBytes(metrics.total_memory * 1024 * 1024)}`;
        }
        
        // Update vCores usage
        if (metrics.total_vcores && metrics.allocated_vcores) {
            const vcoresUsage = (metrics.allocated_vcores / metrics.total_vcores) * 100;
            const vcoresFill = document.getElementById('vcores-usage');
            const vcoresText = document.getElementById('vcores-text');
            
            vcoresFill.style.width = `${vcoresUsage}%`;
            vcoresFill.className = 'metric-fill';
            if (vcoresUsage > 90) vcoresFill.classList.add('error');
            else if (vcoresUsage > 75) vcoresFill.classList.add('warning');
            
            vcoresText.textContent = `${metrics.allocated_vcores} / ${metrics.total_vcores}`;
        }
        
        // Update other metrics
        if (metrics.active_nodes !== undefined) {
            document.getElementById('active-nodes').textContent = metrics.active_nodes;
        }
        
        if (metrics.running_apps !== undefined) {
            document.getElementById('running-apps').textContent = metrics.running_apps;
        }
    }
    
    updateNodeMetrics(metrics) {
        // Update DataNodes
        const datanodeCount = document.getElementById('datanode-count');
        const datanodeList = document.getElementById('datanode-list');
        
        if (metrics.total_datanodes !== undefined) {
            datanodeCount.textContent = `${metrics.healthy_datanodes || 0} / ${metrics.total_datanodes}`;
            this.updateNodeList(datanodeList, metrics.healthy_datanodes, metrics.total_datanodes, 'worker');
        }
        
        // Update NodeManagers
        const nodemanagerCount = document.getElementById('nodemanager-count');
        const nodemanagerList = document.getElementById('nodemanager-list');
        
        if (metrics.total_nodemanagers !== undefined) {
            nodemanagerCount.textContent = `${metrics.healthy_nodemanagers || 0} / ${metrics.total_nodemanagers}`;
            this.updateNodeList(nodemanagerList, metrics.healthy_nodemanagers, metrics.total_nodemanagers, 'worker');
        }
        
        // Update JournalNodes
        const journalnodeCount = document.getElementById('journalnode-count');
        const journalnodeList = document.getElementById('journalnode-list');
        
        if (metrics.total_journalnodes !== undefined) {
            journalnodeCount.textContent = `${metrics.healthy_journalnodes || 0} / ${metrics.total_journalnodes}`;
            this.updateNodeList(journalnodeList, metrics.healthy_journalnodes, metrics.total_journalnodes, 'journalnode');
        }
    }
    
    updateNodeList(listElement, healthy, total, prefix) {
        listElement.innerHTML = '';
        
        for (let i = 1; i <= total; i++) {
            const nodeItem = document.createElement('div');
            nodeItem.className = 'node-item';
            
            const isHealthy = i <= healthy;
            nodeItem.classList.add(isHealthy ? 'healthy' : 'unhealthy');
            
            nodeItem.innerHTML = `
                <span class="status-indicator ${isHealthy ? 'healthy' : 'error'}"></span>
                ${prefix}${i}
            `;
            
            listElement.appendChild(nodeItem);
        }
    }
    
    updateServiceMetrics(metrics) {
        // Update History Server
        const historyServerService = document.getElementById('historyserver-service');
        this.updateServiceStatus(historyServerService, metrics.historyserver_healthy);
        
        // Update Hive
        const hiveService = document.getElementById('hive-service');
        this.updateServiceStatus(hiveService, metrics.hive_healthy);
    }
    
    updateServiceStatus(serviceElement, isHealthy) {
        const statusIndicator = serviceElement.querySelector('.status-indicator');
        const statusText = serviceElement.querySelector('.status-text');
        
        statusIndicator.className = 'status-indicator';
        if (isHealthy) {
            statusIndicator.classList.add('healthy');
            statusText.textContent = 'Healthy';
        } else {
            statusIndicator.classList.add('error');
            statusText.textContent = 'Unhealthy';
        }
    }
    
    updateCardStatus(cardElement, isHealthy, healthyText, unhealthyText) {
        const statusIndicator = cardElement.querySelector('.status-indicator');
        const statusText = cardElement.querySelector('.status-text');
        
        statusIndicator.className = 'status-indicator';
        if (isHealthy) {
            statusIndicator.classList.add('healthy');
            statusText.textContent = healthyText;
        } else {
            statusIndicator.classList.add('error');
            statusText.textContent = unhealthyText;
        }
    }
    
    updateHAControls(metrics) {
        // Update NameNode controls
        const nnActiveSpan = document.getElementById('nn-current-active');
        const nnStandbySpan = document.getElementById('nn-current-standby');
        
        const nnMetrics = metrics.namenode_metrics || {};
        if (nnMetrics.active_state === 'active') {
            nnActiveSpan.textContent = 'active-nn';
            nnStandbySpan.textContent = 'standby-nn';
        } else {
            nnActiveSpan.textContent = 'standby-nn';
            nnStandbySpan.textContent = 'active-nn';
        }
        
        // Update ResourceManager controls
        const rmActiveSpan = document.getElementById('rm-current-active');
        const rmStandbySpan = document.getElementById('rm-current-standby');
        
        const rmMetrics = metrics.resourcemanager_metrics || {};
        if (rmMetrics.active_state === 'active') {
            rmActiveSpan.textContent = 'active-rm';
            rmStandbySpan.textContent = 'standby-rm';
        } else {
            rmActiveSpan.textContent = 'standby-rm';
            rmStandbySpan.textContent = 'active-rm';
        }
    }
    
    async triggerNameNodeFailover(force = false) {
        const progressContainer = document.getElementById('nn-failover-progress');
        const buttons = document.querySelectorAll('#nn-failover-btn, #nn-force-failover-btn');
        
        try {
            // Disable buttons and show progress
            buttons.forEach(btn => btn.disabled = true);
            progressContainer.style.display = 'block';
            
            const response = await fetch('/api/ha/namenode/failover', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ force })
            });
            
            const result = await response.json();
            
            if (result.success) {
                this.showToast('Success', 'NameNode failover completed successfully', 'success');
                this.addFailoverHistory(result);
            } else {
                throw new Error(result.error || 'Failover failed');
            }
            
        } catch (error) {
            console.error('NameNode failover error:', error);
            this.showToast('Error', `NameNode failover failed: ${error.message}`, 'error');
        } finally {
            // Re-enable buttons and hide progress
            buttons.forEach(btn => btn.disabled = false);
            progressContainer.style.display = 'none';
        }
    }
    
    async triggerResourceManagerFailover(force = false) {
        const progressContainer = document.getElementById('rm-failover-progress');
        const buttons = document.querySelectorAll('#rm-failover-btn, #rm-force-failover-btn');
        
        try {
            // Disable buttons and show progress
            buttons.forEach(btn => btn.disabled = true);
            progressContainer.style.display = 'block';
            
            const response = await fetch('/api/ha/resourcemanager/failover', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ force })
            });
            
            const result = await response.json();
            
            if (result.success) {
                this.showToast('Success', 'ResourceManager failover completed successfully', 'success');
                this.addFailoverHistory(result);
            } else {
                throw new Error(result.error || 'Failover failed');
            }
            
        } catch (error) {
            console.error('ResourceManager failover error:', error);
            this.showToast('Error', `ResourceManager failover failed: ${error.message}`, 'error');
        } finally {
            // Re-enable buttons and hide progress
            buttons.forEach(btn => btn.disabled = false);
            progressContainer.style.display = 'none';
        }
    }
    
    addFailoverHistory(result) {
        this.failoverHistory.unshift(result);
        
        // Keep only last 20 entries
        if (this.failoverHistory.length > 20) {
            this.failoverHistory = this.failoverHistory.slice(0, 20);
        }
        
        this.updateFailoverHistory();
    }
    
    updateFailoverHistory() {
        const historyContainer = document.getElementById('failover-history');
        
        if (this.failoverHistory.length === 0) {
            historyContainer.innerHTML = '<p style="text-align: center; color: var(--text-secondary); padding: 2rem;">No failover history yet</p>';
            return;
        }
        
        historyContainer.innerHTML = this.failoverHistory.map(item => `
            <div class="history-item">
                <div class="history-header">
                    <span class="history-type">${item.type.replace('_', ' ').toUpperCase()}</span>
                    <div style="display: flex; align-items: center; gap: 1rem;">
                        <span class="history-status ${item.success ? 'success' : 'failed'}">
                            ${item.success ? 'Success' : 'Failed'}
                        </span>
                        <span class="history-timestamp">${new Date(item.timestamp).toLocaleString()}</span>
                    </div>
                </div>
                <div class="history-details">
                    ${item.error || 'Failover completed successfully'}
                </div>
            </div>
        `).join('');
    }
    
    clearFailoverHistory() {
        this.failoverHistory = [];
        this.updateFailoverHistory();
        this.showToast('Info', 'Failover history cleared', 'info');
    }
    
    async loadYarnApplications() {
        try {
            const response = await fetch('/api/yarn/applications');
            const data = await response.json();
            
            this.updateYarnApplications(data.apps?.app || []);
            
        } catch (error) {
            console.error('Error loading YARN applications:', error);
            this.showToast('Error', 'Failed to load YARN applications', 'error');
        }
    }
    
    updateYarnApplications(applications) {
        const tableContainer = document.getElementById('applications-table');
        
        if (!applications || applications.length === 0) {
            tableContainer.innerHTML = '<p style="text-align: center; color: var(--text-secondary); padding: 2rem;">No applications found</p>';
            return;
        }
        
        const tableHTML = `
            <table>
                <thead>
                    <tr>
                        <th>Application ID</th>
                        <th>Name</th>
                        <th>Type</th>
                        <th>State</th>
                        <th>Progress</th>
                        <th>Started</th>
                    </tr>
                </thead>
                <tbody>
                    ${applications.map(app => `
                        <tr>
                            <td><code>${app.id}</code></td>
                            <td>${app.name}</td>
                            <td>${app.applicationType}</td>
                            <td><span class="app-status ${app.state.toLowerCase()}">${app.state}</span></td>
                            <td>${app.progress}%</td>
                            <td>${new Date(app.startedTime).toLocaleString()}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;
        
        tableContainer.innerHTML = tableHTML;
    }
    
    updateLogs(logs) {
        const logsList = document.getElementById('logs-list');
        
        logs.forEach(log => {
            const logEntry = document.createElement('div');
            logEntry.className = `log-entry ${log.level}`;
            
            logEntry.innerHTML = `
                <span class="log-timestamp">${new Date(log.timestamp).toLocaleTimeString()}</span>
                <span class="log-level ${log.level}">${log.level}</span>
                <span class="log-message">${log.message}</span>
            `;
            
            logsList.appendChild(logEntry);
        });
        
        // Auto-scroll if enabled
        if (this.autoScroll) {
            logsList.scrollTop = logsList.scrollHeight;
        }
        
        // Limit log entries
        const maxLogs = 1000;
        const logEntries = logsList.querySelectorAll('.log-entry');
        if (logEntries.length > maxLogs) {
            for (let i = 0; i < logEntries.length - maxLogs; i++) {
                logEntries[i].remove();
            }
        }
    }
    
    clearLogs() {
        document.getElementById('logs-list').innerHTML = '';
        this.showToast('Info', 'Logs cleared', 'info');
    }
    
    toggleAutoScroll(button) {
        this.autoScroll = !this.autoScroll;
        button.classList.toggle('active', this.autoScroll);
        
        if (this.autoScroll) {
            const logsList = document.getElementById('logs-list');
            logsList.scrollTop = logsList.scrollHeight;
        }
    }
    
    switchTab(tabName) {
        // Update nav tabs
        document.querySelectorAll('.nav-tab').forEach(tab => {
            tab.classList.toggle('active', tab.dataset.tab === tabName);
        });
        
        // Update tab content
        document.querySelectorAll('.tab-content').forEach(content => {
            content.classList.toggle('active', content.id === `${tabName}-tab`);
        });
        
        // Load tab-specific data
        if (tabName === 'job-monitor') {
            this.loadYarnApplications();
        }
    }
    
    showToast(title, message, type = 'info') {
        const toastContainer = document.getElementById('toast-container');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        
        const iconMap = {
            success: 'fas fa-check-circle',
            warning: 'fas fa-exclamation-triangle',
            error: 'fas fa-times-circle',
            info: 'fas fa-info-circle'
        };
        
        toast.innerHTML = `
            <i class="toast-icon ${iconMap[type]}"></i>
            <div class="toast-content">
                <div class="toast-title">${title}</div>
                <div class="toast-message">${message}</div>
            </div>
            <button class="toast-close">&times;</button>
        `;
        
        // Add close functionality
        toast.querySelector('.toast-close').addEventListener('click', () => {
            toast.remove();
        });
        
        toastContainer.appendChild(toast);
        
        // Auto-remove after 5 seconds
        setTimeout(() => {
            if (toast.parentNode) {
                toast.remove();
            }
        }, 5000);
    }
    
    showModal(modalId) {
        const modal = document.getElementById(modalId);
        modal.classList.add('active');
    }
    
    closeModal(modal) {
        modal.classList.remove('active');
    }
    
    showLoading() {
        document.getElementById('loading-overlay').classList.add('active');
    }
    
    hideLoading() {
        document.getElementById('loading-overlay').classList.remove('active');
    }
    
    startPeriodicUpdates() {
        // Refresh cluster status every 30 seconds as backup to SSE
        setInterval(() => {
            if (this.eventSource.readyState !== EventSource.OPEN) {
                this.loadClusterStatus();
            }
        }, 30000);
    }
    
    formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }
    
    // Cleanup
    destroy() {
        if (this.eventSource) {
            this.eventSource.close();
        }
        if (this.logsEventSource) {
            this.logsEventSource.close();
        }
    }
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new HadoopDashboard();
});

// Cleanup on page unload
window.addEventListener('beforeunload', () => {
    if (window.dashboard) {
        window.dashboard.destroy();
    }
});