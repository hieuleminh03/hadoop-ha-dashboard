// Charts and Visualizations for Hadoop Dashboard
class DashboardCharts {
    constructor() {
        this.charts = {};
        this.chartData = {
            performance: {
                labels: [],
                datasets: []
            }
        };
        this.maxDataPoints = 50;
        
        this.init();
    }
    
    init() {
        this.initPerformanceChart();
        this.setupChartUpdates();
    }
    
    initPerformanceChart() {
        const ctx = document.getElementById('performance-chart');
        if (!ctx) return;
        
        this.charts.performance = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    {
                        label: 'Memory Usage (%)',
                        data: [],
                        borderColor: '#3b82f6',
                        backgroundColor: 'rgba(59, 130, 246, 0.1)',
                        borderWidth: 2,
                        fill: true,
                        tension: 0.4,
                        yAxisID: 'percentage'
                    },
                    {
                        label: 'vCores Usage (%)',
                        data: [],
                        borderColor: '#10b981',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        borderWidth: 2,
                        fill: true,
                        tension: 0.4,
                        yAxisID: 'percentage'
                    },
                    {
                        label: 'Active Nodes',
                        data: [],
                        borderColor: '#f59e0b',
                        backgroundColor: 'rgba(245, 158, 11, 0.1)',
                        borderWidth: 2,
                        fill: false,
                        tension: 0.4,
                        yAxisID: 'count'
                    },
                    {
                        label: 'Running Apps',
                        data: [],
                        borderColor: '#ef4444',
                        backgroundColor: 'rgba(239, 68, 68, 0.1)',
                        borderWidth: 2,
                        fill: false,
                        tension: 0.4,
                        yAxisID: 'count'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    intersect: false,
                    mode: 'index'
                },
                plugins: {
                    title: {
                        display: true,
                        text: 'Real-time Cluster Performance',
                        font: {
                            size: 16,
                            weight: 'bold'
                        }
                    },
                    legend: {
                        display: true,
                        position: 'top',
                        labels: {
                            usePointStyle: true,
                            padding: 20
                        }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(0, 0, 0, 0.8)',
                        titleColor: '#fff',
                        bodyColor: '#fff',
                        borderColor: '#374151',
                        borderWidth: 1,
                        cornerRadius: 8,
                        displayColors: true,
                        callbacks: {
                            title: function(context) {
                                return `Time: ${context[0].label}`;
                            },
                            label: function(context) {
                                const label = context.dataset.label;
                                const value = context.parsed.y;
                                
                                if (label.includes('%')) {
                                    return `${label}: ${value.toFixed(1)}%`;
                                } else {
                                    return `${label}: ${value}`;
                                }
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        display: true,
                        title: {
                            display: true,
                            text: 'Time'
                        },
                        grid: {
                            color: 'rgba(156, 163, 175, 0.2)'
                        },
                        ticks: {
                            maxTicksLimit: 10,
                            callback: function(value, index, values) {
                                const label = this.getLabelForValue(value);
                                return label.split(' ')[1]; // Show only time part
                            }
                        }
                    },
                    percentage: {
                        type: 'linear',
                        position: 'left',
                        display: true,
                        title: {
                            display: true,
                            text: 'Usage (%)'
                        },
                        min: 0,
                        max: 100,
                        grid: {
                            color: 'rgba(156, 163, 175, 0.2)'
                        },
                        ticks: {
                            callback: function(value) {
                                return value + '%';
                            }
                        }
                    },
                    count: {
                        type: 'linear',
                        position: 'right',
                        display: true,
                        title: {
                            display: true,
                            text: 'Count'
                        },
                        min: 0,
                        grid: {
                            display: false
                        },
                        ticks: {
                            stepSize: 1
                        }
                    }
                },
                elements: {
                    point: {
                        radius: 3,
                        hoverRadius: 6
                    }
                },
                animation: {
                    duration: 750,
                    easing: 'easeInOutQuart'
                }
            }
        });
    }
    
    setupChartUpdates() {
        // Listen for metrics updates from the main dashboard
        document.addEventListener('metricsUpdated', (event) => {
            this.updateCharts(event.detail);
        });
    }
    
    updateCharts(metrics) {
        if (metrics.performance_metrics) {
            this.updatePerformanceChart(metrics.performance_metrics, metrics.timestamp);
        }
    }
    
    updatePerformanceChart(performanceMetrics, timestamp) {
        if (!this.charts.performance) return;
        
        const chart = this.charts.performance;
        const timeLabel = new Date(timestamp).toLocaleTimeString();
        
        // Calculate usage percentages
        const memoryUsage = performanceMetrics.total_memory > 0 
            ? (performanceMetrics.allocated_memory / performanceMetrics.total_memory) * 100 
            : 0;
        
        const vcoresUsage = performanceMetrics.total_vcores > 0 
            ? (performanceMetrics.allocated_vcores / performanceMetrics.total_vcores) * 100 
            : 0;
        
        // Add new data point
        chart.data.labels.push(timeLabel);
        chart.data.datasets[0].data.push(memoryUsage);
        chart.data.datasets[1].data.push(vcoresUsage);
        chart.data.datasets[2].data.push(performanceMetrics.active_nodes || 0);
        chart.data.datasets[3].data.push(performanceMetrics.running_apps || 0);
        
        // Remove old data points to maintain performance
        if (chart.data.labels.length > this.maxDataPoints) {
            chart.data.labels.shift();
            chart.data.datasets.forEach(dataset => {
                dataset.data.shift();
            });
        }
        
        // Update chart
        chart.update('none'); // No animation for real-time updates
    }
    
    createTopologyVisualization() {
        // Create a visual representation of the cluster topology
        const topologyContainer = document.getElementById('topology-visualization');
        if (!topologyContainer) return;
        
        // This would create an SVG or canvas-based visualization
        // showing the cluster nodes and their relationships
        // For now, we'll create a simple HTML representation
        
        const topologyHTML = `
            <div class="topology-container">
                <div class="topology-section">
                    <h4>NameNodes</h4>
                    <div class="topology-nodes">
                        <div class="topology-node namenode active" id="topo-nn-active">
                            <i class="fas fa-server"></i>
                            <span>Active NN</span>
                        </div>
                        <div class="topology-node namenode standby" id="topo-nn-standby">
                            <i class="fas fa-server"></i>
                            <span>Standby NN</span>
                        </div>
                    </div>
                </div>
                
                <div class="topology-section">
                    <h4>ResourceManagers</h4>
                    <div class="topology-nodes">
                        <div class="topology-node resourcemanager active" id="topo-rm-active">
                            <i class="fas fa-cogs"></i>
                            <span>Active RM</span>
                        </div>
                        <div class="topology-node resourcemanager standby" id="topo-rm-standby">
                            <i class="fas fa-cogs"></i>
                            <span>Standby RM</span>
                        </div>
                    </div>
                </div>
                
                <div class="topology-section">
                    <h4>JournalNodes</h4>
                    <div class="topology-nodes">
                        <div class="topology-node journalnode" id="topo-jn1">
                            <i class="fas fa-database"></i>
                            <span>JN1</span>
                        </div>
                        <div class="topology-node journalnode" id="topo-jn2">
                            <i class="fas fa-database"></i>
                            <span>JN2</span>
                        </div>
                        <div class="topology-node journalnode" id="topo-jn3">
                            <i class="fas fa-database"></i>
                            <span>JN3</span>
                        </div>
                    </div>
                </div>
                
                <div class="topology-section">
                    <h4>Workers</h4>
                    <div class="topology-nodes">
                        <div class="topology-node worker" id="topo-worker1">
                            <i class="fas fa-desktop"></i>
                            <span>Worker1</span>
                        </div>
                        <div class="topology-node worker" id="topo-worker2">
                            <i class="fas fa-desktop"></i>
                            <span>Worker2</span>
                        </div>
                        <div class="topology-node worker" id="topo-worker3">
                            <i class="fas fa-desktop"></i>
                            <span>Worker3</span>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        topologyContainer.innerHTML = topologyHTML;
    }
    
    updateTopologyVisualization(metrics) {
        // Update the topology visualization based on current metrics
        if (!document.getElementById('topology-visualization')) return;
        
        // Update NameNode states
        const nnActive = document.getElementById('topo-nn-active');
        const nnStandby = document.getElementById('topo-nn-standby');
        
        if (metrics.namenode_metrics) {
            this.updateTopologyNode(nnActive, metrics.namenode_metrics.active_healthy, 
                                   metrics.namenode_metrics.active_state === 'active');
            this.updateTopologyNode(nnStandby, metrics.namenode_metrics.standby_healthy,
                                   metrics.namenode_metrics.standby_state === 'active');
        }
        
        // Update ResourceManager states
        const rmActive = document.getElementById('topo-rm-active');
        const rmStandby = document.getElementById('topo-rm-standby');
        
        if (metrics.resourcemanager_metrics) {
            this.updateTopologyNode(rmActive, metrics.resourcemanager_metrics.active_healthy,
                                   metrics.resourcemanager_metrics.active_state === 'active');
            this.updateTopologyNode(rmStandby, metrics.resourcemanager_metrics.standby_healthy,
                                   metrics.resourcemanager_metrics.standby_state === 'active');
        }
        
        // Update JournalNodes
        if (metrics.node_metrics) {
            const journalNodes = [
                document.getElementById('topo-jn1'),
                document.getElementById('topo-jn2'),
                document.getElementById('topo-jn3')
            ];
            
            journalNodes.forEach((node, index) => {
                if (node) {
                    const isHealthy = index < (metrics.node_metrics.healthy_journalnodes || 0);
                    this.updateTopologyNode(node, isHealthy, true);
                }
            });
        }
        
        // Update Workers
        if (metrics.node_metrics) {
            const workers = [
                document.getElementById('topo-worker1'),
                document.getElementById('topo-worker2'),
                document.getElementById('topo-worker3')
            ];
            
            workers.forEach((node, index) => {
                if (node) {
                    const isHealthy = index < (metrics.node_metrics.healthy_datanodes || 0);
                    this.updateTopologyNode(node, isHealthy, true);
                }
            });
        }
    }
    
    updateTopologyNode(nodeElement, isHealthy, isActive) {
        if (!nodeElement) return;
        
        // Remove existing status classes
        nodeElement.classList.remove('healthy', 'unhealthy', 'active', 'standby');
        
        // Add health status
        if (isHealthy) {
            nodeElement.classList.add('healthy');
        } else {
            nodeElement.classList.add('unhealthy');
        }
        
        // Add active/standby status
        if (isActive) {
            nodeElement.classList.add('active');
        } else {
            nodeElement.classList.add('standby');
        }
    }
    
    createResourceUtilizationChart() {
        // Create a donut chart for resource utilization
        const ctx = document.getElementById('resource-utilization-chart');
        if (!ctx) return;
        
        this.charts.resourceUtilization = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['Used Memory', 'Available Memory'],
                datasets: [{
                    data: [0, 100],
                    backgroundColor: [
                        '#ef4444',
                        '#e5e7eb'
                    ],
                    borderWidth: 0,
                    cutout: '70%'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const label = context.label;
                                const value = context.parsed;
                                return `${label}: ${value.toFixed(1)}%`;
                            }
                        }
                    }
                }
            }
        });
    }
    
    updateResourceUtilizationChart(metrics) {
        if (!this.charts.resourceUtilization || !metrics.performance_metrics) return;
        
        const perfMetrics = metrics.performance_metrics;
        const memoryUsage = perfMetrics.total_memory > 0 
            ? (perfMetrics.allocated_memory / perfMetrics.total_memory) * 100 
            : 0;
        
        const chart = this.charts.resourceUtilization;
        chart.data.datasets[0].data = [memoryUsage, 100 - memoryUsage];
        
        // Update colors based on usage
        if (memoryUsage > 90) {
            chart.data.datasets[0].backgroundColor[0] = '#ef4444'; // Red
        } else if (memoryUsage > 75) {
            chart.data.datasets[0].backgroundColor[0] = '#f59e0b'; // Orange
        } else {
            chart.data.datasets[0].backgroundColor[0] = '#10b981'; // Green
        }
        
        chart.update();
    }
    
    animateFailover(componentType, fromNode, toNode) {
        // Create an animated visualization of failover process
        const failoverContainer = document.getElementById('failover-animation');
        if (!failoverContainer) return;
        
        failoverContainer.innerHTML = `
            <div class="failover-animation">
                <div class="failover-step">
                    <div class="node ${componentType} active" id="failover-from">
                        <i class="fas fa-server"></i>
                        <span>${fromNode}</span>
                        <div class="node-status active">Active</div>
                    </div>
                    <div class="failover-arrow">
                        <i class="fas fa-arrow-right"></i>
                    </div>
                    <div class="node ${componentType} standby" id="failover-to">
                        <i class="fas fa-server"></i>
                        <span>${toNode}</span>
                        <div class="node-status standby">Standby</div>
                    </div>
                </div>
                <div class="failover-progress">
                    <div class="progress-bar">
                        <div class="progress-fill"></div>
                    </div>
                    <div class="progress-text">Initiating failover...</div>
                </div>
            </div>
        `;
        
        // Animate the failover process
        setTimeout(() => {
            const progressFill = failoverContainer.querySelector('.progress-fill');
            const progressText = failoverContainer.querySelector('.progress-text');
            const fromNodeEl = failoverContainer.querySelector('#failover-from');
            const toNodeEl = failoverContainer.querySelector('#failover-to');
            
            // Step 1: Stop active
            progressFill.style.width = '25%';
            progressText.textContent = 'Stopping active service...';
            fromNodeEl.classList.remove('active');
            fromNodeEl.classList.add('stopping');
            
            setTimeout(() => {
                // Step 2: Start standby
                progressFill.style.width = '50%';
                progressText.textContent = 'Starting standby service...';
                toNodeEl.classList.remove('standby');
                toNodeEl.classList.add('starting');
                
                setTimeout(() => {
                    // Step 3: Verify new active
                    progressFill.style.width = '75%';
                    progressText.textContent = 'Verifying new active...';
                    toNodeEl.classList.remove('starting');
                    toNodeEl.classList.add('active');
                    
                    setTimeout(() => {
                        // Step 4: Complete
                        progressFill.style.width = '100%';
                        progressText.textContent = 'Failover completed successfully!';
                        fromNodeEl.classList.remove('stopping');
                        fromNodeEl.classList.add('standby');
                        
                        // Hide animation after 3 seconds
                        setTimeout(() => {
                            failoverContainer.style.display = 'none';
                        }, 3000);
                    }, 1000);
                }, 1500);
            }, 1500);
        }, 500);
    }
    
    // Cleanup method
    destroy() {
        Object.values(this.charts).forEach(chart => {
            if (chart) {
                chart.destroy();
            }
        });
        this.charts = {};
    }
}

// Global charts instance
window.dashboardCharts = null;

// Initialize charts when dashboard is ready
document.addEventListener('DOMContentLoaded', () => {
    // Wait for Chart.js to be available
    if (typeof Chart !== 'undefined') {
        window.dashboardCharts = new DashboardCharts();
        
        // Configure Chart.js defaults
        Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu, Cantarell, sans-serif';
        Chart.defaults.color = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary');
        Chart.defaults.borderColor = getComputedStyle(document.documentElement).getPropertyValue('--border-color');
        Chart.defaults.backgroundColor = getComputedStyle(document.documentElement).getPropertyValue('--background-color');
    }
});

// Update charts when metrics are updated
document.addEventListener('metricsUpdated', (event) => {
    if (window.dashboardCharts) {
        window.dashboardCharts.updateCharts(event.detail);
        window.dashboardCharts.updateTopologyVisualization(event.detail);
        window.dashboardCharts.updateResourceUtilizationChart(event.detail);
    }
});

// Cleanup on page unload
window.addEventListener('beforeunload', () => {
    if (window.dashboardCharts) {
        window.dashboardCharts.destroy();
    }
});