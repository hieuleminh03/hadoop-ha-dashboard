// File Manager for HDFS operations
class HDFSFileManager {
    constructor() {
        this.currentPath = '/';
        this.selectedFiles = [];
        
        this.init();
    }
    
    init() {
        this.setupEventListeners();
        this.loadDirectory('/');
    }
    
    setupEventListeners() {
        // File upload button
        document.getElementById('upload-file-btn').addEventListener('click', () => {
            this.showUploadModal();
        });
        
        // Download URL button
        document.getElementById('download-url-btn').addEventListener('click', () => {
            this.showDownloadUrlModal();
        });
        
        // Refresh button
        document.getElementById('refresh-files-btn').addEventListener('click', () => {
            this.loadDirectory(this.currentPath);
        });
        
        // Upload modal
        this.setupUploadModal();
        
        // Download URL modal
        this.setupDownloadUrlModal();
        
        // File input change
        document.getElementById('file-input').addEventListener('change', (e) => {
            this.handleFileSelection(e.target.files);
        });
        
        // Drag and drop
        this.setupDragAndDrop();
    }
    
    setupUploadModal() {
        const modal = document.getElementById('upload-modal');
        const confirmBtn = document.getElementById('upload-confirm-btn');
        const pathInput = document.getElementById('upload-path-input');
        
        confirmBtn.addEventListener('click', () => {
            this.uploadFiles();
        });
        
        // Update path input with current path when modal opens
        document.getElementById('upload-file-btn').addEventListener('click', () => {
            pathInput.value = this.currentPath;
        });
    }
    
    setupDownloadUrlModal() {
        const modal = document.getElementById('download-url-modal');
        const confirmBtn = document.getElementById('download-confirm-btn');
        const pathInput = document.getElementById('download-path-input');
        
        confirmBtn.addEventListener('click', () => {
            this.downloadFromUrl();
        });
        
        // Update path input with current path when modal opens
        document.getElementById('download-url-btn').addEventListener('click', () => {
            pathInput.value = this.currentPath;
        });
    }
    
    setupDragAndDrop() {
        const dropZone = document.querySelector('.upload-drop-zone');
        const fileInput = document.getElementById('file-input');
        
        dropZone.addEventListener('click', () => {
            fileInput.click();
        });
        
        dropZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            dropZone.classList.add('dragover');
        });
        
        dropZone.addEventListener('dragleave', (e) => {
            e.preventDefault();
            dropZone.classList.remove('dragover');
        });
        
        dropZone.addEventListener('drop', (e) => {
            e.preventDefault();
            dropZone.classList.remove('dragover');
            
            const files = e.dataTransfer.files;
            this.handleFileSelection(files);
        });
    }
    
    async loadDirectory(path) {
        try {
            this.showLoading();
            
            const response = await fetch(`/api/hdfs/browse?path=${encodeURIComponent(path)}`);
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            
            this.currentPath = data.path;
            this.updateBreadcrumb(data.path);
            this.renderFileList(data.contents || []);
            
        } catch (error) {
            console.error('Error loading directory:', error);
            this.showToast('Error', `Failed to load directory: ${error.message}`, 'error');
        } finally {
            this.hideLoading();
        }
    }
    
    updateBreadcrumb(path) {
        const breadcrumbPath = document.getElementById('breadcrumb-path');
        
        // Create clickable breadcrumb
        const pathParts = path.split('/').filter(part => part !== '');
        let currentPath = '';
        
        let breadcrumbHTML = '<span class="breadcrumb-segment" data-path="/">/</span>';
        
        pathParts.forEach((part, index) => {
            currentPath += `/${part}`;
            breadcrumbHTML += `<span class="breadcrumb-separator">/</span>`;
            breadcrumbHTML += `<span class="breadcrumb-segment" data-path="${currentPath}">${part}</span>`;
        });
        
        breadcrumbPath.innerHTML = breadcrumbHTML;
        
        // Add click listeners to breadcrumb segments
        breadcrumbPath.querySelectorAll('.breadcrumb-segment').forEach(segment => {
            segment.addEventListener('click', () => {
                const targetPath = segment.dataset.path;
                this.loadDirectory(targetPath);
            });
        });
    }
    
    renderFileList(contents) {
        const fileList = document.getElementById('file-list');
        
        if (contents.length === 0) {
            fileList.innerHTML = `
                <div class="empty-directory">
                    <i class="fas fa-folder-open"></i>
                    <p>This directory is empty</p>
                </div>
            `;
            return;
        }
        
        // Sort contents: directories first, then files
        const sortedContents = contents.sort((a, b) => {
            if (a.type === 'DIRECTORY' && b.type !== 'DIRECTORY') return -1;
            if (a.type !== 'DIRECTORY' && b.type === 'DIRECTORY') return 1;
            return a.pathSuffix.localeCompare(b.pathSuffix);
        });
        
        fileList.innerHTML = sortedContents.map(item => this.renderFileItem(item)).join('');
        
        // Add click listeners
        fileList.querySelectorAll('.file-item').forEach(item => {
            item.addEventListener('click', () => {
                const path = item.dataset.path;
                const type = item.dataset.type;
                
                if (type === 'DIRECTORY') {
                    this.loadDirectory(path);
                } else {
                    this.selectFile(item);
                }
            });
            
            item.addEventListener('dblclick', () => {
                const path = item.dataset.path;
                const type = item.dataset.type;
                
                if (type === 'FILE') {
                    this.downloadFile(path);
                }
            });
        });
    }
    
    renderFileItem(item) {
        const isDirectory = item.type === 'DIRECTORY';
        const fullPath = this.currentPath === '/' ? `/${item.pathSuffix}` : `${this.currentPath}/${item.pathSuffix}`;
        
        const icon = isDirectory ? 'fas fa-folder' : this.getFileIcon(item.pathSuffix);
        const size = isDirectory ? '--' : this.formatFileSize(item.length);
        const modified = new Date(item.modificationTime).toLocaleString();
        const permissions = item.permission;
        const owner = item.owner;
        const group = item.group;
        
        return `
            <div class="file-item" data-path="${fullPath}" data-type="${item.type}">
                <i class="file-icon ${icon}"></i>
                <div class="file-info">
                    <div class="file-name">${item.pathSuffix}</div>
                    <div class="file-details">
                        ${owner}:${group} | ${permissions} | Modified: ${modified}
                    </div>
                </div>
                <div class="file-size">${size}</div>
                <div class="file-actions">
                    ${!isDirectory ? `
                        <button class="btn btn-sm btn-secondary" onclick="fileManager.downloadFile('${fullPath}')">
                            <i class="fas fa-download"></i>
                        </button>
                    ` : ''}
                    <button class="btn btn-sm btn-warning" onclick="fileManager.deleteFile('${fullPath}')">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            </div>
        `;
    }
    
    getFileIcon(filename) {
        const extension = filename.split('.').pop().toLowerCase();
        
        const iconMap = {
            'txt': 'fas fa-file-alt',
            'log': 'fas fa-file-alt',
            'csv': 'fas fa-file-csv',
            'json': 'fas fa-file-code',
            'xml': 'fas fa-file-code',
            'java': 'fas fa-file-code',
            'py': 'fas fa-file-code',
            'js': 'fas fa-file-code',
            'html': 'fas fa-file-code',
            'css': 'fas fa-file-code',
            'jpg': 'fas fa-file-image',
            'jpeg': 'fas fa-file-image',
            'png': 'fas fa-file-image',
            'gif': 'fas fa-file-image',
            'pdf': 'fas fa-file-pdf',
            'zip': 'fas fa-file-archive',
            'tar': 'fas fa-file-archive',
            'gz': 'fas fa-file-archive',
            'jar': 'fas fa-file-archive'
        };
        
        return iconMap[extension] || 'fas fa-file';
    }
    
    formatFileSize(bytes) {
        if (bytes === 0) return '0 B';
        
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }
    
    selectFile(fileItem) {
        // Toggle selection
        fileItem.classList.toggle('selected');
        
        const path = fileItem.dataset.path;
        const index = this.selectedFiles.indexOf(path);
        
        if (index > -1) {
            this.selectedFiles.splice(index, 1);
        } else {
            this.selectedFiles.push(path);
        }
        
        this.updateSelectionCount();
    }
    
    updateSelectionCount() {
        // Update UI to show selected file count
        const count = this.selectedFiles.length;
        const selectionInfo = document.getElementById('selection-info');
        
        if (selectionInfo) {
            if (count > 0) {
                selectionInfo.textContent = `${count} file${count !== 1 ? 's' : ''} selected`;
                selectionInfo.style.display = 'block';
            } else {
                selectionInfo.style.display = 'none';
            }
        }
    }
    
    showUploadModal() {
        const modal = document.getElementById('upload-modal');
        modal.classList.add('active');
    }
    
    showDownloadUrlModal() {
        const modal = document.getElementById('download-url-modal');
        modal.classList.add('active');
    }
    
    handleFileSelection(files) {
        const fileInput = document.getElementById('file-input');
        const dropZone = document.querySelector('.upload-drop-zone');
        
        if (files.length > 0) {
            fileInput.files = files;
            
            // Update drop zone text
            const fileNames = Array.from(files).map(f => f.name).join(', ');
            dropZone.querySelector('p').textContent = `Selected: ${fileNames}`;
            
            // Show upload modal if not already visible
            const modal = document.getElementById('upload-modal');
            if (!modal.classList.contains('active')) {
                this.showUploadModal();
            }
        }
    }
    
    async uploadFiles() {
        const fileInput = document.getElementById('file-input');
        const pathInput = document.getElementById('upload-path-input');
        const files = fileInput.files;
        
        if (files.length === 0) {
            this.showToast('Warning', 'Please select files to upload', 'warning');
            return;
        }
        
        const uploadPath = pathInput.value || this.currentPath;
        
        try {
            this.showLoading();
            
            for (const file of files) {
                const formData = new FormData();
                formData.append('file', file);
                
                const response = await fetch(`/api/hdfs/upload?path=${encodeURIComponent(uploadPath)}`, {
                    method: 'POST',
                    body: formData
                });
                
                if (!response.ok) {
                    throw new Error(`Failed to upload ${file.name}: ${response.statusText}`);
                }
                
                const result = await response.json();
                this.showToast('Success', `Uploaded ${file.name} successfully`, 'success');
            }
            
            // Close modal and refresh directory
            this.closeUploadModal();
            this.loadDirectory(this.currentPath);
            
        } catch (error) {
            console.error('Upload error:', error);
            this.showToast('Error', `Upload failed: ${error.message}`, 'error');
        } finally {
            this.hideLoading();
        }
    }
    
    async downloadFromUrl() {
        const urlInput = document.getElementById('download-url-input');
        const pathInput = document.getElementById('download-path-input');
        
        const sourceUrl = urlInput.value.trim();
        const destinationPath = pathInput.value || this.currentPath;
        
        if (!sourceUrl) {
            this.showToast('Warning', 'Please enter a URL', 'warning');
            return;
        }
        
        try {
            this.showLoading();
            
            const response = await fetch('/api/hdfs/download-url', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    url: sourceUrl,
                    hdfs_path: destinationPath
                })
            });
            
            if (!response.ok) {
                throw new Error(`Download failed: ${response.statusText}`);
            }
            
            const result = await response.json();
            this.showToast('Success', `Downloaded ${result.filename} successfully`, 'success');
            
            // Close modal and refresh directory
            this.closeDownloadUrlModal();
            this.loadDirectory(this.currentPath);
            
        } catch (error) {
            console.error('Download error:', error);
            this.showToast('Error', `Download failed: ${error.message}`, 'error');
        } finally {
            this.hideLoading();
        }
    }
    
    async downloadFile(filePath) {
        try {
            // For HDFS file download, we would typically use WebHDFS API
            const downloadUrl = `/api/hdfs/download?path=${encodeURIComponent(filePath)}`;
            
            // Create a temporary link and trigger download
            const link = document.createElement('a');
            link.href = downloadUrl;
            link.download = filePath.split('/').pop();
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            
            this.showToast('Info', 'Download started', 'info');
            
        } catch (error) {
            console.error('Download error:', error);
            this.showToast('Error', `Download failed: ${error.message}`, 'error');
        }
    }
    
    async deleteFile(filePath) {
        if (!confirm(`Are you sure you want to delete ${filePath}?`)) {
            return;
        }
        
        try {
            this.showLoading();
            
            const response = await fetch(`/api/hdfs/delete?path=${encodeURIComponent(filePath)}`, {
                method: 'DELETE'
            });
            
            if (!response.ok) {
                throw new Error(`Delete failed: ${response.statusText}`);
            }
            
            this.showToast('Success', 'File deleted successfully', 'success');
            this.loadDirectory(this.currentPath);
            
        } catch (error) {
            console.error('Delete error:', error);
            this.showToast('Error', `Delete failed: ${error.message}`, 'error');
        } finally {
            this.hideLoading();
        }
    }
    
    closeUploadModal() {
        const modal = document.getElementById('upload-modal');
        modal.classList.remove('active');
        
        // Reset form
        document.getElementById('file-input').value = '';
        document.querySelector('.upload-drop-zone p').textContent = 'Drag & drop files here or click to select';
    }
    
    closeDownloadUrlModal() {
        const modal = document.getElementById('download-url-modal');
        modal.classList.remove('active');
        
        // Reset form
        document.getElementById('download-url-input').value = '';
    }
    
    createDirectory() {
        const dirName = prompt('Enter directory name:');
        if (!dirName) return;
        
        // Implementation for creating directory
        this.showToast('Info', 'Directory creation not implemented yet', 'info');
    }
    
    showToast(title, message, type) {
        // Use the dashboard's toast system
        if (window.dashboard) {
            window.dashboard.showToast(title, message, type);
        }
    }
    
    showLoading() {
        if (window.dashboard) {
            window.dashboard.showLoading();
        }
    }
    
    hideLoading() {
        if (window.dashboard) {
            window.dashboard.hideLoading();
        }
    }
}

// Initialize file manager
window.fileManager = null;

document.addEventListener('DOMContentLoaded', () => {
    window.fileManager = new HDFSFileManager();
});

// Add CSS for file manager specific styles
const fileManagerStyles = `
    .breadcrumb-segment {
        cursor: pointer;
        padding: 0.25rem 0.5rem;
        border-radius: 0.25rem;
        transition: background-color 0.2s ease;
    }
    
    .breadcrumb-segment:hover {
        background: var(--background-color);
    }
    
    .breadcrumb-separator {
        margin: 0 0.25rem;
        color: var(--text-secondary);
    }
    
    .file-item {
        transition: background-color 0.2s ease;
    }
    
    .file-item:hover {
        background: var(--background-color);
    }
    
    .file-item.selected {
        background: rgba(37, 99, 235, 0.1);
        border-color: var(--primary-color);
    }
    
    .file-actions {
        display: none;
        gap: 0.5rem;
    }
    
    .file-item:hover .file-actions {
        display: flex;
    }
    
    .upload-drop-zone.dragover {
        border-color: var(--primary-color);
        background: rgba(37, 99, 235, 0.05);
    }
    
    .empty-directory {
        text-align: center;
        padding: 4rem 2rem;
        color: var(--text-secondary);
    }
    
    .empty-directory i {
        font-size: 3rem;
        margin-bottom: 1rem;
        opacity: 0.5;
    }
    
    .selection-info {
        padding: 0.5rem 1rem;
        background: var(--primary-color);
        color: white;
        border-radius: 0.375rem;
        font-size: 0.875rem;
        display: none;
    }
`;

// Inject styles
const styleElement = document.createElement('style');
styleElement.textContent = fileManagerStyles;
document.head.appendChild(styleElement);