document.getElementById('source').addEventListener('change', function() {
    const placeholder = this.value === 'pitr' 
        ? 'YYYY-MM-DDTHH:MM:SSZ' 
        : 'gs://[BUCKET_NAME]/[DIRECTORY]';
    const backupSourceInput = document.getElementById('backupSource');
    backupSourceInput.placeholder = placeholder;

    // Clear any existing value when switching source types
    backupSourceInput.value = '';
    updateExplorerButtonVisibility();
});

// Add event listeners for database fields
document.getElementById('sourceProjectId').addEventListener('change', updateExplorerButtonVisibility);
document.getElementById('sourceDb').addEventListener('change', updateExplorerButtonVisibility);
document.getElementById('destProjectId').addEventListener('change', updateExplorerButtonVisibility);
document.getElementById('destDb').addEventListener('change', updateExplorerButtonVisibility);

function updateExplorerButtonVisibility() {
    const sourceProjectId = document.getElementById('sourceProjectId').value;
    const destProjectId = document.getElementById('destProjectId').value;
    const showExplorerButton = document.getElementById('showExplorerButton');
    
    if (sourceProjectId && destProjectId) {
        showExplorerButton.style.display = 'inline-block';
    } else {
        showExplorerButton.style.display = 'none';
    }
}

function showExplorer() {
    const sourceProjectId = document.getElementById('sourceProjectId').value;
    const sourceDb = document.getElementById('sourceDb').value || '(default)';
    const destProjectId = document.getElementById('destProjectId').value;
    const destDb = document.getElementById('destDb').value || 'korl-backup-db';

    // Update the explorer section with database information
    document.getElementById('sourceProject').textContent = sourceProjectId;
    document.getElementById('sourceDb').textContent = sourceDb;
    document.getElementById('destProject').textContent = destProjectId;
    document.getElementById('destDbInfo').textContent = destDb;

    // Show the explorer section
    document.getElementById('explorerSection').style.display = 'block';

    // Load both databases
    explorer.loadDatabase(sourceProjectId, sourceDb, true);
    explorer.loadDatabase(destProjectId, destDb, false);
}

// Add event listener for source project ID changes
document.getElementById('sourceProjectId').addEventListener('change', function() {
    const source = document.getElementById('source').value;
    if (source === 'gcs') {
        const backupSourceInput = document.getElementById('backupSource');
        // Only list backups if a bucket path is already entered
        if (backupSourceInput.value.startsWith('gs://')) {
            listBackups();
        }
    }
});

// Add event listener for backup source changes
document.getElementById('backupSource').addEventListener('change', function() {
    const source = document.getElementById('source').value;
    if (source === 'gcs' && this.value.startsWith('gs://')) {
        listBackups();
    }
});

async function listBackups() {
    showStatus('Listing backups and collections...', 'info');

    const sourceProjectId = document.getElementById('sourceProjectId').value;
    const sourceDb = document.getElementById('sourceDb').value || '(default)';
    const source = document.getElementById('source').value;
    const backupSource = document.getElementById('backupSource').value;
    const sourceServiceAccountKey = document.getElementById('sourceServiceAccountKey').value;

    if (!sourceProjectId || !backupSource) {
        showStatus('Please fill in all required fields', 'error');
        return;
    }

    try {
        const response = await fetch('/list-backups', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            body: new URLSearchParams({
                sourceProjectId,
                sourceDb,
                source,
                backupSource,
                serviceAccountKey: sourceServiceAccountKey
            })
        });

        const data = await response.json();
        
        if (data.error) {
            showStatus(data.error, 'error');
            return;
        }

        const backupList = document.getElementById('backupList');
        backupList.innerHTML = '';

        // Update label based on source
        const backupListLabel = document.getElementById('backupListLabel');
        if (source === 'pitr') {
            backupListLabel.textContent = 'Collections Found:';
        } else {
            backupListLabel.textContent = 'Available Backups:';
        }

        // Ensure backups is an array before using forEach
        const backups = Array.isArray(data.backups) ? data.backups : [];
        
        if (backups.length === 0) {
            showStatus(`No ${source === 'pitr' ? 'collections' : 'backups'} found`, 'info');
            return;
        }

        backups.forEach(backup => {
            const option = document.createElement('option');
            option.value = backup;
            option.textContent = backup;
            backupList.appendChild(option);
        });

        // Automatically select the first backup in the list
        if (backups.length > 0) {
            backupList.value = backups[0];
            showRestoreOptions(backups[0]);
        }

        document.getElementById('backupListSection').style.display = 'block';

        // For PITR, show restore button immediately and hide granularity section
        if (source === 'pitr') {
            document.getElementById('granularitySection').style.display = 'none';
            document.getElementById('restoreButton').style.display = 'block';
            document.getElementById('restoreButton').disabled = false;
            // Set granularity to full by default
            document.getElementById('granularity').value = 'full';
            // Hide collection section
            document.getElementById('collectionSection').style.display = 'none';
        } else {
            document.getElementById('granularitySection').style.display =
                data.granularRestorePossible ? 'block' : 'none';
            document.getElementById('restoreButton').style.display = 'block';
            document.getElementById('restoreButton').disabled = false;
        }

        // Re-enable cancel button and ensure it's hidden
        const cancelButton = document.getElementById('cancelButton');
        cancelButton.disabled = false;
        cancelButton.style.display = 'none';

        showStatus(`Successfully found ${backups.length} ${source === 'pitr' ? 'collections' : 'backups'}`, 'success');
    } catch (error) {
        console.error('Error listing backups:', error);
        showStatus('Error listing backups: ' + error, 'error');
    }
}

function showRestoreOptions(selectedValue) {
    const source = document.getElementById('source').value;
    const granularitySection = document.getElementById('granularitySection');
    const restoreButton = document.getElementById('restoreButton');

    if (!selectedValue) {
        restoreButton.disabled = true;
        return;
    }

    // For PITR, show restore button immediately
    if (source === 'pitr') {
        // Show restore button immediately
        restoreButton.style.display = 'block';
        restoreButton.disabled = false;

        // Only show granularity options if a collection is selected
        const match = selectedValue.match(/PITR - .* \(Collection: (.*)\)/);
        if (match) {
            const collectionId = match[1];
            // Show granularity options
            granularitySection.style.display = 'block';
            // Auto-fill collection ID
            document.getElementById('collectionId').value = collectionId;
            // If we're in document mode, trigger document loading
            if (document.getElementById('granularity').value === 'document') {
                loadDocuments();
            }
        }
    } else {
        // For GCS backups
        granularitySection.style.display = 'block';
        restoreButton.disabled = false;
    }

    restoreButton.style.display = 'block';
}

// Add collection input handler at the document level
document.addEventListener('DOMContentLoaded', function() {
    console.log('Setting up collection input handler');
    const collectionInput = document.getElementById('collectionId');
    if (!collectionInput) {
        console.error('Could not find collectionId input element');
        return;
    }
    
    collectionInput.addEventListener('change', function(event) {
        console.log('Collection input changed:', event.target.value);
        const granularity = document.getElementById('granularity').value;
        console.log('Current granularity:', granularity);
        if (granularity === 'document') {
            console.log('Triggering loadDocuments');
            loadDocuments();
        }
    });
});

function toggleGranularityInputs() {
    const granularity = document.getElementById('granularity').value;
    console.log('Toggling granularity inputs:', granularity);
    
    const collectionSection = document.getElementById('collectionSection');
    const documentSection = document.getElementById('documentSection');
    
    console.log('Collection section display before:', collectionSection.style.display);
    collectionSection.style.display = granularity === 'collection' || granularity === 'document' ? 'block' : 'none';
    console.log('Collection section display after:', collectionSection.style.display);
    
    documentSection.style.display = 'none';

    if (granularity === 'document') {
        const collectionId = document.getElementById('collectionId').value;
        console.log('Current collection ID:', collectionId);
        if (collectionId) {
            console.log('Collection ID exists, loading documents');
            loadDocuments();
        }
    }
}

async function loadDocuments() {
    console.log('loadDocuments called');
    const sourceProjectId = document.getElementById('sourceProjectId').value;
    const sourceDb = document.getElementById('sourceDb').value || '(default)';
    const collectionId = document.getElementById('collectionId').value;
    const granularity = document.getElementById('granularity').value;
    
    console.log('Loading documents with params:', {
        sourceProjectId,
        sourceDb,
        collectionId,
        granularity,
        granularityDisplay: document.getElementById('granularitySection').style.display,
        collectionDisplay: document.getElementById('collectionSection').style.display,
        documentDisplay: document.getElementById('documentSection').style.display
    });
    
    if (!sourceProjectId || !collectionId) {
        console.log('Missing required fields:', { sourceProjectId, collectionId });
        showStatus('Please fill in project ID and collection ID', 'error');
        return;
    }

    showStatus('Loading documents...', 'info');

    try {
        const requestBody = {
            projectId: sourceProjectId,
            dbId: sourceDb,
            collection: collectionId
        };
        console.log('Sending request to list-documents:', requestBody);

        const response = await fetch('/list-documents', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(requestBody)
        });

        const data = await response.json();
        console.log('Server response:', data);
        
        if (data.error) {
            console.error('Server returned error:', data.error);
            showStatus(data.error, 'error');
            return;
        }

        // Remove any existing search box
        const existingSearch = document.querySelector('.document-search');
        if (existingSearch) {
            console.log('Removing existing search box');
            existingSearch.remove();
        }

        const documentSection = document.getElementById('documentSection');
        const documentSelect = document.getElementById('documentId');
        
        if (!documentSection || !documentSelect) {
            console.error('Could not find document section elements:', {
                documentSection: !!documentSection,
                documentSelect: !!documentSelect
            });
            return;
        }
        
        console.log('Setting up document select');
        // Clear and initialize the document select
        documentSelect.innerHTML = '<option value="">Select a document</option>';
        
        // Sort documents by ID for easier navigation
        const sortedDocs = data.documents.sort((a, b) => a.id.localeCompare(b.id));
        console.log(`Adding ${sortedDocs.length} documents to select`);
        
        sortedDocs.forEach((doc, index) => {
            console.log(`Adding document ${index + 1}:`, { id: doc.id, path: doc.path });
            const option = document.createElement('option');
            option.value = doc.id;
            option.textContent = doc.id;
            option.title = doc.path; // Add full path as tooltip
            documentSelect.appendChild(option);
        });

        console.log('Making document section visible');
        // Make sure the document section is visible
        documentSection.style.display = 'block';
        documentSelect.style.display = 'block';

        // Adjust the size of the select based on number of documents
        documentSelect.size = Math.min(10, data.documents.length + 1);
        console.log('Select size set to:', documentSelect.size);

        showStatus(`Loaded ${data.documents.length} documents`, 'success');
    } catch (error) {
        console.error('Error in loadDocuments:', error);
        showStatus('Error loading documents: ' + error, 'error');
    }
}

// Add event listener for the restore button
document.addEventListener('DOMContentLoaded', function() {
    const restoreBtn = document.getElementById('restoreBtn');
    if (restoreBtn) {
        restoreBtn.addEventListener('click', restore);
    }
});

let currentWs = null;  // Store the WebSocket connection globally

async function restore() {
    const sourceProjectId = document.getElementById('sourceProjectId').value;
    const sourceDb = document.getElementById('sourceDb').value || '(default)';
    const destProjectId = document.getElementById('destProjectId').value;
    const destDb = document.getElementById('destDb').value || 'korl-backup-db';
    const source = document.getElementById('source').value;
    const granularity = document.getElementById('granularity').value;
    const collectionId = document.getElementById('collectionId').value;
    const documentId = document.getElementById('documentId').value;
    const userEmail = document.getElementById('userEmail').value;
    const sourceServiceAccountKey = document.getElementById('sourceServiceAccountKey').value;
    const destServiceAccountKey = document.getElementById('destServiceAccountKey').value || sourceServiceAccountKey;
    let backupSource = document.getElementById('backupSource').value;

    // For GCS restores, use the selected backup from the list
    if (source === 'gcs') {
        const backupList = document.getElementById('backupList');
        const selectedBackup = backupList.value;
        if (!selectedBackup) {
            showStatus('Please select a backup from the list', 'error');
            return;
        }
        backupSource = selectedBackup;
    }

    // Validate required fields
    if (!sourceProjectId || !destProjectId || !backupSource) {
        showStatus('Please fill in all required fields', 'error');
        return;
    }

    // Validate collection/document selection based on granularity
    // Skip collection validation for PITR when granularity is 'full'
    if (!(source === 'pitr' && granularity === 'full')) {
        if (granularity === 'collection' && !collectionId) {
            showStatus('Please enter a collection ID', 'error');
            return;
        }
        if (granularity === 'document' && (!collectionId || !documentId)) {
            showStatus('Please select both a collection and a document', 'error');
            return;
        }
    }

    // Construct the WebSocket URL
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsURL = `${protocol}//${window.location.host}/ws`;

    showStatus('Starting restore operation...', 'info');

    // Create WebSocket connection
    currentWs = new WebSocket(wsURL);
    
    currentWs.onopen = function() {
        console.log('WebSocket connection established');
        // Send restore request
        currentWs.send(JSON.stringify({
            type: 'restore',
            sourceProjectId: sourceProjectId,
            sourceDb: sourceDb,
            destProjectId: destProjectId,
            destDb: destDb,
            source: source,
            backupSource: backupSource,
            granularity: granularity,
            collectionId: collectionId,
            documentId: documentId,
            userEmail: userEmail,
            sourceServiceAccountKey: sourceServiceAccountKey,
            destServiceAccountKey: destServiceAccountKey
        }));
    };
    
    currentWs.onmessage = function(event) {
        showProgress(event.data);
    };
    
    currentWs.onerror = function(error) {
        console.error('WebSocket error:', error);
        showProgress('Error: WebSocket connection failed');
        document.getElementById('restoreButton').disabled = false;
        document.getElementById('cancelButton').style.display = 'none';
    };
    
    currentWs.onclose = function(event) {
        console.log('WebSocket connection closed:', event);
        
        // Check if the close was clean and expected
        if (event.wasClean) {
            // If we have a completion code, show any final message
            if (event.code === 1000) {
                showProgress('Operation completed successfully.');
            }
        } else {
            // If the connection was not closed cleanly, show an error
            showProgress('Connection closed unexpectedly. Check server logs for complete details.');
        }
        
        document.getElementById('cancelButton').style.display = 'none';
        document.getElementById('restoreButton').disabled = false;
    };

    // Disable the restore button and show cancel button
    document.getElementById('restoreButton').disabled = true;
    const cancelButton = document.getElementById('cancelButton');
    cancelButton.style.display = 'inline-block';
    cancelButton.disabled = false;
}

async function cancelRestore() {
    if (currentWs && currentWs.readyState === WebSocket.OPEN) {
        currentWs.send(JSON.stringify({ type: 'cancel' }));
        showStatus('Cancelling restore operation...', 'info');
        document.getElementById('cancelButton').disabled = true;
        document.getElementById('restoreButton').disabled = false;
    } else {
        showStatus('No active restore operation to cancel', 'error');
        document.getElementById('cancelButton').style.display = 'none';
    }
}

function showProgress(message) {
    console.log('Received WebSocket message:', message);
    try {
        // Try to parse as JSON in case it's a structured message
        const data = JSON.parse(message);
        if (data.message) {
            console.log('Processing message:', data.message);
            showStatus(data.message, data.type || 'info');
            
            // Only close on explicit completion signal
            if (data.type === 'complete' || data.status === 'complete') {
                console.log('Received completion signal, closing connection');
                if (currentWs && currentWs.readyState === WebSocket.OPEN) {
                    currentWs.close(1000, 'Operation completed');
                }
            }
        } else {
            showStatus(JSON.stringify(data), 'info');
        }
    } catch (e) {
        // If not JSON, show as plain text
        console.log('Processing plain text message:', message);
        showStatus(message, message.includes('Error:') ? 'error' : 'info');
        
        // Don't auto-close the connection for plain text messages
        // Let the server explicitly send a completion signal
    }
}

// Add function to check for active restores
async function checkActiveRestores() {
    try {
        const response = await fetch('/list-restores');
        const data = await response.json();
        
        if (data.restores && data.restores.length > 0) {
            // Show active restores
            const activeRestores = data.restores.map(r => `
                <div class="restore-item">
                    <strong>Restore Operation</strong><br>
                    Started: ${new Date(r.startTime).toLocaleString()}<br>
                    Status: <span class="status-badge ${r.status}">${r.status}</span><br>
                    Source: ${r.sourceProject}<br>
                    Destination: ${r.destProject} (${r.destDb})<br>
                    ${r.status === 'running' ? 
                        '<div class="info-message">Restore is running in the background. Check server logs for progress.</div>' : ''}
                    ${r.error ? `<div class="error-message">${r.error}</div>` : ''}
                </div>
            `).join('');
            
            showStatus(`Active restores:<br>${activeRestores}`, 'info');
        }
    } catch (error) {
        console.error('Error checking active restores:', error);
    }
}

// Add function to reconnect to a specific restore
function reconnectToRestore(restoreId) {
    const eventSource = new EventSource(`/restore-logs?restoreId=${restoreId}`);
    
    showStatus('Restore is running in the background. Check server logs for detailed progress.', 'info');
    
    eventSource.onmessage = function(event) {
        const data = JSON.parse(event.data);
        
        // Only handle completion or failure status
        if (data.status === 'completed') {
            eventSource.close();
            showStatus('Restore completed successfully', 'success');
            showExplorerIfNeeded(data);
        } else if (data.status === 'failed') {
            eventSource.close();
            showStatus(`Restore failed: ${data.error}`, 'error');
        }
    };
    
    eventSource.onerror = function(error) {
        console.error('EventSource error:', error);
        // Just close the connection on error - restore continues in background
        eventSource.close();
        showStatus('Lost connection to server. Restore continues in the background - check server logs for progress.', 'info');
    };
}

// Helper function to show explorer section
function showExplorerIfNeeded(data) {
    if (data.sourceProject && data.destProject && data.destDb) {
        document.getElementById('explorerSection').style.display = 'block';
        
        // Update source info
        document.getElementById('sourceProject').textContent = data.sourceProject;
        document.getElementById('sourceDb').textContent = data.destDb;
        
        // Update destination info
        document.getElementById('destProject').textContent = data.destProject;
        document.getElementById('destDbInfo').textContent = data.destDb;
        
        // Load both databases
        explorer.loadDatabase(data.sourceProject, data.destDb, true);
        explorer.loadDatabase(data.destProject, data.destDb, false);
    }
}

// Check for active restores when the page loads
window.addEventListener('load', checkActiveRestores);

// Add timestamp validation helper
function isValidPITRTimestamp(timestamp) {
    const regex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/;
    if (!regex.test(timestamp)) {
        console.error('Timestamp format validation failed');
        return false;
    }
    
    const date = new Date(timestamp);
    if (isNaN(date.getTime())) {
        console.error('Invalid date value');
        return false;
    }
    
    console.log('Timestamp validation passed:', timestamp);
    return true;
}

function toggleStatusContainer(container) {
    container.classList.toggle('collapsed');
}

function showStatus(message, type = 'info') {
    const statusDiv = document.getElementById('status');
    const messagesContainer = statusDiv.querySelector('.status-messages');
    const newStatus = document.createElement('div');
    newStatus.className = `status ${type}`;
    
    // Add timestamp
    const timestamp = new Date().toLocaleTimeString();
    
    // Handle HTML content if present
    if (message.includes('<br>') || message.includes('<div>')) {
        newStatus.innerHTML = `[${timestamp}] ${message}`;
    } else {
        newStatus.textContent = `[${timestamp}] ${message}`;
    }
    
    messagesContainer.appendChild(newStatus);
    
    // Limit the number of status messages to prevent excessive memory usage
    while (messagesContainer.children.length > 100) {
        messagesContainer.removeChild(messagesContainer.firstChild);
    }

    // Auto-expand if collapsed and new message arrives
    if (statusDiv.classList.contains('collapsed')) {
        statusDiv.classList.remove('collapsed');
    }
}

// Initialize
const explorer = new DatabaseExplorer();

// Load destination database
async function loadDestination() {
    const projectId = document.getElementById('destProjectId').value;
    const dbId = document.getElementById('destDb').value || 'korl-backup-db';
    const destServiceAccountKey = document.getElementById('destServiceAccountKey').value;
    const sourceServiceAccountKey = document.getElementById('sourceServiceAccountKey').value;
    
    // Use destination key if provided, otherwise fall back to source key
    const serviceAccountKey = destServiceAccountKey || sourceServiceAccountKey;

    try {
        const response = await fetch('/list-collections', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                projectId: projectId,
                dbId: dbId,
                serviceAccountKey: serviceAccountKey
            })
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        if (data.error) {
            throw new Error(data.error);
        }

        updateDestinationTree(data);
        showStatus('Destination loaded successfully', 'success');
    } catch (error) {
        showStatus('Error: ' + error.message, 'error');
    }
}

// Load source database (called after PITR restore)
function loadSource(projectId, dbId) {
    document.getElementById('sourceProject').textContent = projectId;
    document.getElementById('sourceDb').textContent = dbId;
    explorer.loadDatabase(projectId, dbId, true);
}

// Make loadDocuments available globally
window.loadDocuments = loadDocuments; 