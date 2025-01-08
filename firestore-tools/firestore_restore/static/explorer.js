class DatabaseExplorer {
    constructor() {
        console.log('Initializing DatabaseExplorer');
        this.sourceTree = document.getElementById('sourceTree');
        this.destTree = document.getElementById('destTree');
        this.copyRightBtn = document.getElementById('copyRight');
        this.copyLeftBtn = document.getElementById('copyLeft');
        this.previewPanel = document.getElementById('previewPanel');
        this.documentPreview = document.getElementById('documentPreview');
        
        this.selectedSource = new Set();
        this.selectedDest = new Set();
        this.multiSelectMode = false;
        
        // Add tracking for last selected item and shift key state
        this.lastSelectedItem = null;
        this.lastSelectedSource = null;
        
        this.setupEventListeners();
        console.log('DatabaseExplorer initialized');
    }

    async loadDatabase(projectId, dbId, isSource = true) {
        const tree = isSource ? this.sourceTree : this.destTree;
        const serviceAccountKey = isSource ? 
            document.getElementById('sourceServiceAccountKey').value :
            document.getElementById('destServiceAccountKey').value || document.getElementById('sourceServiceAccountKey').value;

        try {
            const response = await fetch('/list-collections', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    projectId,
                    dbId,
                    serviceAccountKey
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            if (data.error) {
                throw new Error(data.error);
            }

            this.renderCollections(data.collections, tree);
            showStatus(`Successfully loaded ${isSource ? 'source' : 'destination'} database`, 'success');
        } catch (error) {
            console.error('Error loading database:', error);
            showStatus(`Error loading ${isSource ? 'source' : 'destination'} database: ${error.message}`, 'error');
        }
    }

    async loadDocuments(projectId, dbId, collection, isSource = true) {
        const serviceAccountKey = isSource ? 
            document.getElementById('sourceServiceAccountKey').value :
            document.getElementById('destServiceAccountKey').value || document.getElementById('sourceServiceAccountKey').value;

        try {
            const response = await fetch('/list-documents', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    projectId,
                    dbId,
                    collection,
                    serviceAccountKey
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            if (data.error) {
                throw new Error(data.error);
            }

            return data.documents;
        } catch (error) {
            console.error('Error loading documents:', error);
            showStatus(`Error loading documents: ${error.message}`, 'error');
            return [];
        }
    }

    async loadDocument(projectId, dbId, path, isSource = true) {
        const serviceAccountKey = isSource ? 
            document.getElementById('sourceServiceAccountKey').value :
            document.getElementById('destServiceAccountKey').value || document.getElementById('sourceServiceAccountKey').value;

        try {
            const response = await fetch('/get-document', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    projectId,
                    dbId,
                    path,
                    serviceAccountKey
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            if (data.error) {
                throw new Error(data.error);
            }

            return data.document;
        } catch (error) {
            console.error('Error loading document:', error);
            showStatus(`Error loading document: ${error.message}`, 'error');
            return null;
        }
    }

    renderTree(collections, isSource) {
        console.log(`Rendering tree for ${isSource ? 'source' : 'destination'}:`, collections);
        const tree = isSource ? this.sourceTree : this.destTree;
        tree.innerHTML = '';
        
        if (!collections || collections.length === 0) {
            const emptyMessage = document.createElement('div');
            emptyMessage.className = 'tree-empty-message';
            emptyMessage.textContent = 'No collections found';
            tree.appendChild(emptyMessage);
            return;
        }
        
        collections.forEach(collection => {
            const collectionNode = this.createCollectionNode(collection, isSource);
            tree.appendChild(collectionNode);
        });
    }

    createCollectionNode(collection, isSource) {
        console.log('Creating collection node:', {
            id: collection.id,
            path: collection.path,
            isSource: isSource,
            hasDocuments: !!collection.documents
        });

        const node = document.createElement('div');
        node.className = 'tree-item collection-item';
        if (this.multiSelectMode) {
            node.classList.add('multi-select-enabled');
        }
        node.setAttribute('data-path', collection.path);

        // Create a checkbox for selection
        const checkbox = document.createElement('div');
        checkbox.className = 'tree-checkbox';
        checkbox.addEventListener('click', (e) => {
            e.stopPropagation();
            this.toggleSelection(node, isSource, e.shiftKey);
        });
        node.appendChild(checkbox);

        // Create label for collection name
        const label = document.createElement('span');
        label.className = 'tree-label';
        label.textContent = collection.id;
        node.appendChild(label);

        // Create a container for documents
        const docsContainer = document.createElement('div');
        docsContainer.style.marginLeft = '20px';
        docsContainer.style.display = 'none';
        node.appendChild(docsContainer);

        // Add click handler for all collection nodes
        node.addEventListener('click', async (e) => {
            console.log('Collection clicked:', collection.id);
            
            // If in multi-select mode and clicking the label
            if (this.multiSelectMode && e.target === label) {
                this.toggleSelection(node, isSource, e.shiftKey);
            } 
            // If clicking the label (not in multi-select) or the node itself
            else if (e.target === label || e.target === node) {
                // Toggle documents visibility
                const currentDisplay = docsContainer.style.display;
                docsContainer.style.display = currentDisplay === 'none' ? 'block' : 'none';

                // Load documents if container is being shown and is empty
                if (currentDisplay === 'none' && docsContainer.children.length === 0) {
                    try {
                        const sourceProjectId = document.getElementById(isSource ? 'sourceProject' : 'destProject').textContent;
                        const sourceDb = document.getElementById(isSource ? 'sourceDb' : 'destDbInfo').textContent;
                        
                        console.log('Loading documents for:', {
                            projectId: sourceProjectId,
                            dbId: sourceDb,
                            collection: collection.id
                        });

                        const response = await fetch('/list-documents', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify({
                                projectId: sourceProjectId,
                                dbId: sourceDb,
                                collection: collection.id
                            })
                        });

                        const data = await response.json();
                        console.log('Received documents:', data);

                        if (data.error) {
                            console.error('Error listing documents:', data.error);
                            showStatus(data.error, 'error');
                            return;
                        }

                        // Sort and add documents to the container
                        const sortedDocs = data.documents.sort((a, b) => a.id.localeCompare(b.id));
                        sortedDocs.forEach(doc => {
                            const docNode = this.createDocumentNode({
                                id: doc.id,
                                path: doc.path,
                                data: doc.data
                            }, isSource);
                            docsContainer.appendChild(docNode);
                        });

                        showStatus(`Loaded ${data.documents.length} documents`, 'success');
                    } catch (error) {
                        console.error('Error loading documents:', error);
                        showStatus('Error loading documents: ' + error, 'error');
                    }
                }
            }
            e.stopPropagation();
        });

        return node;
    }

    createDocumentNode(doc, isSource) {
        const node = document.createElement('div');
        node.className = 'tree-item document-item';
        if (this.multiSelectMode) {
            node.classList.add('multi-select-enabled');
        }
        node.setAttribute('data-path', doc.path);

        // Create a checkbox for selection
        const checkbox = document.createElement('div');
        checkbox.className = 'tree-checkbox';
        checkbox.addEventListener('click', (e) => {
            e.stopPropagation();
            this.toggleSelection(node, isSource, e.shiftKey);
        });
        node.appendChild(checkbox);

        // Create label for document name
        const label = document.createElement('span');
        label.className = 'tree-label';
        label.textContent = doc.id;
        node.appendChild(label);

        node.addEventListener('click', async (e) => {
            e.stopPropagation();
            // If in multi-select mode and clicking the label
            if (this.multiSelectMode && e.target === label) {
                this.toggleSelection(node, isSource, e.shiftKey);
            } 
            // If clicking the label and not in multi-select mode
            else if (e.target === label && !this.multiSelectMode) {
                await this.showDocumentPreview(doc.path, isSource);
            }
        });

        return node;
    }

    async showDocumentPreview(path, isSource) {
        try {
            const response = await fetch('/get-document', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    projectId: isSource ? 
                        document.getElementById('sourceProject').textContent :
                        document.getElementById('destProject').textContent,
                    dbId: isSource ?
                        document.getElementById('sourceDb').textContent :
                        document.getElementById('destDbInfo').textContent,
                    path: path
                })
            });

            const data = await response.json();
            if (data.error) {
                showStatus(data.error, 'error');
                return;
            }

            this.documentPreview.textContent = JSON.stringify(data.document, null, 2);
            this.previewPanel.style.display = 'block';
        } catch (error) {
            showStatus(`Failed to load document: ${error}`, 'error');
        }
    }

    toggleSelection(item, isSource, isShiftClick = false) {
        const selected = isSource ? this.selectedSource : this.selectedDest;
        const path = item.getAttribute('data-path');

        if (isShiftClick && this.lastSelectedItem && this.lastSelectedSource === isSource) {
            // Get all tree items in the same container
            const container = item.closest('.tree-view');
            const items = Array.from(container.querySelectorAll('.tree-item'));
            
            // Find indices of current and last selected items
            const currentIndex = items.indexOf(item);
            const lastIndex = items.indexOf(this.lastSelectedItem);
            
            // Select all items between the two indices
            const start = Math.min(currentIndex, lastIndex);
            const end = Math.max(currentIndex, lastIndex);
            
            for (let i = start; i <= end; i++) {
                const currentItem = items[i];
                currentItem.classList.add('selected', 'multi-select-enabled');
                selected.add(currentItem.getAttribute('data-path'));
            }
        } else {
            // Regular selection toggle
            if (item.classList.contains('selected')) {
                item.classList.remove('selected');
                selected.delete(path);
                if (selected.size === 0 && !this.multiSelectMode) {
                    item.classList.remove('multi-select-enabled');
                }
            } else {
                item.classList.add('selected', 'multi-select-enabled');
                selected.add(path);
            }
        }

        // Update last selected item
        this.lastSelectedItem = item;
        this.lastSelectedSource = isSource;

        // Keep multi-select-enabled class on all selected items
        document.querySelectorAll('.tree-item.selected').forEach(selectedItem => {
            selectedItem.classList.add('multi-select-enabled');
        });

        this.updateTransferButtons();
    }

    updateTransferButtons() {
        this.copyRightBtn.disabled = this.selectedSource.size === 0;
        this.copyLeftBtn.disabled = this.selectedDest.size === 0;
    }

    async transferData(toRight = true) {
        const source = toRight ? this.selectedSource : this.selectedDest;
        const target = toRight ? 'destination' : 'source';
        const includeSubcollections = document.getElementById('includeSubcollections').checked;
        const overwriteExisting = document.getElementById('overwriteExisting').checked;
        
        showStatus(`Transferring data to ${target}...`, 'info');

        // Create WebSocket connection
        const ws = new WebSocket(`ws://${window.location.host}/ws`);
        
        ws.onopen = () => {
            // Send transfer request
            ws.send(JSON.stringify({
                type: 'transfer',
                paths: Array.from(source),
                sourceProjectId: document.getElementById('sourceProject').textContent,
                sourceDb: document.getElementById('sourceDb').textContent,
                destProjectId: document.getElementById('destProject').textContent,
                destDb: document.getElementById('destDbInfo').textContent,
                includeSubcollections,
                overwriteExisting
            }));
        };

        ws.onmessage = (event) => {
            const message = event.data;
            if (message.includes('Error:')) {
                showStatus(message, 'error');
            } else if (message.includes('Transfer completed')) {
                const match = message.match(/Total items transferred: (\d+)/);
                const count = match ? match[1] : '0';
                showStatus(`Successfully transferred ${count} items to ${target}`, 'success');
                
                // Reload both trees to show updated state
                this.loadDatabase(
                    document.getElementById('sourceProject').textContent,
                    document.getElementById('sourceDb').textContent,
                    true
                );
                this.loadDatabase(
                    document.getElementById('destProject').textContent,
                    document.getElementById('destDbInfo').textContent,
                    false
                );
                
                // Clear selections
                this.selectedSource.clear();
                this.selectedDest.clear();
                this.updateTransferButtons();
                
                // Close WebSocket
                ws.close();
            } else {
                showStatus(message, 'info');
            }
        };

        ws.onerror = (error) => {
            console.error('WebSocket error:', error);
            showStatus('Error: WebSocket connection failed', 'error');
        };

        ws.onclose = () => {
            console.log('WebSocket connection closed');
        };
    }

    setupEventListeners() {
        // Transfer buttons
        this.copyRightBtn.addEventListener('click', () => this.transferData(true));
        this.copyLeftBtn.addEventListener('click', () => this.transferData(false));

        // Preview panel close button
        document.querySelector('.close-preview')?.addEventListener('click', () => {
            this.previewPanel.style.display = 'none';
        });

        // Multi-select mode toggle (Ctrl/Cmd key)
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Control' || e.key === 'Meta') {
                this.multiSelectMode = true;
                document.querySelectorAll('.tree-item').forEach(item => {
                    item.classList.add('multi-select-enabled');
                });
            }
        });

        document.addEventListener('keyup', (e) => {
            if (e.key === 'Control' || e.key === 'Meta') {
                this.multiSelectMode = false;
                document.querySelectorAll('.tree-item').forEach(item => {
                    item.classList.remove('multi-select-enabled');
                });
            }
        });

        // Expand/Collapse all buttons
        document.querySelectorAll('.expand-all').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const panel = e.target.closest('.database-panel');
                panel.querySelectorAll('.tree-item div').forEach(container => {
                    container.style.display = 'block';
                });
            });
        });

        document.querySelectorAll('.collapse-all').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const panel = e.target.closest('.database-panel');
                panel.querySelectorAll('.tree-item div').forEach(container => {
                    container.style.display = 'none';
                });
            });
        });

        // Refresh buttons
        document.querySelectorAll('.refresh').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const panel = e.target.closest('.database-panel');
                const isSource = panel.querySelector('#sourceTree') !== null;
                const projectId = document.getElementById(isSource ? 'sourceProject' : 'destProject').textContent;
                const dbId = document.getElementById(isSource ? 'sourceDb' : 'destDbInfo').textContent;
                this.loadDatabase(projectId, dbId, isSource);
            });
        });

        // Add query event listeners
        document.querySelectorAll('.query-submit').forEach(button => {
            button.addEventListener('click', async (e) => {
                const panel = e.target.closest('.database-panel');
                const isSource = panel.querySelector('#sourceTree') !== null;
                const projectId = document.getElementById(isSource ? 'sourceProject' : 'destProject').textContent;
                const dbId = document.getElementById(isSource ? 'sourceDb' : 'destDbInfo').textContent;
                
                // Get query parameters
                const queryField = panel.querySelector('.query-field').value;
                const queryOperator = panel.querySelector('.query-operator').value;
                let queryValue = panel.querySelector('.query-value').value;

                // Try to parse the value as a number if it looks like one
                if (!isNaN(queryValue)) {
                    queryValue = Number(queryValue);
                }

                // Get the selected collection if any
                const selectedCollection = panel.querySelector('.collection-item.selected');
                const collection = selectedCollection ? selectedCollection.getAttribute('data-path') : null;

                console.log('Selected collection:', collection);
                // Remove the requirement for collection selection
                // if (!collection) {
                //     showStatus('Please select a collection', 'error');
                //     return;
                // }

                await this.executeQuery(projectId, dbId, collection, queryField, queryOperator, queryValue, isSource);
            });
        });
    }

    async executeQuery(projectId, dbId, collection, field, operator, value, isSource) {
        if (!field || !operator || !value) {
            showStatus('Please fill in all query fields', 'error');
            return;
        }

        try {
            const response = await fetch('/query-documents', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    projectId,
                    dbId,
                    collection,
                    field,
                    operator,
                    value
                })
            });

            const data = await response.json();
            
            if (data.error) {
                showStatus(data.error, 'error');
                return;
            }

            const docs = data.documents;
            if (docs.length === 0) {
                showStatus('No documents found matching the query', 'info');
                return;
            }

            // Create or clear search results section
            const tree = isSource ? this.sourceTree : this.destTree;
            let searchResults = tree.querySelector('.search-results');
            if (!searchResults) {
                searchResults = document.createElement('div');
                searchResults.className = 'search-results';
            }
            searchResults.innerHTML = '';

            // Add header
            const header = document.createElement('div');
            header.className = 'search-results-header';
            header.innerHTML = `
                <span>Search Results (${docs.length} documents found)</span>
                <button onclick="this.closest('.search-results').remove()">Clear Results</button>
            `;
            searchResults.appendChild(header);

            // Add each document as a result item
            docs.forEach(doc => {
                const item = document.createElement('div');
                item.className = 'search-result-item';
                item.setAttribute('data-path', doc.path);

                const checkbox = document.createElement('div');
                checkbox.className = 'tree-checkbox';
                item.appendChild(checkbox);

                const label = document.createElement('div');
                label.className = 'path-label';
                // Extract collection and document ID from path
                const pathParts = doc.path.split('/');
                const docId = pathParts.pop();
                const collectionId = pathParts.pop();
                label.textContent = `${collectionId}/${docId}`;
                label.title = doc.path; // Show full path on hover
                item.appendChild(label);

                // Add click handler for selection
                item.addEventListener('click', (e) => {
                    this.toggleSelection(item, isSource, e.shiftKey);
                });

                searchResults.appendChild(item);
                // Auto-select the item
                this.toggleSelection(item, isSource, false);
            });

            // Add search results to tree
            tree.appendChild(searchResults);
            
            // Enable multi-select mode since we have selected documents
            this.toggleMultiSelect(true);
            
            showStatus(`Found and selected ${docs.length} matching documents`, 'success');
        } catch (error) {
            console.error('Error executing query:', error);
            showStatus('Error executing query: ' + error, 'error');
        }
    }

    toggleMultiSelect(enabled) {
        console.log('Toggling multi-select mode:', enabled);
        this.multiSelectMode = enabled;
        
        // Add or remove multi-select-enabled class from all tree items
        const allTreeItems = document.querySelectorAll('.tree-item');
        allTreeItems.forEach(item => {
            if (enabled || item.classList.contains('selected')) {
                item.classList.add('multi-select-enabled');
            } else {
                item.classList.remove('multi-select-enabled');
            }
        });

        // Update button visibility
        this.copyLeftBtn.style.display = enabled ? 'block' : 'none';
        this.copyRightBtn.style.display = enabled ? 'block' : 'none';
    }

    renderCollections(collections, tree) {
        tree.innerHTML = '';
        
        if (!collections || collections.length === 0) {
            const emptyMessage = document.createElement('div');
            emptyMessage.className = 'tree-empty-message';
            emptyMessage.textContent = 'No collections found';
            tree.appendChild(emptyMessage);
            return;
        }
        
        collections.forEach(collection => {
            const collectionNode = this.createCollectionNode(collection, tree === this.sourceTree);
            tree.appendChild(collectionNode);
        });
    }
}

// Load destination database
function loadDestination() {
    const projectId = document.getElementById('destProjectId').value;
    const dbId = document.getElementById('destDb').value || '(default)';
    explorer.loadDatabase(projectId, dbId, false);
}

// Load source database (called after PITR restore)
function loadSource(projectId, dbId) {
    document.getElementById('sourceProject').textContent = projectId;
    document.getElementById('sourceDb').textContent = dbId;
    explorer.loadDatabase(projectId, dbId, true);
} 