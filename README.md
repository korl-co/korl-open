# firestore-tools
Open Source Firestore Tools

Firestore is great and easy! However, it is missing some basic tools that many mature DBs have.

It's pretty easy to write the tools yourself, but it seems like we can all share a few as a community rather than repeatedly rewriting them.
Especially for dev and staging environments (as these tools are built for), a few quick tools can really help speed development.

Here are a couple of firestore tools we use at Korl regularly and thought you all could perhaps use as well. Feedback and PRs welcome!

Note: Naming is not our strong suit, as such we are happy to change names, documentation for clarity and understanding :-)

## **firestore_doc_deleter:**
  This is a Firestore document deletion and restoration tool written in Go. 
  
  Main functions:

1. **Document Deletion**:
   - Delete specific documents from a collection
   - Delete documents from subcollections
   - Supports batch deletions with rate limiting
   - Has a dry-run mode to preview deletions without actually performing them

2. **Backup & Restore**:
   - Automatically backs up documents before deletion to a backup collection (default: "deleted_documents")
   - Can restore previously deleted documents from the backup collection
   - Can restore specific documents or entire collections

3. **Safety Features**:
   - Rate limiting to prevent overwhelming Firestore
   - Dry-run mode for testing
   - Document backups before deletion
   - Validation of inputs and file permissions
   - Progress tracking and detailed operation statistics

Example usage:

```
# Delete specific documents
go run firestore_doc_deleter.go --credentials=creds.json --collection="users" --documents="user123,user456"

# Delete documents in a subcollection
go run firestore_doc_deleter.go --credentials=creds.json --collection="users" --documents="user123" --subcollection="posts"

# Restore deleted documents
go run firestore_doc_deleter.go --credentials=creds.json --restore --backup="deleted_documents" --collection="users"

# Dry run (preview without actual deletion)
go run firestore_doc_deleter.go --credentials=creds.json --collection="users" --documents="user123" --dryrun=true
```

The tool is particularly useful for:
- Safely deleting Firestore documents with automatic backups
- Managing subcollection deletions
- Restoring accidentally deleted documents
- Batch operations with rate limiting to prevent service disruption

**Prerequisites**

1. Install Go (if not already installed)
2. Set up a Firebase project and enable Firestore
3. Generate a service account credentials file:
   - Go to Firebase Console > Project Settings > Service Accounts
   - Click "Generate New Private Key"
   - Save the JSON file
4. Set correct permissions on credentials file:
   ```bash
   chmod 600 path/to/credentials.json
   ```
5. Install required dependencies:
   ```bash
   go get cloud.google.com/go/firestore
   go get firebase.google.com/go
   go get google.golang.org/api
   ```

## **firestore_restore**

This is a Firestore database restoration and transfer tool with a web-based interface. 

Main features:

1. **Backup Restoration**:
   - Can restore data from Google Cloud Storage (GCS) backups (sort of, need some improvements here)
   - Supports Point-in-Time Recovery (PITR) restoration
   - Beginning support for different granularity levels (full database, collection, or document)

2. **Database Transfer**:
   - UI to transfer data between different Firestore databases
   - Supports copying specific collections or documents
   - Includes options for handling subcollections

3. **Database Explorer**:
   - (Very Poort) Interactive UI to browse both source and destination databases
   - Tree view of collections and documents
   - Document preview functionality
   - Search/query capabilities across collections

4. **Key Features**:
   - Progress monitoring and real-time status updates
   - Batch processing for efficient data transfer
   - Error handling and retry mechanisms
   - Support for service account authentication
   - Granular control over what data gets restored/transferred

The tool is particularly useful for:
- Disaster recovery
- Database migrations
- Creating test environments with production data
- Selective data restoration
- Database exploration and management

It provides a terrible web interface to perform these operations without needing to use command-line tools or write code.

Let me break down the prerequisites and usage instructions:

### Prerequisites:

1. **Google Cloud Project Setup**:
   - One or more Google Cloud projects with Firestore databases
   - Appropriate IAM permissions:
     - `roles/datastore.importExportAdmin` for import/export operations
     - `roles/datastore.owner` or `roles/datastore.user` for database access
     - `roles/storage.objectViewer` for accessing GCS backups

2. **Service Account Credentials**:
   - Option 1: Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable with path to service account key
   - Option 2: Provide service account key paths directly in the UI for source/destination

3. **For GCS Backups**:
   - Existing Firestore backups in a GCS bucket
   - Access permissions to the GCS bucket

### Ways to Use:

1. **Restore from GCS Backup**:
   - Enter source project details and GCS backup path
   - Choose destination project and database
   - Select restore granularity:
     - Full database restore
   - Configure options like including subcollections

2. **Point-in-Time Recovery (PITR)**:
   - Enter source project details
   - Specify the timestamp for recovery
   - Choose destination project and database
   - Select collections to restore

3. **Database Explorer**:
   - Browse and search through source/destination databases
   - Preview document contents
   - Execute queries across collections
   - Transfer specific collections/documents between databases:
     - Select items in source/destination
     - Use transfer buttons (→/←) to copy data
     - Choose whether to include subcollections
     - Choose whether to overwrite existing documents

4. **Monitoring and Control**:
   - View real-time progress in the status panel
   - Cancel ongoing operations if needed
   - See detailed logs of the operation

To start using:
1. Run the tool (it's a web server) ```go run .```
2. Access the web interface (default port 8080)
3. Configure source/destination credentials
4. Choose the operation type and follow the UI prompts

The tool provides a flexible interface to handle various database restoration and transfer scenarios while maintaining control over the process.

