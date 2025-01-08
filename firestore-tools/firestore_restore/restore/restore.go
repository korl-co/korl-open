package restore

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/firestore"
	admin "cloud.google.com/go/firestore/apiv1/admin"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/firestore/admin/v1"
)

// Add a new type to represent progress metrics
type ProgressMetrics struct {
	State            string `json:"state"`
	DocsCompleted    int64  `json:"docsCompleted"`
	DocsEstimated    int64  `json:"docsEstimated"`
	BytesCompleted   int64  `json:"bytesCompleted"`
	BytesEstimated   int64  `json:"bytesEstimated"`
	EstimatedTimeStr string `json:"estimatedTimeStr,omitempty"`
}

func (p ProgressMetrics) String() string {
	var parts []string
	parts = append(parts, fmt.Sprintf("State: %s", p.State))

	if p.DocsCompleted > 0 || p.DocsEstimated > 0 {
		percent := 0.0
		if p.DocsEstimated > 0 {
			percent = float64(p.DocsCompleted) / float64(p.DocsEstimated) * 100
		}
		parts = append(parts, fmt.Sprintf("Documents: %.1f%% (%d/%d)", percent, p.DocsCompleted, p.DocsEstimated))
	}

	if p.BytesCompleted > 0 || p.BytesEstimated > 0 {
		percent := 0.0
		if p.BytesEstimated > 0 {
			percent = float64(p.BytesCompleted) / float64(p.BytesEstimated) * 100
		}
		completedMB := float64(p.BytesCompleted) / 1024 / 1024
		estimatedMB := float64(p.BytesEstimated) / 1024 / 1024
		parts = append(parts, fmt.Sprintf("Data: %.1f%% (%.1f/%.1f MB)", percent, completedMB, estimatedMB))
	}

	if p.EstimatedTimeStr != "" {
		parts = append(parts, fmt.Sprintf("Estimated completion: %s", p.EstimatedTimeStr))
	}

	return strings.Join(parts, ", ")
}

func extractProgressFromMetadata(metadata *adminpb.ImportDocumentsMetadata) ProgressMetrics {
	state := "UNKNOWN"
	if metadata != nil {
		state = metadata.GetOperationState().String()
	}

	progress := ProgressMetrics{
		State: state,
	}

	log.Printf("Starting progress extraction from metadata for operation state: %s", progress.State)
	log.Printf("Full metadata dump: %#v", metadata)

	// Extract document progress
	if docs := metadata.GetProgressDocuments(); docs != nil {
		// Handle both completed and estimated work
		completed := docs.GetCompletedWork()
		estimated := docs.GetEstimatedWork()

		log.Printf("Raw document metrics - Completed: %d, Estimated: %d", completed, estimated)

		// Sometimes the API returns 0 for estimated work initially
		if estimated == 0 && completed > 0 {
			log.Printf("API returned 0 for estimated docs but has completed work, using completed as estimate")
			estimated = completed
		}

		progress.DocsCompleted = completed
		progress.DocsEstimated = estimated
	} else {
		log.Printf("No document progress information available in metadata: %+v", metadata)
		// Try to get progress from raw metadata fields
		log.Printf("Metadata fields available:")
		log.Printf("  StartTime: %v", metadata.GetStartTime())
		log.Printf("  EndTime: %v", metadata.GetEndTime())
		log.Printf("  OperationState: %v", metadata.GetOperationState())
		log.Printf("  InputUriPrefix: %v", metadata.GetInputUriPrefix())
		log.Printf("  CollectionIds: %v", metadata.GetCollectionIds())
		log.Printf("  NamespaceIds: %v", metadata.GetNamespaceIds())

		// Log all available fields for debugging
		log.Printf("Raw metadata fields:")
		if docs := metadata.GetProgressDocuments(); docs != nil {
			log.Printf("  ProgressDocuments: %#v", docs)
		}
		if bytes := metadata.GetProgressBytes(); bytes != nil {
			log.Printf("  ProgressBytes: %#v", bytes)
		}
	}

	// Extract bytes progress
	if bytes := metadata.GetProgressBytes(); bytes != nil {
		// Handle both completed and estimated bytes
		completed := bytes.GetCompletedWork()
		estimated := bytes.GetEstimatedWork()

		log.Printf("Raw bytes metrics - Completed: %d, Estimated: %d", completed, estimated)

		// Sometimes the API returns 0 for estimated bytes initially
		if estimated == 0 && completed > 0 {
			log.Printf("API returned 0 for estimated bytes but has completed work, using completed as estimate")
			estimated = completed
		}

		progress.BytesCompleted = completed
		progress.BytesEstimated = estimated
	} else {
		log.Printf("No bytes progress information available in metadata: %+v", metadata)
	}

	// If we have completed work but no estimates, use completed as estimate
	if progress.BytesEstimated == 0 && progress.BytesCompleted > 0 {
		log.Printf("Using completed bytes as estimate: %d", progress.BytesCompleted)
		progress.BytesEstimated = progress.BytesCompleted
	}
	if progress.DocsEstimated == 0 && progress.DocsCompleted > 0 {
		log.Printf("Using completed docs as estimate: %d", progress.DocsCompleted)
		progress.DocsEstimated = progress.DocsCompleted
	}

	// Log final computed progress
	log.Printf("Final computed progress - State: %s, Docs: %d/%d, Bytes: %d/%d",
		progress.State,
		progress.DocsCompleted, progress.DocsEstimated,
		progress.BytesCompleted, progress.BytesEstimated)

	return progress
}

// Add new type for backup file info
type BackupFileInfo struct {
	Path         string
	Size         int64
	Collection   string
	ChunkNumber  int
	TotalChunks  int
	LastModified time.Time
}

func getBackupFiles(ctx context.Context, gcsPath string) ([]BackupFileInfo, error) {
	// Extract bucket and prefix from the GCS path
	parts := strings.SplitN(gcsPath, "/", 4)
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid GCS path: %s", gcsPath)
	}
	bucketName := parts[2]
	prefix := parts[3]

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %v", err)
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)
	var files []BackupFileInfo
	var maxChunk int

	// List all files in the backup folder
	it := bucket.Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error listing objects: %v", err)
		}

		// Skip the metadata file and empty "directories"
		if strings.HasSuffix(attrs.Name, ".overall_export_metadata") || attrs.Size == 0 {
			continue
		}

		// Try to extract collection and chunk info from the path
		pathParts := strings.Split(attrs.Name, "/")
		var collection string
		var chunkNum int
		if len(pathParts) > 2 {
			// The collection name is typically in the format: all_namespaces/collection_id/...
			collection = pathParts[len(pathParts)-2]

			// Try to extract chunk number from filename
			fileName := pathParts[len(pathParts)-1]
			if strings.Contains(fileName, "output-") {
				chunkStr := strings.TrimPrefix(fileName, "output-")
				chunkStr = strings.TrimSuffix(chunkStr, ".json")
				if chunk, err := strconv.Atoi(chunkStr); err == nil {
					chunkNum = chunk
					if chunk > maxChunk {
						maxChunk = chunk
					}
				}
			}
		}

		files = append(files, BackupFileInfo{
			Path:         attrs.Name,
			Size:         attrs.Size,
			Collection:   collection,
			ChunkNumber:  chunkNum,
			LastModified: attrs.Updated,
		})
	}

	// Update total chunks for all files
	for i := range files {
		files[i].TotalChunks = maxChunk + 1
	}

	// Sort files by collection and chunk number
	sort.Slice(files, func(i, j int) bool {
		if files[i].Collection != files[j].Collection {
			return files[i].Collection < files[j].Collection
		}
		return files[i].ChunkNumber < files[j].ChunkNumber
	})

	// Group files by collection and count chunks
	collections := make(map[string]struct {
		chunks int
		size   int64
	})
	for _, file := range files {
		if file.Collection != "" {
			info := collections[file.Collection]
			info.chunks++
			info.size += file.Size
			collections[file.Collection] = info
		}
	}

	log.Printf("Found %d backup files in %s", len(files), gcsPath)
	log.Printf("Collections in backup:")
	for col, info := range collections {
		log.Printf("  %s: %d chunks (%.2f MB)", col, info.chunks, float64(info.size)/1024/1024)
		// Log first few chunks for each collection
		chunkCount := 0
		for _, file := range files {
			if file.Collection == col {
				log.Printf("    Chunk %d/%d: %.2f MB",
					file.ChunkNumber+1, file.TotalChunks,
					float64(file.Size)/1024/1024)
				chunkCount++
				if chunkCount >= 5 {
					remaining := info.chunks - chunkCount
					if remaining > 0 {
						log.Printf("    ... and %d more chunks", remaining)
					}
					break
				}
			}
		}
	}

	return files, nil
}

func monitorImportProgress(ctx context.Context, op *admin.ImportDocumentsOperation, gcsPath, destDB string) error {
	// Log initial details to server console
	log.Printf("Starting progress monitoring for operation %s - Source: %s, Destination DB: %s", op.Name(), gcsPath, destDB)

	// Get list of backup files first
	backupFiles, err := getBackupFiles(ctx, gcsPath)
	if err != nil {
		log.Printf("Warning: Could not get backup files: %v", err)
	} else {
		var totalSize int64
		collectionSizes := make(map[string]int64)
		for _, file := range backupFiles {
			totalSize += file.Size
			if file.Collection != "" {
				collectionSizes[file.Collection] += file.Size
			}
		}
		log.Printf("Total backup size: %.2f MB across %d files", float64(totalSize)/1024/1024, len(backupFiles))
		log.Printf("Collection sizes:")
		for col, size := range collectionSizes {
			log.Printf("  %s: %.2f MB", col, float64(size)/1024/1024)
		}
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	var lastState string
	var lastLogTime time.Time

	// Log initial state
	if metadata, err := op.Metadata(); err == nil {
		currentState := metadata.GetOperationState().String()
		stateMsg := fmt.Sprintf("%s   Operation State: %s", time.Now().Format("15:04:05"), currentState)
		log.Printf(stateMsg)
		lastState = currentState
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("Operation cancelled by user")
			// We can't directly cancel the operation, but we can stop monitoring it
			// The operation will continue in Firestore but we won't wait for it
			return fmt.Errorf("operation monitoring cancelled by user")

		case <-ticker.C:
			// Get the latest metadata
			metadata, err := op.Metadata()
			if err != nil {
				errMsg := fmt.Sprintf("Warning: Could not get metadata for operation: %v", err)
				log.Printf(errMsg)
				continue
			}

			currentState := metadata.GetOperationState().String()
			currentTime := time.Now()

			// Log state changes or periodic updates
			if currentState != lastState || time.Since(lastLogTime) >= 30*time.Second {
				stateMsg := fmt.Sprintf("%s   Operation State: %s", currentTime.Format("15:04:05"), currentState)
				log.Printf(stateMsg)
				lastState = currentState
				lastLogTime = currentTime
			}

			// Check if operation is done
			if op.Done() {
				if err := op.Wait(ctx); err != nil {
					errMsg := fmt.Sprintf("Import operation failed: %v", err)
					log.Printf(errMsg)
					return fmt.Errorf(errMsg)
				}
				completionMsg := fmt.Sprintf("%s   Operation State: COMPLETED", time.Now().Format("15:04:05"))
				log.Printf(completionMsg)
				successMsg := fmt.Sprintf("Successfully imported documents from %s to database %s", gcsPath, destDB)
				log.Printf(successMsg)
				return nil
			}
		}
	}
}

// RestoreFromGCS restores data from a Firestore export in GCS
func RestoreFromGCS(ctx context.Context, sourceProjectID, destProjectID, destDB, gcsPath string, granularity int, collectionID, documentID, userEmail string) error {
	log.Printf("Starting GCS restore with parameters:")
	log.Printf("  Source Project: %s", sourceProjectID)
	log.Printf("  Destination Project: %s", destProjectID)
	log.Printf("  Destination DB: %s", destDB)
	log.Printf("  GCS Path: %s", gcsPath)
	log.Printf("  Granularity: %d", granularity)
	log.Printf("  Collection ID: %s", collectionID)
	log.Printf("  Document ID: %s", documentID)
	log.Printf("  User Email: %s", userEmail)

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return fmt.Errorf("operation cancelled before starting")
	default:
	}

	// Add credential check
	credPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if credPath == "" {
		return &RestoreError{
			Message: "No service account credentials found",
			Commands: `Please set up service account credentials:

1. Create a service account key:
gcloud iam service-accounts keys create key.json --iam-account=YOUR_SERVICE_ACCOUNT@YOUR_PROJECT.iam.gserviceaccount.com

2. Set the environment variable:
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"

Or use application default credentials:
gcloud auth application-default login`,
		}
	}
	log.Printf("Using credentials from: %s", credPath)

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return fmt.Errorf("operation cancelled during credential check")
	default:
	}

	// First verify GCS access and check if metadata file exists
	if err := verifyGCSAccess(ctx, gcsPath); err != nil {
		return &RestoreError{
			Message: fmt.Sprintf("GCS access check failed: %v", err),
			Commands: fmt.Sprintf(`This could mean:
1. Missing GCS permissions - run:
gcloud projects add-iam-policy-binding %s --member="user:%s" --role="roles/storage.objectViewer"

2. The backup folder doesn't contain a valid Firestore export
3. The backup path is incorrect - current path: %s`,
				sourceProjectID, userEmail, gcsPath),
		}
	}

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return fmt.Errorf("operation cancelled after GCS access check")
	default:
	}

	adminClient, err := admin.NewFirestoreAdminClient(ctx)
	if err != nil {
		return &RestoreError{
			Message: fmt.Sprintf("Failed to create Firestore Admin client: %v", err),
			Commands: fmt.Sprintf(`Run these commands to grant necessary permissions:

1. Grant Firestore Admin access:
gcloud projects add-iam-policy-binding %s --member="user:%s" --role="roles/datastore.importExportAdmin"

2. Grant Firestore Owner access (if needed):
gcloud projects add-iam-policy-binding %s --member="user:%s" --role="roles/datastore.owner"

3. Check your current credentials:
gcloud auth list

4. If using a service account, grant it the same permissions:
gcloud projects add-iam-policy-binding %s --member="serviceAccount:YOUR_SERVICE_ACCOUNT_EMAIL" --role="roles/datastore.importExportAdmin"
gcloud projects add-iam-policy-binding %s --member="serviceAccount:YOUR_SERVICE_ACCOUNT_EMAIL" --role="roles/datastore.owner"`,
				destProjectID, userEmail, destProjectID, userEmail, destProjectID, destProjectID),
		}
	}
	defer adminClient.Close()

	log.Printf("Successfully created Firestore Admin client")

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return fmt.Errorf("operation cancelled after creating admin client")
	default:
	}

	// Verify destination database access
	if err := verifyFirestoreAccess(ctx, destProjectID, destDB); err != nil {
		return &RestoreError{
			Message: fmt.Sprintf("Failed to access destination Firestore: %v", err),
			Commands: fmt.Sprintf(`Ensure you have access to the destination database:

1. Check if you can access the database directly:
gcloud firestore databases list --project=%s

2. Grant necessary database access:
gcloud projects add-iam-policy-binding %s --member="user:%s" --role="roles/datastore.user"`,
				destProjectID, destProjectID, userEmail),
		}
	}

	log.Printf("Successfully verified destination database access")

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return fmt.Errorf("operation cancelled after verifying database access")
	default:
	}

	req := &adminpb.ImportDocumentsRequest{
		Name:           fmt.Sprintf("projects/%s/databases/%s", destProjectID, destDB),
		InputUriPrefix: gcsPath,
	}

	log.Printf("Starting import operation with request: %+v", req)
	op, err := adminClient.ImportDocuments(ctx, req)
	if err != nil {
		return &RestoreError{
			Message: fmt.Sprintf("Import operation failed to start: %v", err),
			Commands: fmt.Sprintf(`This error typically means missing Import/Export permissions. Run:

1. Grant Import/Export Admin role:
gcloud projects add-iam-policy-binding %s --member="user:%s" --role="roles/datastore.importExportAdmin"

2. Grant Owner role (if needed):
gcloud projects add-iam-policy-binding %s --member="user:%s" --role="roles/datastore.owner"

3. If using a service account, check its email:
gcloud iam service-accounts list --project=%s

4. Grant the service account permissions:
gcloud projects add-iam-policy-binding %s --member="serviceAccount:YOUR_SERVICE_ACCOUNT_EMAIL" --role="roles/datastore.importExportAdmin"`,
				destProjectID, userEmail, destProjectID, userEmail, destProjectID, destProjectID),
		}
	}

	log.Printf("Import operation started successfully, operation name: %s", op.Name())

	// Monitor the import progress
	return monitorImportProgress(ctx, op, gcsPath, destDB)
}

func verifyGCSAccess(ctx context.Context, gcsPath string) error {
	// Extract bucket and prefix from the GCS path
	parts := strings.SplitN(gcsPath, "/", 4)
	if len(parts) < 3 {
		log.Printf("Invalid GCS path format: %s (parts: %v)", gcsPath, parts)
		return fmt.Errorf("invalid GCS path: %s - path should be in format gs://bucket-name/backup-folder", gcsPath)
	}
	bucketName := parts[2]
	prefix := ""
	if len(parts) >= 4 {
		prefix = parts[3]
	}

	log.Printf("Verifying GCS access - Bucket: %s, Prefix: %s", bucketName, prefix)

	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %v", err)
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)

	// List objects with the given prefix to verify access and existence
	it := bucket.Objects(ctx, &storage.Query{
		Prefix:    prefix,
		Delimiter: "/", // Use delimiter to get folders
	})

	// Try to get at least one object to verify access
	obj, err := it.Next()
	if err == iterator.Done {
		log.Printf("No objects found in bucket %s with prefix %s", bucketName, prefix)
		return fmt.Errorf("no backup files or folders found at path: %s - please select a specific backup folder", gcsPath)
	}
	if err != nil {
		log.Printf("Error listing objects in bucket %s: %v", bucketName, err)
		return fmt.Errorf("failed to list objects in bucket: %v", err)
	}

	// If we only have a bucket, list available backup folders
	if prefix == "" {
		log.Printf("Available backup folders in %s:", gcsPath)
		for {
			if obj.Prefix != "" {
				// Fix the path formatting by ensuring there's a / between bucket and prefix
				folderPath := fmt.Sprintf("gs://%s/%s", bucketName, obj.Prefix)
				log.Printf("  %s", folderPath)
			}
			obj, err = it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return fmt.Errorf("failed to list objects in bucket: %v", err)
			}
		}
		return fmt.Errorf("please select a specific backup folder from the list above")
	}

	// Look for the metadata file
	metadataPath := prefix + "/" + path.Base(prefix) + ".overall_export_metadata"
	log.Printf("Looking for metadata file at: %s", metadataPath)

	// Check if metadata file exists
	_, err = bucket.Object(metadataPath).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			log.Printf("Warning: Could not find metadata file at %s, but proceeding with restore", metadataPath)
		} else {
			log.Printf("Error checking metadata file: %v", err)
			return fmt.Errorf("failed to check metadata file: %v", err)
		}
	} else {
		log.Printf("Found metadata file at %s", metadataPath)
	}

	return nil
}

func verifyFirestoreAccess(ctx context.Context, projectID, dbID string) error {
	client, err := firestore.NewClientWithDatabase(ctx, projectID, dbID)
	if err != nil {
		return fmt.Errorf("failed to create Firestore client: %v", err)
	}
	defer client.Close()

	// Try to list collections to verify access
	iter := client.Collections(ctx)
	_, err = iter.Next()
	if err != nil && err != iterator.Done {
		return fmt.Errorf("failed to list collections: %v", err)
	}

	return nil
}

// RestoreFromPITR restores data from a point in time
func RestoreFromPITR(ctx context.Context, sourceProjectID, destProjectID, destDB string, pitrTime time.Time, granularity int, collectionID string) error {
	log.Printf("Starting PITR restore process...")
	log.Printf("Source Project: %s", sourceProjectID)
	log.Printf("Destination Project: %s", destProjectID)
	log.Printf("Destination DB: %s", destDB)
	log.Printf("PITR Time: %v", pitrTime)
	log.Printf("Granularity: %d", granularity)
	log.Printf("Collection ID: %s", collectionID)

	// Create source client
	log.Printf("Creating source Firestore client...")
	sourceClient, err := firestore.NewClientWithDatabase(ctx, sourceProjectID, "(default)")
	if err != nil {
		log.Printf("Failed to create source client: %v", err)
		return fmt.Errorf("failed to create source client: %v", err)
	}
	defer sourceClient.Close()

	// Create destination client
	log.Printf("Creating destination Firestore client...")
	destClient, err := firestore.NewClientWithDatabase(ctx, destProjectID, destDB)
	if err != nil {
		log.Printf("Failed to create destination client: %v", err)
		return fmt.Errorf("failed to create destination client: %v", err)
	}
	defer destClient.Close()

	// Create stats for the restore operation
	stats := &RestoreStats{
		StartTime:       time.Now(),
		CollectionStats: make(map[string]*CollectionStats),
	}
	defer func() {
		stats.EndTime = time.Now()
		log.Printf(stats.String())
	}()

	// Handle different granularity levels
	switch granularity {
	case 0: // Full database
		log.Printf("Performing full database restore")
		return restoreFullDatabase(ctx, sourceClient, destClient, pitrTime)
	case 1: // Specific collection
		if collectionID == "" {
			return fmt.Errorf("collection ID required for collection-level restore")
		}
		log.Printf("Performing collection restore: %s", collectionID)
		return restoreCollection(ctx, sourceClient, destClient, collectionID, pitrTime, stats)
	default:
		return fmt.Errorf("unsupported granularity level: %d", granularity)
	}
}

// Add RestoreStats struct near the top with other types
type RestoreStats struct {
	StartTime       time.Time
	EndTime         time.Time
	CollectionStats map[string]*CollectionStats
	TotalDocuments  int64
	TotalBytes      int64
	ErrorCount      int32
	RetryCount      int32
}

type CollectionStats struct {
	DocumentCount  int64
	ByteSize       int64
	SubCollections []string
	ErrorCount     int32
	RetryCount     int32
	Duration       time.Duration
}

func (rs *RestoreStats) String() string {
	var b strings.Builder
	duration := rs.EndTime.Sub(rs.StartTime)

	b.WriteString("\n=== Restore Operation Summary ===\n")
	b.WriteString(fmt.Sprintf("Total Duration: %v\n", duration.Round(time.Second)))
	b.WriteString(fmt.Sprintf("Total Documents: %d\n", rs.TotalDocuments))
	b.WriteString(fmt.Sprintf("Total Data Size: %.2f MB\n", float64(rs.TotalBytes)/1024/1024))
	b.WriteString(fmt.Sprintf("Total Errors: %d\n", rs.ErrorCount))
	b.WriteString(fmt.Sprintf("Total Retries: %d\n", rs.RetryCount))
	b.WriteString("\nCollection Statistics:\n")

	// Sort collections by document count
	type colStat struct {
		name  string
		stats *CollectionStats
	}
	var collections []colStat
	for name, stats := range rs.CollectionStats {
		collections = append(collections, colStat{name, stats})
	}
	sort.Slice(collections, func(i, j int) bool {
		return collections[i].stats.DocumentCount > collections[j].stats.DocumentCount
	})

	for _, col := range collections {
		stats := col.stats
		b.WriteString(fmt.Sprintf("\n  %s:\n", col.name))
		b.WriteString(fmt.Sprintf("    Documents: %d\n", stats.DocumentCount))
		b.WriteString(fmt.Sprintf("    Data Size: %.2f MB\n", float64(stats.ByteSize)/1024/1024))
		b.WriteString(fmt.Sprintf("    Duration: %v\n", stats.Duration.Round(time.Second)))
		if len(stats.SubCollections) > 0 {
			b.WriteString(fmt.Sprintf("    SubCollections: %s\n", strings.Join(stats.SubCollections, ", ")))
		}
		if stats.ErrorCount > 0 {
			b.WriteString(fmt.Sprintf("    Errors: %d\n", stats.ErrorCount))
		}
		if stats.RetryCount > 0 {
			b.WriteString(fmt.Sprintf("    Retries: %d\n", stats.RetryCount))
		}
	}

	return b.String()
}

// Modify restoreCollection to track stats
func restoreCollection(ctx context.Context, sourceClient, destClient *firestore.Client, collectionID string, pitrTime time.Time, stats *RestoreStats) error {
	collStart := time.Now()
	if stats.CollectionStats == nil {
		stats.CollectionStats = make(map[string]*CollectionStats)
	}
	collStats := &CollectionStats{
		DocumentCount: 0,
		ByteSize:      0,
	}
	stats.CollectionStats[collectionID] = collStats

	log.Printf("Starting restore of collection %s at time %v", collectionID, pitrTime)

	// Use a smaller batch size to avoid timeouts
	const batchSize = 20           // Reduced from 100 to avoid timeouts
	const maxConcurrentBatches = 5 // Adjust based on system capabilities
	var processedCount int64
	var totalDocs int64
	var errorCount int32
	const maxErrors = 100

	// Create a channel for batches of document references
	batchChan := make(chan []*firestore.DocumentSnapshot, maxConcurrentBatches)
	errChan := make(chan error, maxConcurrentBatches)
	var wg sync.WaitGroup

	// Start worker goroutines to process batches
	for i := 0; i < maxConcurrentBatches; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for docs := range batchChan {
				batch := destClient.Batch()
				batchStart := time.Now()

				for _, doc := range docs {
					atomic.AddInt64(&processedCount, 1)
					atomic.AddInt64(&totalDocs, 1)
					atomic.AddInt64(&stats.TotalDocuments, 1)
					atomic.AddInt64(&collStats.DocumentCount, 1)

					// Log every document for progress tracking
					log.Printf("Restoring document: Collection=%s, Document=%s (%d processed)",
						collectionID, doc.Ref.ID, atomic.LoadInt64(&processedCount))

					docRef := doc.Ref
					docData := doc.Data()
					if docData == nil {
						log.Printf("Skipping nil document in collection %s", collectionID)
						continue
					}

					// Estimate document size (rough approximation)
					docSize := int64(len(fmt.Sprintf("%v", docData)))
					atomic.AddInt64(&collStats.ByteSize, docSize)
					atomic.AddInt64(&stats.TotalBytes, docSize)

					destRef := destClient.Collection(collectionID).Doc(docRef.ID)
					batch.Set(destRef, docData)
				}

				// Commit the batch
				log.Printf("Committing batch of %d docs for collection %s...", len(docs), collectionID)
				if err := commitBatchAndRestoreSubcollections(ctx, batch, sourceClient, destClient, docs[len(docs)-1], collectionID); err != nil {
					errMsg := fmt.Errorf("failed to commit batch and restore subcollections: %v", err)
					log.Printf("Error: %v", errMsg)
					if atomic.AddInt32(&errorCount, 1) > maxErrors {
						errChan <- fmt.Errorf("too many errors (%d), aborting", errorCount)
						return
					}
					atomic.AddInt32(&collStats.ErrorCount, 1)
					atomic.AddInt32(&stats.ErrorCount, 1)
					continue
				}
				log.Printf("Successfully committed batch for collection %s (took %v)", collectionID, time.Since(batchStart))
			}
		}()
	}

	// Query and send batches to workers
	var lastDoc *firestore.DocumentSnapshot
	for {
		// Query with pagination
		query := sourceClient.Collection(collectionID).OrderBy(firestore.DocumentID, firestore.Asc).Limit(batchSize)
		if lastDoc != nil {
			query = query.StartAfter(lastDoc)
		}

		docs := query.Documents(ctx)
		batch := make([]*firestore.DocumentSnapshot, 0, batchSize)
		documentsInBatch := 0

		for {
			doc, err := docs.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				if atomic.AddInt32(&errorCount, 1) > maxErrors {
					errChan <- fmt.Errorf("too many errors (%d), aborting", errorCount)
					break
				}
				log.Printf("Error reading document in collection %s: %v", collectionID, err)
				continue
			}

			lastDoc = doc
			batch = append(batch, doc)
			documentsInBatch++

			if len(batch) == batchSize {
				batchChan <- batch
				batch = make([]*firestore.DocumentSnapshot, 0, batchSize)
			}
		}

		// Send any remaining documents
		if len(batch) > 0 {
			batchChan <- batch
		}

		// If we didn't process any new documents in this query, we're done
		if documentsInBatch == 0 {
			break
		}
	}

	// Close the batch channel and wait for workers to finish
	close(batchChan)
	wg.Wait()
	close(errChan)

	// Check for any errors
	var collectedErrors []error
	for err := range errChan {
		collectedErrors = append(collectedErrors, err)
	}

	if len(collectedErrors) > 0 {
		// Combine all errors into a single error message
		var errMsgs []string
		for _, err := range collectedErrors {
			errMsgs = append(errMsgs, err.Error())
		}
		return fmt.Errorf("multiple restore errors occurred:\n%s", strings.Join(errMsgs, "\n"))
	}

	// Update final stats
	collStats.Duration = time.Since(collStart)
	collStats.ErrorCount = errorCount

	log.Printf("Collection %s: completed processing %d documents, restored %d, errors %d",
		collectionID, atomic.LoadInt64(&processedCount), atomic.LoadInt64(&totalDocs), atomic.LoadInt32(&errorCount))
	return nil
}

// Modify restoreFullDatabase to use stats
func restoreFullDatabase(ctx context.Context, sourceClient, destClient *firestore.Client, pitrTime time.Time) error {
	stats := &RestoreStats{
		StartTime:       time.Now(),
		CollectionStats: make(map[string]*CollectionStats),
	}
	defer func() {
		stats.EndTime = time.Now()
		log.Printf(stats.String())
	}()

	log.Printf("Listing collections from source database")
	collections := sourceClient.Collections(ctx)
	var colList []string

	// First, get all collections
	for {
		col, err := collections.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to iterate collections: %v", err)
		}
		colList = append(colList, col.ID)
	}

	// Process collections in parallel with a worker pool
	maxWorkers := 5 // Adjust this based on system capabilities
	log.Printf("Found %d collections to restore, will process up to %d in parallel", len(colList), maxWorkers)

	// Create error channel and wait group for parallel processing
	errChan := make(chan error, len(colList))
	var wg sync.WaitGroup

	// Track active collections
	activeCollections := make(map[string]bool)
	var activeColMutex sync.Mutex

	// Create semaphore for worker pool
	semaphore := make(chan struct{}, maxWorkers)

	for i, colID := range colList {
		wg.Add(1)
		go func(index int, collectionID string) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() {
				activeColMutex.Lock()
				delete(activeCollections, collectionID)
				activeColMutex.Unlock()
				<-semaphore
			}()

			// Track active collections
			activeColMutex.Lock()
			activeCollections[collectionID] = true
			active := make([]string, 0, len(activeCollections))
			for col := range activeCollections {
				active = append(active, col)
			}
			log.Printf("Starting collection %d/%d: %s (currently processing: %v)",
				index+1, len(colList), collectionID, active)
			activeColMutex.Unlock()

			err := restoreCollection(ctx, sourceClient, destClient, collectionID, pitrTime, stats)
			if err != nil {
				log.Printf("Error restoring collection %s: %v", collectionID, err)
				errChan <- fmt.Errorf("error restoring collection %s: %v", collectionID, err)
				return
			}

			log.Printf("Completed collection %d/%d: %s", index+1, len(colList), collectionID)
		}(i, colID)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errChan)

	// Check for any errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		// Combine all errors into a single error message
		var errMsgs []string
		for _, err := range errors {
			errMsgs = append(errMsgs, err.Error())
		}
		return fmt.Errorf("multiple restore errors occurred:\n%s", strings.Join(errMsgs, "\n"))
	}

	return nil
}

func commitBatchAndRestoreSubcollections(ctx context.Context, batch *firestore.WriteBatch, sourceClient, destClient *firestore.Client, doc *firestore.DocumentSnapshot, collectionID string) error {
	maxRetries := 3
	initialBackoff := 1 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			backoff := initialBackoff * time.Duration(1<<uint(attempt-1))
			log.Printf("Retrying batch commit for collection %s after %v (attempt %d/%d)",
				collectionID, backoff, attempt+1, maxRetries)
			time.Sleep(backoff)
		}

		// Check for context cancellation before commit
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation cancelled during batch commit")
		default:
		}

		commitStart := time.Now()
		log.Printf("Starting batch commit for collection %s (attempt %d/%d)",
			collectionID, attempt+1, maxRetries)

		// Commit the current batch
		_, err := batch.Commit(ctx)
		if err == nil {
			log.Printf("Batch commit completed in %v", time.Since(commitStart))
			break
		}

		lastErr = err
		if strings.Contains(err.Error(), "DeadlineExceeded") ||
			strings.Contains(err.Error(), "unavailable") ||
			strings.Contains(err.Error(), "canceled") {
			log.Printf("Transient error during batch commit (attempt %d/%d): %v",
				attempt+1, maxRetries, err)
			continue
		}

		// For non-transient errors, fail immediately
		return fmt.Errorf("failed to commit batch: %v", err)
	}

	if lastErr != nil {
		return fmt.Errorf("failed to commit batch after %d retries: %v", maxRetries, lastErr)
	}

	// Check for context cancellation before processing subcollections
	select {
	case <-ctx.Done():
		return fmt.Errorf("operation cancelled before processing subcollections")
	default:
	}

	// Create stats for subcollections
	subStats := &RestoreStats{
		StartTime:       time.Now(),
		CollectionStats: make(map[string]*CollectionStats),
	}

	// Get subcollections for the document
	log.Printf("Checking for subcollections in document %s/%s", collectionID, doc.Ref.ID)
	subCollections := doc.Ref.Collections(ctx)

	// Process subcollections
	for {
		// Check for context cancellation before each iteration
		select {
		case <-ctx.Done():
			// Return silently when cancelled to avoid error spam
			return nil
		default:
		}

		subCol, err := subCollections.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// If the error is due to cancellation, return silently
			if strings.Contains(err.Error(), "context canceled") {
				return nil
			}
			log.Printf("Error getting subcollections for doc %s/%s: %v", collectionID, doc.Ref.ID, err)
			continue
		}

		// Recursively restore the subcollection
		subCollectionID := fmt.Sprintf("%s/%s/%s", collectionID, doc.Ref.ID, subCol.ID)
		log.Printf("Found subcollection %s, starting restore", subCollectionID)
		if err := restoreCollection(ctx, sourceClient, destClient, subCollectionID, time.Time{}, subStats); err != nil {
			// If the error is due to cancellation, return silently
			if strings.Contains(err.Error(), "context canceled") {
				return nil
			}
			log.Printf("Error restoring subcollection %s: %v", subCollectionID, err)
			continue
		}
		log.Printf("Completed restore of subcollection %s", subCollectionID)
	}

	return nil
}

func listPITRBackups(ctx context.Context, projectID string, pitrTime time.Time) ([]string, bool, error) {
	// Use the earliest possible time for the query
	endTime := pitrTime
	startTime := endTime.Add(-7 * 24 * time.Hour) // Go back 7 days, the max allowed for PITR

	backups := []string{}

	for t := startTime; t.Before(endTime) || t.Equal(endTime); t = t.Add(time.Minute) {
		formattedTime := t.Format(time.RFC3339)
		backupItem := fmt.Sprintf("PITR - %s", formattedTime)
		backups = append(backups, backupItem)
	}

	// PITR does not have a concept of per-collection backups, so granular restore is not possible
	return backups, false, nil
}

// Add a custom error type to include commands
type RestoreError struct {
	Message  string
	Commands string
}

func (e *RestoreError) Error() string {
	return fmt.Sprintf("%s\n\n%s", e.Message, e.Commands)
}

// Helper function to extract numbers from metadata string
func extractNumber(s, prefix string) int64 {
	start := strings.Index(s, prefix)
	if start == -1 {
		return 0
	}
	start += len(prefix)
	end := strings.Index(s[start:], " ")
	if end == -1 {
		end = len(s[start:])
	}
	numStr := strings.TrimSpace(s[start : start+end])
	num, _ := strconv.ParseInt(numStr, 10, 64)
	return num
}

// Helper function to extract strings from metadata string
func extractString(s, prefix string) string {
	start := strings.Index(s, prefix)
	if start == -1 {
		return ""
	}
	start += len(prefix)
	end := strings.Index(s[start:], " ")
	if end == -1 {
		end = len(s[start:])
	}
	return strings.TrimSpace(s[start : start+end])
}
