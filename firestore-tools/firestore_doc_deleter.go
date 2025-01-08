/* go run firestore_doc_deleter.go --credentials=your-credentials-file.json --collection="users/user123/posts" \
--documents="zTixTfcFSGYNa6Pf9UkE, Wz1nMaev6bvpcMRkcDJN, 3c073dGOusGmYYjf3ENJ"

go run firestore_doc_deleter.go --credentials=your-credentials-file.json --collection="users" --documents="user123,user456" \
--subcollection="posts" --dryrun=true

Delete specific documents within a subcollection:

go run firestore_deleter.go --credentials=your-credentials-file.json --collection="users" \
--documents="user123" --subcollection="posts" --subdocuments="post123,post456" --dryrun=false

Restore specific documents that were deleted (when specifying a document,
must be the original name of the document, not the current name in deleted_documents collection):

go run ./firestore_doc_deleter.go \
  --credentials=/path/to/credentials.json \
  --restore \
  --backup="deleted_documents" \
  --collection="roadmapItems" \
  --restore-documents="doc1,doc2,doc3"

go run ./firestore_doc_deleter.go \
  --credentials=/path/to/credentials.json \
  --restore \
  --backup="deleted_documents" \
  --collection="roadmapItems"

go run ./firestore_doc_deleter.go \
  --credentials=/path/to/credentials.json \
  --restore \
  --backup="deleted_documents" \
  --collection="roadmapItems"

New Flags:

--subcollection: (Optional) Specifies the path to a subcollection under the main documents.
Examples: posts, orders/items, users/user123/friends etc.

--subdocuments: (Optional) A comma-separated list of document IDs within the specified subcollection.
If this is empty and --subcollection is provided, the entire subcollection will be deleted.

# Dry run (no actual deletions)
go run firestore_deleter.go --credentials=your-credentials-file.json --collection="users" \
--documents="user123,user456" --subcollection="posts" --backup="deleted_documents" --dryrun=true

# Actual deletion with backup
go run firestore_deleter.go --credentials=your-credentials-file.json --collection="users" \
--documents="user123,user456" --subcollection="posts" --backup="deleted_documents" --dryrun=false

*/

package main

import (
        "context"
        "encoding/json"
        "errors"
        "flag"
        "fmt"
        "io/ioutil"
        "log"
        "os"
        "os/signal"

        //"signal"
        "strings"
        "sync"
        "syscall"
        "time"

        "cloud.google.com/go/firestore"
        firebase "firebase.google.com/go"
        "google.golang.org/api/iterator"
        "google.golang.org/api/option"
        "google.golang.org/grpc/codes"
        "google.golang.org/grpc/status"
)

type FirebaseConfig struct {
        ProjectID   string `json:"projectId"`
        DatabaseURL string `json:"databaseURL"` // You might need this for other Firebase services
        // Add other fields as needed from your Firebase config
}

const (
        MaxDocumentsPerBatch    = 500
        MaxConcurrentOperations = 10
        OperationsPerSecond     = 500 // Consider making this configurable
        RestoreTimeout          = 5 * time.Minute
        BatchSize               = 500
)

type DeletionError struct {
        Operation string
        Path      string
        Err       error
}

func (e *DeletionError) Error() string {
        return fmt.Sprintf("%s failed for %s: %v", e.Operation, e.Path, e.Err)
}

type DeletionStats struct {
        TotalAttempted int
        Successful     int
        Failed         int
        SkippedDryRun  int
        StartTime      time.Time
        EndTime        time.Time
        Errors         []DeletionError
        mu             sync.Mutex
}

func (s *DeletionStats) addResult(success bool, err error) {
        s.mu.Lock()
        defer s.mu.Unlock()

        if success {
                s.Successful++
        } else {
                s.Failed++
                if err != nil {
                        s.Errors = append(s.Errors, DeletionError{
                                Operation: "delete",
                                Path:      "",
                                Err:       err,
                        })
                }
        }
}

func (s *DeletionStats) printSummary() {
        duration := s.EndTime.Sub(s.StartTime)
        log.Printf("\nDeletion Summary:")
        log.Printf("Total Attempted: %d", s.TotalAttempted)
        log.Printf("Successful: %d", s.Successful)
        log.Printf("Failed: %d", s.Failed)
        log.Printf("Duration: %v", duration)
        if len(s.Errors) > 0 {
                log.Printf("\nErrors encountered:")
                for _, err := range s.Errors {
                        log.Printf("- %v", err)
                }
        }
}

type RateLimiter struct {
        tokens chan struct{}
        rate   time.Duration
}

func NewRateLimiter(maxConcurrent int, rate time.Duration) *RateLimiter {
        rl := &RateLimiter{
                tokens: make(chan struct{}, maxConcurrent),
                rate:   rate,
        }

        // Fill token bucket
        for i := 0; i < maxConcurrent; i++ {
                rl.tokens <- struct{}{}
        }

        return rl
}

func (rl *RateLimiter) acquire(ctx context.Context) error {
        select {
        case <-ctx.Done():
                return ctx.Err()
        case <-rl.tokens:
                time.AfterFunc(rl.rate, func() {
                        rl.tokens <- struct{}{}
                })
                return nil
        }
}

type ProgressTracker struct {
        Total     int
        Current   int
        StartTime time.Time
        mu        sync.Mutex
}

func (p *ProgressTracker) Update(n int) {
        p.mu.Lock()
        defer p.mu.Unlock()
        p.Current += n
        elapsed := time.Since(p.StartTime)
        log.Printf("Progress: %d/%d (%.1f%%) - Elapsed: %v",
                p.Current, p.Total,
                float64(p.Current)/float64(p.Total)*100,
                elapsed.Round(time.Second))
}

func validateSubcollectionPath(path string) error {
        if path == "" {
                return nil
        }
        parts := strings.Split(path, "/")
        if len(parts) > 10 {
                return fmt.Errorf("subcollection path too deep (max 10 levels): %s", path)
        }
        return nil
}

func main() {
        sigChan := make(chan os.Signal, 1)
        signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

        go func() {
                <-sigChan
                log.Println("Received shutdown signal, cleaning up...")
                // Cleanup code here
                os.Exit(0)
        }()

        // Flags
        credentialsFile := flag.String("credentials", "", "Path to the Firebase credentials JSON file.")
        collectionPath := flag.String("collection", "", "Path to the main collection (e.g., 'users').")
        documentIDsStr := flag.String("documents", "", "Comma-separated list of document IDs under the main collection.")
        subcollectionPath := flag.String("subcollection", "", "Optional path to a subcollection under the specified documents (e.g., 'posts' or 'posts/postId/comments').")
        subcollectionDocumentIDsStr := flag.String("subdocuments", "", "Optional comma-separated list of document IDs to delete within the subcollection. If empty, all documents in the subcollection will be deleted.")
        dryRun := flag.Bool("dryrun", true, "Perform a dry run (log actions without deleting).")
        restore := flag.Bool("restore", false, "Restore documents from the backup collection.")
        restoreDocIdsStr := flag.String("restore-documents", "", "Comma-separated list of document IDs to restore from backup")
        backupCollectionName := flag.String("backup", "deleted_documents", "Name of the collection to store backups.")
        timeout := flag.Duration("timeout", 30*time.Minute, "Operation timeout")
        ctx, cancel := context.WithTimeout(context.Background(), *timeout)
        defer cancel()
        flag.Parse()

        // Validate flags
        if *credentialsFile == "" {
                log.Fatal("Error: --credentials flag is required.")
        }
        if *collectionPath == "" && !*restore {
                log.Fatal("Error: --collection flag is required if not used with restore.")
        }
        if *documentIDsStr == "" && !*restore {
                log.Fatal("Error: --documents flag is required unless --restore is set.")
        }

        if *restore && *backupCollectionName == "" {
                log.Fatal("Error: --backup flag is required when --restore is set.")
        }

        var restoreDocIDs []string
        if *restore && *restoreDocIdsStr != "" {
                restoreDocIDs = cleanDocumentIDs(*restoreDocIdsStr)
                log.Printf("Will restore specific documents: %v", restoreDocIDs)
        }

        // Clean up the document IDs
        documentIDs := cleanDocumentIDs(*documentIDsStr)
        subcollectionDocumentIDs := cleanDocumentIDs(*subcollectionDocumentIDsStr)

        // Read Firebase project ID from credentials file
        config, err := readFirebaseConfig(*credentialsFile)
        if err != nil {
                log.Fatalf("Error reading Firebase config: %v", err)
        }

        // Initialize Firestore client
        client, err := initializeFirestoreClient(ctx, config.ProjectID, *credentialsFile)
        if err != nil {
                log.Fatalf("Error initializing Firestore client: %v", err)
        }
        defer client.Close()

        // Log run mode
        if *dryRun {
                log.Println("Running in DRY RUN mode. No documents will be deleted or restored.")
        } else if *restore {
                log.Println("Running in RESTORE mode. Documents will be restored from the backup collection.")
        } else {
                log.Println("WARNING: Running in DESTRUCTIVE mode. Documents will be deleted.")
        }

        if *restore {
                err = restoreDocuments(ctx, client, *backupCollectionName, *collectionPath, restoreDocIDs)
                if err != nil {
                        log.Fatalf("Error restoring documents: %v", err)
                }
                return // Exit after restore operation
        }

        // Delete documents
        stats, err := deleteDocuments(ctx, client, *collectionPath, documentIDs, *subcollectionPath, subcollectionDocumentIDs, *dryRun, *backupCollectionName)
        if err != nil {
                log.Fatalf("Error deleting documents: %v", err)
        }

        // Log results
        stats.printSummary()
}

// readFirebaseConfig reads the Firebase project ID from the credentials JSON file.
func readFirebaseConfig(credentialsFile string) (*FirebaseConfig, error) {
        data, err := ioutil.ReadFile(credentialsFile)
        if err != nil {
                return nil, fmt.Errorf("error reading credentials file: %v", err)
        }

        var config FirebaseConfig
        err = json.Unmarshal(data, &config)
        if err != nil {
                return nil, fmt.Errorf("error unmarshalling credentials JSON: %v", err)
        }

        return &config, nil
}

// initializeFirestoreClient initializes a Firestore client.
func initializeFirestoreClient(ctx context.Context, projectID, credentialsFile string) (*firestore.Client, error) {
        // Check if the file exists and has correct permissions
        if _, err := os.Stat(credentialsFile); os.IsNotExist(err) {
                return nil, fmt.Errorf("credentials file does not exist: %v", err)
        }
        if err := checkFilePermissions(credentialsFile); err != nil {
                return nil, err
        }

        conf := &firebase.Config{ProjectID: projectID}
        opt := option.WithCredentialsFile(credentialsFile)
        app, err := firebase.NewApp(ctx, conf, opt)
        if err != nil {
                return nil, fmt.Errorf("error initializing Firebase app: %v", err)
        }

        client, err := app.Firestore(ctx)
        if err != nil {
                return nil, fmt.Errorf("error creating Firestore client: %v", err)
        }

        return client, nil
}

// checkFilePermissions checks if the file has secure permissions (e.g., 0600).
func checkFilePermissions(filepath string) error {
        info, err := os.Stat(filepath)
        if err != nil {
                return fmt.Errorf("error getting file info: %v", err)
        }

        // Check for owner read/write permissions only (0600)
        if info.Mode().Perm() != 0600 {
                return fmt.Errorf("insecure file permissions for %s: %v. Please set to 0600 (owner read/write only)", filepath, info.Mode().Perm())
        }

        return nil
}

// cleanDocumentIDs cleans and splits the document IDs string.
func cleanDocumentIDs(documentIDsStr string) []string {
        var documentIDs []string
        if documentIDsStr == "" {
                return documentIDs
        }

        for _, id := range strings.Split(documentIDsStr, ",") {
                id = strings.TrimSpace(id)
                id = strings.Trim(id, "\"") // Remove surrounding quotes
                if id != "" {
                        documentIDs = append(documentIDs, id)
                }
        }
        return documentIDs
}

// deleteDocuments deletes documents and subcollections based on the provided parameters.
func deleteDocuments(ctx context.Context, client *firestore.Client, collectionPath string, documentIDs []string, subcollectionPath string, subcollectionDocumentIDs []string, dryRun bool, backupCollectionName string) (*DeletionStats, error) {
        stats := &DeletionStats{
                StartTime: time.Now(),
                Errors:    []DeletionError{},
        }

        if err := validateInput(documentIDs, subcollectionDocumentIDs); err != nil {
                return stats, fmt.Errorf("input validation failed: %v", err)
        }

        if err := validateSubcollectionPath(subcollectionPath); err != nil {
                return stats, fmt.Errorf("subcollection path validation failed: %v", err)
        }

        rateLimiter := NewRateLimiter(MaxConcurrentOperations, time.Second/OperationsPerSecond)
        collectionRef := client.Collection(collectionPath)
        var wg sync.WaitGroup

        docsToProcess := len(documentIDs)
        if subcollectionPath != "" {
                for _, docID := range documentIDs {
                        subcollectionRef, _ := getCollectionRef(collectionRef.Doc(docID), subcollectionPath)
                        snapshots, err := subcollectionRef.Documents(ctx).GetAll()
                        if err != nil {
                                return nil, err
                        }
                        subDocRefs := make([]*firestore.DocumentRef, len(snapshots))
                        for i, snap := range snapshots {
                                subDocRefs[i] = snap.Ref
                        }
                        docsToProcess += len(subDocRefs)
                }
        }

        tracker := ProgressTracker{
                Total:     docsToProcess,
                StartTime: time.Now(),
        }
        tracker.Update(0)

        for _, docID := range documentIDs {
                docRef := collectionRef.Doc(docID)

                // Handle subcollection deletion
                if subcollectionPath != "" {
                        subcollectionRef, subPaths := getCollectionRef(docRef, subcollectionPath)

                        var subDocRefs []*firestore.DocumentRef

                        if len(subcollectionDocumentIDs) > 0 {
                                // Delete specific documents within the subcollection
                                for _, subDocID := range subcollectionDocumentIDs {
                                        subDocRef := subcollectionRef.Doc(subDocID)
                                        subDocRefs = append(subDocRefs, subDocRef)
                                }
                        } else {
                                // Get all documents in the subcollection for deletion
                                snapshots, err := subcollectionRef.Documents(ctx).GetAll()
                                if err != nil {
                                        log.Printf("Error getting documents from subcollection %s: %v", subcollectionPath, err)
                                        continue
                                }
                                subDocRefs = make([]*firestore.DocumentRef, len(snapshots))
                                for i, snap := range snapshots {
                                        subDocRefs[i] = snap.Ref
                                }
                        }

                        for _, subDocRef := range subDocRefs {
                                wg.Add(1)
                                if err := rateLimiter.acquire(ctx); err != nil {
                                        log.Printf("Failed to acquire rate limiter token: %v", err)
                                        continue
                                }
                                go func(subDocRef *firestore.DocumentRef) {
                                        defer wg.Done()
                                        fullPath := append([]string{collectionPath, docID}, subPaths...) // Include subcollection path
                                        fullPath = append(fullPath, subDocRef.ID)

                                        if dryRun {
                                                log.Printf("[DRY RUN] Would delete subcollection document: %s", strings.Join(fullPath, "/"))
                                                stats.mu.Lock()
                                                stats.SkippedDryRun++
                                                stats.mu.Unlock()
                                        } else {
                                                op := func() error {
                                                        return deleteDocumentWithBackup(ctx, client, subDocRef, backupCollectionName)
                                                }
                                                if err := deleteWithRetry(ctx, op); err != nil {
                                                        log.Printf("Error deleting subcollection document %s: %v", strings.Join(fullPath, "/"), err)
                                                        stats.addResult(false, fmt.Errorf("error deleting subcollection document %s: %v", strings.Join(fullPath, "/"), err))
                                                } else {
                                                        log.Printf("Deleted subcollection document: %s", strings.Join(fullPath, "/"))
                                                        stats.addResult(true, nil)
                                                }
                                        }
                                        tracker.Update(1)
                                }(subDocRef)
                        }
                } else {
                        // Delete the main document (and its subcollections if any)
                        fullPath := []string{collectionPath, docID}
                        wg.Add(1)
                        if err := rateLimiter.acquire(ctx); err != nil {
                                log.Printf("Failed to acquire rate limiter token: %v", err)
                                continue
                        }
                        go func(docRef *firestore.DocumentRef) {
                                defer wg.Done()
                                if dryRun {
                                        log.Printf("[DRY RUN] Would delete document (and its subcollections): %s", strings.Join(fullPath, "/"))
                                        stats.mu.Lock()
                                        stats.SkippedDryRun++
                                        stats.mu.Unlock()
                                } else {
                                        log.Printf("Deleting document (and its subcollections): %s", strings.Join(fullPath, "/"))
                                        if err := deleteDocumentAndSubcollections(ctx, client, docRef, backupCollectionName); err != nil {
                                                log.Printf("Error deleting document %s or its subcollections: %v", strings.Join(fullPath, "/"), err)
                                                stats.addResult(false, fmt.Errorf("error deleting document %s or its subcollections: %v", strings.Join(fullPath, "/"), err))
                                        } else {
                                                log.Printf("Deleted document and its subcollections: %s", strings.Join(fullPath, "/"))
                                                stats.addResult(true, nil)
                                        }
                                }
                                tracker.Update(1)
                        }(docRef)
                }
        }

        wg.Wait()
        stats.EndTime = time.Now()
        stats.TotalAttempted = stats.Successful + stats.Failed + stats.SkippedDryRun
        return stats, nil
}

// getCollectionRef takes a collection path string and returns the reference to the
// collection along with the individual path segments.
func getCollectionRef(docRef *firestore.DocumentRef, subcollectionPath string) (*firestore.CollectionRef, []string) {
        paths := strings.Split(subcollectionPath, "/")
        var collectionRef *firestore.CollectionRef

        for i, pathSegment := range paths {
                if i == 0 {
                        collectionRef = docRef.Collection(pathSegment)
                } else if i%2 == 0 {
                        collectionRef = collectionRef.Doc(paths[i-1]).Collection(pathSegment)
                }
        }
        return collectionRef, paths
}

// deleteDocumentAndSubcollections recursively deletes a document and all its subcollections.
func deleteDocumentAndSubcollections(ctx context.Context, client *firestore.Client, docRef *firestore.DocumentRef, backupCollectionName string) error {
        // Backup the document before deleting
        if err := backupDocument(ctx, client, docRef, backupCollectionName); err != nil {
                return fmt.Errorf("error backing up document %s: %v", docRef.Path, err)
        }

        // Delete subcollections first
        subCollections := docRef.Collections(ctx)
        for {
                col, err := subCollections.Next()
                if err == iterator.Done {
                        break
                }
                if err != nil {
                        return fmt.Errorf("error getting subcollections: %v", err)
                }
                if err := deleteCollection(ctx, client, col); err != nil {
                        return fmt.Errorf("error deleting subcollection %s: %v", col.Path, err)
                }
        }

        // Delete the document
        op := func() error {
                _, err := docRef.Delete(ctx)
                return err
        }
        if err := deleteWithRetry(ctx, op); err != nil {
                return fmt.Errorf("error deleting document %s: %v", docRef.Path, err)
        }

        return nil
}

// deleteCollection deletes all documents in a collection with retries.
func deleteCollection(ctx context.Context, client *firestore.Client, colRef *firestore.CollectionRef) error {
        for {
                iter := colRef.Limit(MaxDocumentsPerBatch).Documents(ctx)
                numDeleted := 0
                batch := client.Batch()

                for {
                        doc, err := iter.Next()
                        if err == iterator.Done {
                                break
                        }
                        if err != nil {
                                return fmt.Errorf("error iterating documents: %v", err)
                        }

                        batch.Delete(doc.Ref)
                        numDeleted++
                }

                // If there are no documents to delete, we're done
                if numDeleted == 0 {
                        return nil
                }

                // Commit the batch
                op := func() error {
                        _, err := batch.Commit(ctx)
                        return err
                }
                if err := deleteWithRetry(ctx, op); err != nil {
                        return fmt.Errorf("error committing batch delete: %v", err)
                }
        }
}

func deleteWithRetry(ctx context.Context, operation func() error) error {
        maxRetries := 3
        backoff := time.Second

        for i := 0; i < maxRetries; i++ {
                err := operation()
                if err == nil {
                        return nil
                }

                if i < maxRetries-1 {
                        select {
                        case <-ctx.Done():
                                return ctx.Err()
                        case <-time.After(backoff):
                                backoff *= 2 // Exponential backoff
                                continue
                        }
                }
                return err
        }
        return nil
}

func deleteDocumentWithBackup(ctx context.Context, client *firestore.Client, docRef *firestore.DocumentRef, backupCollectionName string) error {
        // Backup the document
        if err := backupDocument(ctx, client, docRef, backupCollectionName); err != nil {
                return fmt.Errorf("error backing up document %s: %v", docRef.Path, err)
        }

        // Delete the document
        _, err := docRef.Delete(ctx)
        if err != nil {
                return fmt.Errorf("error deleting document %s: %v", docRef.Path, err)
        }

        return nil
}

func backupDocument(ctx context.Context, client *firestore.Client, docRef *firestore.DocumentRef, backupCollectionName string) error {
        doc, err := docRef.Get(ctx)
        if err != nil {
                return fmt.Errorf("error getting document %s: %v", docRef.Path, err)
        }

        backupDocRef := client.Collection(backupCollectionName).NewDoc()
        _, err = backupDocRef.Set(ctx, map[string]interface{}{
                "originalPath": docRef.Path,
                "data":         doc.Data(),
                "deletedAt":    time.Now(),
        })

        if err != nil {
                return fmt.Errorf("error backing up document %s to %s: %v", docRef.Path, backupCollectionName, err)
        }

        log.Printf("Backed up document %s to %s", docRef.Path, backupDocRef.Path)
        return nil
}

func restoreDocuments(ctx context.Context, client *firestore.Client, backupCollectionName string, collectionPath string, documentIDs []string) error {
        log.Printf("Starting restore from backup collection: %s", backupCollectionName)
        if collectionPath != "" {
                log.Printf("Filtering for documents in collection: %s", collectionPath)
        }
        if len(documentIDs) > 0 {
                log.Printf("Filtering for specific documents: %v", documentIDs)
        }

        restoreCtx, cancel := context.WithTimeout(ctx, RestoreTimeout)
        defer cancel()

        iter := client.Collection(backupCollectionName).Documents(restoreCtx)
        for {
                doc, err := iter.Next()
                if err == iterator.Done {
                        break
                }
                if err != nil {
                        return fmt.Errorf("error iterating backup documents: %v", err)
                }

                log.Printf("Processing backup document: %s", doc.Ref.Path)

                backupData := doc.Data()
                originalPath, ok := backupData["originalPath"].(string)
                if !ok {
                        log.Printf("Skipping document %s: missing or invalid originalPath", doc.Ref.Path)
                        continue
                }

                // Convert full Firestore path to relative path
                parts := strings.Split(originalPath, "/documents/")
                if len(parts) != 2 {
                        log.Printf("Skipping document %s: invalid path format", doc.Ref.Path)
                        continue
                }
                relativePath := parts[1]

                // Extract document ID from the path
                pathParts := strings.Split(relativePath, "/")
                docID := pathParts[len(pathParts)-1]

                // Skip if not in the list of documents to restore (when list is provided)
                if len(documentIDs) > 0 {
                        found := false
                        for _, id := range documentIDs {
                                if id == docID {
                                        found = true
                                        break
                                }
                        }
                        if !found {
                                log.Printf("Skipping document %s: not in restore list", docID)
                                continue
                        }
                }

                // Check collection match using relative path
                if collectionPath != "" {
                        if !strings.HasPrefix(relativePath, collectionPath+"/") {
                                log.Printf("Skipping document %s: path does not match collection %s", doc.Ref.Path, collectionPath)
                                continue
                        }
                }

                docData, ok := backupData["data"].(map[string]interface{})
                if !ok {
                        log.Printf("Skipping document %s: missing or invalid data field", doc.Ref.Path)
                        continue
                }

                // Use relative path to create document reference
                originalDocRef := client.Doc(relativePath)
                err = client.RunTransaction(restoreCtx, func(ctx context.Context, tx *firestore.Transaction) error {
                        originalDoc, err := tx.Get(originalDocRef)
                        if err != nil {
                                if status.Code(err) == codes.NotFound {
                                        if err := tx.Set(originalDocRef, docData); err != nil {
                                                return fmt.Errorf("error restoring document %s: %v", relativePath, err)
                                        }
                                        return nil
                                }
                                return fmt.Errorf("error checking document %s: %v", relativePath, err)
                        }

                        // If document exists, skip it
                        if originalDoc.Exists() {
                                return fmt.Errorf("document %s already exists", relativePath)
                        }

                        // Default case: restore the document
                        if err := tx.Set(originalDocRef, docData); err != nil {
                                return fmt.Errorf("error restoring document %s: %v", relativePath, err)
                        }

                        return nil
                })

                if err != nil {
                        log.Printf("Error restoring document %s: %v", relativePath, err)
                } else {
                        log.Printf("Successfully restored document %s", relativePath)
                        // Delete the backup document
                        if _, err := doc.Ref.Delete(restoreCtx); err != nil {
                                log.Printf("Warning: Could not delete backup document %s: %v", doc.Ref.Path, err)
                        }
                }
        }

        return nil
}

func validateInput(documentIDs []string, subcollectionDocumentIDs []string) error {
        if len(documentIDs) == 0 {
                return errors.New("no documents specified for deletion")
        }

        if len(documentIDs) > MaxDocumentsPerBatch {
                return fmt.Errorf("too many documents requested for deletion (max %d): %d",
                        MaxDocumentsPerBatch, len(documentIDs))
        }

        // Validate document ID format
        for _, id := range documentIDs {
                if strings.Contains(id, "/") {
                        return fmt.Errorf("invalid document ID format: %s (should not contain '/')", id)
                }
                if len(id) == 0 {
                        return errors.New("empty document ID provided")
                }
        }

        return nil
}

func deleteInBatches(ctx context.Context, client *firestore.Client, refs []*firestore.DocumentRef) error {
        for i := 0; i < len(refs); i += BatchSize {
                end := i + BatchSize
                if end > len(refs) {
                        end = len(refs)
                }
                batch := client.Batch()
                for _, ref := range refs[i:end] {
                        batch.Delete(ref)
                }
                if _, err := batch.Commit(ctx); err != nil {
                        return err
                }
        }
        return nil
}
