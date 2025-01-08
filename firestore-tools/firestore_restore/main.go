package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	"firestore-restore/backup"
	"firestore-restore/restore"
	"firestore-restore/utils"
)

type BackupResponse struct {
	Backups                 []string `json:"backups"`
	GranularRestorePossible bool     `json:"granularRestorePossible"`
	Error                   string   `json:"error,omitempty"`
}

type TransferRequest struct {
	Type                  string   `json:"type"`
	Paths                 []string `json:"paths"`
	SourceProjectID       string   `json:"sourceProjectId"`
	SourceDb              string   `json:"sourceDb"`
	DestProjectID         string   `json:"destProjectId"`
	DestDb                string   `json:"destDb"`
	IncludeSubcollections bool     `json:"includeSubcollections"`
	OverwriteExisting     bool     `json:"overwriteExisting"`
}

// RestoreOperation tracks the status of a restore
type RestoreOperation struct {
	ID            string    `json:"id"`
	StartTime     time.Time `json:"startTime"`
	Status        string    `json:"status"` // "running", "completed", "failed"
	Error         string    `json:"error,omitempty"`
	SourceProject string    `json:"sourceProject"`
	DestProject   string    `json:"destProject"`
	DestDB        string    `json:"destDb"`
	LogChan       chan string
	Done          chan bool
	Cancel        context.CancelFunc
}

// Global map to track restore operations
var (
	activeRestores = make(map[string]*RestoreOperation)
	restoreMutex   sync.RWMutex
)

// LogWriter captures log output for streaming
type LogWriter struct {
	messages chan string
	stdout   *log.Logger
	mu       sync.RWMutex
	closed   bool
	lastLog  time.Time
}

func NewLogWriter() *LogWriter {
	return &LogWriter{
		messages: make(chan string, 1000),
		stdout:   log.New(os.Stdout, "", log.LstdFlags),
		closed:   false,
		lastLog:  time.Now(),
	}
}

func (w *LogWriter) Write(p []byte) (n int, err error) {
	msg := string(p)
	// Write to stdout first
	w.stdout.Print(strings.TrimSpace(msg))

	w.mu.RLock()
	if w.closed {
		w.mu.RUnlock()
		return len(p), nil
	}
	w.mu.RUnlock()

	// Rate limit logging to once every 100ms
	if time.Since(w.lastLog) < 100*time.Millisecond {
		return len(p), nil
	}
	w.lastLog = time.Now()

	// Try to send to channel, but don't block if it's full
	select {
	case w.messages <- msg:
	default:
		// Channel is full, but we don't want to block or lose the message
		// Try again after a short delay
		go func() {
			time.Sleep(10 * time.Millisecond)
			select {
			case w.messages <- msg:
			default:
				// If still full after retry, log warning to stdout only
				w.stdout.Print("Warning: log message dropped due to full channel")
			}
		}()
	}

	return len(p), nil
}

func (w *LogWriter) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		w.closed = true
		close(w.messages)
	}
}

type QueryRequest struct {
	ProjectID  string      `json:"projectId"`
	DbID       string      `json:"dbId"`
	Collection string      `json:"collection"`
	Field      string      `json:"field"`
	Operator   string      `json:"operator"`
	Value      interface{} `json:"value"`
}

// Helper function to extract collection name from full path
func extractCollectionName(path string) string {
	// Handle full Firestore paths (e.g., "projects/projectId/databases/dbId/documents/collectionName")
	if strings.Contains(path, "/documents/") {
		parts := strings.Split(path, "/documents/")
		if len(parts) > 1 {
			// Get everything after "/documents/"
			return strings.TrimSpace(parts[1])
		}
	}
	// If it's not a full path, return as is
	return path
}

func handleQueryDocuments(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		log.Printf("Invalid method for query-documents: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		log.Printf("Error decoding query-documents request: %v", err)
		sendJSONResponse(w, map[string]interface{}{"error": "Invalid request format"})
		return
	}

	// Extract collection name from full path if necessary
	if request.Collection != "" {
		request.Collection = extractCollectionName(request.Collection)
	}

	// Log the request after successful decode
	log.Printf("Received request: %+v", request)
	log.Printf("Querying documents with filter: %s %s %v",
		request.Field, request.Operator, request.Value)

	ctx := context.Background()
	client, err := firestore.NewClientWithDatabase(ctx, request.ProjectID, request.DbID)
	if err != nil {
		log.Printf("Failed to create Firestore client: %v", err)
		sendJSONResponse(w, map[string]interface{}{"error": fmt.Sprintf("Failed to create Firestore client: %v", err)})
		return
	}
	defer client.Close()

	docs := make([]map[string]interface{}, 0)
	docCount := 0

	if request.Collection != "" {
		// Search in specific collection
		log.Printf("Searching in collection: %s", request.Collection)
		results, count, err := searchCollection(ctx, client, request.Collection, request.Field, request.Operator, request.Value)
		if err != nil {
			log.Printf("Error searching collection %s: %v", request.Collection, err)
			sendJSONResponse(w, map[string]interface{}{"error": fmt.Sprintf("Failed to search collection: %v", err)})
			return
		}
		docs = append(docs, results...)
		docCount += count
	} else {
		// Search across all collections
		log.Printf("No specific collection provided, searching all collections")
		collections := client.Collections(ctx)
		for {
			col, err := collections.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Printf("Error listing collections: %v", err)
				continue
			}

			log.Printf("Searching collection: %s", col.ID)
			results, count, err := searchCollection(ctx, client, col.ID, request.Field, request.Operator, request.Value)
			if err != nil {
				log.Printf("Error searching collection %s: %v", col.ID, err)
				continue
			}
			docs = append(docs, results...)
			docCount += count
		}
	}

	log.Printf("Successfully found %d documents matching query", docCount)
	sendJSONResponse(w, map[string]interface{}{
		"documents": docs,
	})
}

// Helper function to search a single collection
func searchCollection(ctx context.Context, client *firestore.Client, collection, field, operator string, value interface{}) ([]map[string]interface{}, int, error) {
	query := client.Collection(collection).Query
	switch operator {
	case "==":
		query = query.Where(field, "==", value)
	case "!=":
		query = query.Where(field, "!=", value)
	case ">":
		query = query.Where(field, ">", value)
	case ">=":
		query = query.Where(field, ">=", value)
	case "<":
		query = query.Where(field, "<", value)
	case "<=":
		query = query.Where(field, "<=", value)
	case "array-contains":
		query = query.Where(field, "array-contains", value)
	default:
		return nil, 0, fmt.Errorf("invalid operator: %s", operator)
	}

	iter := query.Documents(ctx)
	docs := make([]map[string]interface{}, 0)
	docCount := 0

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, docCount, fmt.Errorf("error querying documents: %v", err)
		}

		docCount++
		log.Printf("Found document %d in collection %s: ID=%s, Path=%s",
			docCount, collection, doc.Ref.ID, doc.Ref.Path)
		docs = append(docs, map[string]interface{}{
			"id":   doc.Ref.ID,
			"path": doc.Ref.Path,
			"data": doc.Data(),
		})
	}

	return docs, docCount, nil
}

func main() {
	r := mux.NewRouter()

	// Serve static files
	fs := http.FileServer(http.Dir("static"))
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", fs))

	// Handle routes
	r.HandleFunc("/", handleHome)
	r.HandleFunc("/list-backups", handleListBackups)
	r.HandleFunc("/restore-logs", handleRestoreLogs)
	r.HandleFunc("/list-collections", handleListCollections)
	r.HandleFunc("/get-document", handleGetDocument)
	r.HandleFunc("/list-documents", handleListDocuments)
	r.HandleFunc("/transfer-data", handleTransferData)
	r.HandleFunc("/list-restores", handleListRestores)
	r.HandleFunc("/ws", handleWebSocket)
	r.HandleFunc("/query-documents", handleQueryDocuments)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Server starting on port %s", port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		log.Fatal(err)
	}
}

func handleHome(w http.ResponseWriter, r *http.Request) {
	tmpl := template.Must(template.ParseFiles("templates/index.html"))
	tmpl.Execute(w, nil)
}

func handleListBackups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := context.Background()
	sourceProjectID := r.FormValue("sourceProjectId")
	sourceDb := r.FormValue("sourceDb")
	if sourceDb == "" {
		sourceDb = "(default)"
	}
	source := r.FormValue("source")
	backupSource := r.FormValue("backupSource")
	sourceServiceAccountKey := r.FormValue("serviceAccountKey")

	// Set up credentials for source operations
	if err := utils.SetupCredentials(sourceServiceAccountKey, ""); err != nil {
		sendJSONResponse(w, BackupResponse{Error: fmt.Sprintf("Failed to setup credentials: %v", err)})
		return
	}

	var backups []string
	var granularRestorePossible bool
	var err error

	switch source {
	case "pitr":
		pitrTime, err := time.Parse(time.RFC3339, backupSource)
		if err != nil {
			sendJSONResponse(w, BackupResponse{Error: "Invalid PITR time format"})
			return
		}
		backups, granularRestorePossible, err = listPITRCollections(ctx, sourceProjectID, pitrTime)

	case "gcs":
		backups, granularRestorePossible, err = listGCSBackups(ctx, backupSource)
	}

	if err != nil {
		sendJSONResponse(w, BackupResponse{Error: err.Error()})
		return
	}

	sendJSONResponse(w, BackupResponse{
		Backups:                 backups,
		GranularRestorePossible: granularRestorePossible,
	})
}

func handleRestoreLogs(w http.ResponseWriter, r *http.Request) {
	// Set headers for SSE
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Check if this is a reconnection
	restoreID := r.URL.Query().Get("restoreId")
	if restoreID != "" {
		// Try to resume existing restore
		restoreMutex.RLock()
		restoreOp, exists := activeRestores[restoreID]
		restoreMutex.RUnlock()

		if exists {
			log.Printf("Reconnecting to restore %s (status: %s)", restoreID, restoreOp.Status)
			// Stream existing restore logs immediately
			streamLogsToClient(context.Background(), w, restoreOp)
			return
		} else {
			log.Printf("Restore %s not found", restoreID)
			sendSSEMessage(w, map[string]interface{}{
				"error": fmt.Sprintf("Restore %s not found or expired", restoreID),
			})
			return
		}
	}

	// Create a separate context for the restore operation that won't be canceled when client disconnects
	restoreCtx := context.Background()

	// Create a context that's canceled when the client disconnects (only for SSE streaming)
	streamCtx, streamCancel := context.WithCancel(r.Context())
	defer streamCancel()

	// Parse query parameters
	sourceProjectID := r.URL.Query().Get("sourceProjectId")
	destProjectID := r.URL.Query().Get("destProjectId")
	destDB := r.URL.Query().Get("destDb")
	source := r.URL.Query().Get("source")
	backupSource := r.URL.Query().Get("backupSource")
	granularity := r.URL.Query().Get("granularity")
	collectionID := r.URL.Query().Get("collectionId")
	documentID := r.URL.Query().Get("documentId")
	userEmail := r.URL.Query().Get("userEmail")

	// Create new restore operation
	restoreOp := &RestoreOperation{
		ID:            uuid.New().String(),
		StartTime:     time.Now(),
		Status:        "running",
		SourceProject: sourceProjectID,
		DestProject:   destProjectID,
		DestDB:        destDB,
		LogChan:       make(chan string, 100),
		Done:          make(chan bool),
	}

	// Add to active restores
	restoreMutex.Lock()
	activeRestores[restoreOp.ID] = restoreOp
	restoreMutex.Unlock()

	// Send restore ID to client
	sendSSEMessage(w, map[string]interface{}{
		"restoreId": restoreOp.ID,
	})

	// Create custom log writer that writes to the restore's log channel
	logWriter := NewLogWriter()
	defer logWriter.Close()

	// Set the log output to our custom writer
	originalLogger := log.Default()
	log.SetOutput(logWriter)
	defer log.SetOutput(originalLogger.Writer())

	// Start restore in a goroutine
	go func() {
		defer func() {
			close(restoreOp.LogChan)
			close(restoreOp.Done)

			// Keep the restore in activeRestores for 1 hour before cleanup
			time.AfterFunc(1*time.Hour, func() {
				restoreMutex.Lock()
				delete(activeRestores, restoreOp.ID)
				restoreMutex.Unlock()
			})
		}()

		var err error
		if source == "pitr" {
			pitrTime, err := time.Parse(time.RFC3339, backupSource)
			if err != nil {
				restoreOp.Status = "failed"
				restoreOp.Error = fmt.Sprintf("Invalid PITR timestamp: %v", err)
				return
			}
			granularityInt := 0
			if granularity == "collection" {
				granularityInt = 1
			}
			err = restore.RestoreFromPITR(restoreCtx, sourceProjectID, destProjectID, destDB, pitrTime, granularityInt, collectionID)
		} else {
			granularityInt := 0
			if granularity == "collection" {
				granularityInt = 1
			}
			err = restore.RestoreFromGCS(restoreCtx, sourceProjectID, destProjectID, destDB, backupSource, granularityInt, collectionID, documentID, userEmail)
		}

		if err != nil {
			restoreOp.Status = "failed"
			if restoreErr, ok := err.(*restore.RestoreError); ok {
				restoreOp.Error = fmt.Sprintf("%s\n%s", restoreErr.Message, restoreErr.Commands)
			} else {
				restoreOp.Error = err.Error()
			}
			return
		}

		restoreOp.Status = "completed"
	}()

	// Stream logs to client using the streaming context
	streamLogsToClient(streamCtx, w, restoreOp)
}

func streamLogsToClient(ctx context.Context, w http.ResponseWriter, restoreOp *RestoreOperation) {
	// Then stream logs
	for {
		select {
		case <-ctx.Done():
			// Client disconnected, clean up but let restore continue
			log.Printf("Client disconnected from restore %s", restoreOp.ID)
			return

		case msg, ok := <-restoreOp.LogChan:
			if !ok {
				// Channel closed, check final status
				log.Printf("Restore %s completed with status: %s", restoreOp.ID, restoreOp.Status)
				if err := sendSSEMessage(w, map[string]interface{}{
					"status":        restoreOp.Status,
					"error":         restoreOp.Error,
					"complete":      restoreOp.Status == "completed",
					"sourceProject": restoreOp.SourceProject,
					"destProject":   restoreOp.DestProject,
					"destDb":        restoreOp.DestDB,
				}); err != nil {
					log.Printf("Error sending final status: %v", err)
				}
				return
			}

			// Send log message and flush
			if err := sendSSEMessage(w, map[string]interface{}{
				"log": strings.TrimSpace(msg),
			}); err != nil {
				log.Printf("Error sending SSE message: %v", err)
				return
			}
			w.(http.Flusher).Flush()

		case <-restoreOp.Done:
			// Restore finished
			log.Printf("Restore %s finished with status: %s", restoreOp.ID, restoreOp.Status)
			if err := sendSSEMessage(w, map[string]interface{}{
				"status":        restoreOp.Status,
				"error":         restoreOp.Error,
				"complete":      restoreOp.Status == "completed",
				"sourceProject": restoreOp.SourceProject,
				"destProject":   restoreOp.DestProject,
				"destDb":        restoreOp.DestDB,
			}); err != nil {
				log.Printf("Error sending final status: %v", err)
			}
			return
		}
	}
}

// Add endpoint to list active restores
func handleListRestores(w http.ResponseWriter, r *http.Request) {
	restoreMutex.RLock()
	restores := make([]map[string]interface{}, 0, len(activeRestores))
	for _, op := range activeRestores {
		restores = append(restores, map[string]interface{}{
			"id":            op.ID,
			"startTime":     op.StartTime,
			"status":        op.Status,
			"error":         op.Error,
			"sourceProject": op.SourceProject,
			"destProject":   op.DestProject,
			"destDb":        op.DestDB,
		})
	}
	restoreMutex.RUnlock()

	sendJSONResponse(w, map[string]interface{}{
		"restores": restores,
	})
}

func sendSSEMessage(w http.ResponseWriter, data map[string]interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error marshaling SSE data: %v", err)
	}

	_, err = fmt.Fprintf(w, "data: %s\n\n", jsonData)
	if err != nil {
		return fmt.Errorf("error writing SSE message: %v", err)
	}

	return nil
}

func sendSSEError(w http.ResponseWriter, errMsg string) error {
	return sendSSEMessage(w, map[string]interface{}{
		"error": errMsg,
	})
}

func handleListCollections(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		ProjectID         string `json:"projectId"`
		DbID              string `json:"dbId"`
		ServiceAccountKey string `json:"serviceAccountKey"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendJSONResponse(w, map[string]interface{}{"error": "Invalid request format"})
		return
	}

	// Set up credentials for the operation
	if err := utils.SetupCredentials(request.ServiceAccountKey, ""); err != nil {
		sendJSONResponse(w, map[string]interface{}{"error": fmt.Sprintf("Failed to setup credentials: %v", err)})
		return
	}

	// If this is a destination operation, set up the destination credentials
	cleanup, err := utils.SetupDestinationCredentials(request.ServiceAccountKey)
	if err != nil {
		sendJSONResponse(w, map[string]interface{}{"error": fmt.Sprintf("Failed to setup destination credentials: %v", err)})
		return
	}
	defer cleanup()

	ctx := context.Background()
	client, err := firestore.NewClientWithDatabase(ctx, request.ProjectID, request.DbID)
	if err != nil {
		sendJSONResponse(w, map[string]interface{}{"error": fmt.Sprintf("Failed to create Firestore client: %v", err)})
		return
	}
	defer client.Close()

	collections := make([]map[string]interface{}, 0)
	iter := client.Collections(ctx)

	for {
		col, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			sendJSONResponse(w, map[string]interface{}{"error": fmt.Sprintf("Failed to list collections: %v", err)})
			return
		}

		collections = append(collections, map[string]interface{}{
			"id":   col.ID,
			"path": col.Path,
		})
	}

	sendJSONResponse(w, map[string]interface{}{
		"collections": collections,
	})
}

func handleGetDocument(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		ProjectID string `json:"projectId"`
		DbID      string `json:"dbId"`
		Path      string `json:"path"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		sendJSONResponse(w, map[string]interface{}{"error": "Invalid request format"})
		return
	}

	ctx := context.Background()
	client, err := firestore.NewClientWithDatabase(ctx, request.ProjectID, request.DbID)
	if err != nil {
		sendJSONResponse(w, map[string]interface{}{"error": fmt.Sprintf("Failed to create Firestore client: %v", err)})
		return
	}
	defer client.Close()

	// Split path into segments to get document reference
	pathSegments := strings.Split(request.Path, "/")
	if len(pathSegments) < 2 || len(pathSegments)%2 != 0 {
		sendJSONResponse(w, map[string]interface{}{"error": "Invalid document path"})
		return
	}

	var docRef *firestore.DocumentRef
	colRef := client.Collection(pathSegments[0])
	docRef = colRef.Doc(pathSegments[1])

	// Handle nested collections if they exist
	for i := 2; i < len(pathSegments)-1; i += 2 {
		colRef = docRef.Collection(pathSegments[i])
		docRef = colRef.Doc(pathSegments[i+1])
	}

	doc, err := docRef.Get(ctx)
	if err != nil {
		sendJSONResponse(w, map[string]interface{}{"error": fmt.Sprintf("Failed to get document: %v", err)})
		return
	}

	sendJSONResponse(w, map[string]interface{}{
		"document": doc.Data(),
	})
}

func handleListDocuments(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		log.Printf("Invalid method for list-documents: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		ProjectID  string `json:"projectId"`
		DbID       string `json:"dbId"`
		Collection string `json:"collection"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		log.Printf("Error decoding list-documents request: %v", err)
		sendJSONResponse(w, map[string]interface{}{"error": "Invalid request format"})
		return
	}

	log.Printf("Listing documents for collection: %s in project: %s, database: %s",
		request.Collection, request.ProjectID, request.DbID)

	ctx := context.Background()
	client, err := firestore.NewClientWithDatabase(ctx, request.ProjectID, request.DbID)
	if err != nil {
		log.Printf("Failed to create Firestore client: %v", err)
		sendJSONResponse(w, map[string]interface{}{"error": fmt.Sprintf("Failed to create Firestore client: %v", err)})
		return
	}
	defer client.Close()

	docs := make([]map[string]interface{}, 0)
	log.Printf("Starting document iteration for collection: %s", request.Collection)
	iter := client.Collection(request.Collection).Documents(ctx)
	docCount := 0

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("Error listing documents: %v", err)
			sendJSONResponse(w, map[string]interface{}{"error": fmt.Sprintf("Failed to list documents: %v", err)})
			return
		}

		docCount++
		log.Printf("Found document %d: ID=%s, Path=%s", docCount, doc.Ref.ID, doc.Ref.Path)
		docs = append(docs, map[string]interface{}{
			"id":   doc.Ref.ID,
			"path": doc.Ref.Path,
			"data": doc.Data(),
		})
	}

	log.Printf("Successfully listed %d documents in collection %s", docCount, request.Collection)
	sendJSONResponse(w, map[string]interface{}{
		"documents": docs,
	})
}

func handleTransferData(w http.ResponseWriter, r *http.Request) {
	var req TransferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Error decoding transfer request: %v", err)
		sendJSONResponse(w, map[string]interface{}{"error": "Invalid request"})
		return
	}

	log.Printf("Received transfer request: source=%s/%s, dest=%s/%s, paths=%v, includeSubcollections=%v, overwriteExisting=%v",
		req.SourceProjectID, req.SourceDb, req.DestProjectID, req.DestDb, req.Paths, req.IncludeSubcollections, req.OverwriteExisting)

	ctx := context.Background()
	transferred, err := utils.TransferData(ctx, req.SourceProjectID, req.SourceDb,
		req.DestProjectID, req.DestDb, req.Paths, req.IncludeSubcollections, req.OverwriteExisting)
	if err != nil {
		log.Printf("Error transferring data: %v", err)
		sendJSONResponse(w, map[string]interface{}{"error": err.Error()})
		return
	}

	log.Printf("Successfully transferred %d items from %s/%s to %s/%s",
		transferred, req.SourceProjectID, req.SourceDb, req.DestProjectID, req.DestDb)

	sendJSONResponse(w, map[string]interface{}{
		"transferred":     transferred,
		"sourceProjectId": req.SourceProjectID,
		"sourceDb":        req.SourceDb,
		"destProjectId":   req.DestProjectID,
		"destDb":          req.DestDb,
	})
}

func sendJSONResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// List backups from PITR
func listPITRCollections(ctx context.Context, projectID string, pitrTime time.Time) ([]string, bool, error) {
	log.Printf("Attempting to list PITR collections for project %s at time %v", projectID, pitrTime)

	client, err := firestore.NewClientWithDatabase(ctx, projectID, "(default)")
	if err != nil {
		return []string{}, false, fmt.Errorf("failed to create Firestore client: %v", err)
	}
	defer client.Close()

	log.Printf("Listing collections for PITR at %v", pitrTime)

	// Query the database to get a list of collections
	iter := client.Collections(ctx)
	backups := make([]string, 0)
	for {
		col, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return []string{}, false, fmt.Errorf("failed to list collections: %v", err)
		}

		// Add the collection to the list
		backupItem := fmt.Sprintf("PITR - %s (Collection: %s)", pitrTime.Format(time.RFC3339), col.ID)
		backups = append(backups, backupItem)
		log.Printf("Found collection: %s", col.ID)
	}

	if len(backups) == 0 {
		log.Printf("No collections found at time %v", pitrTime)
	} else {
		log.Printf("Found %d collections", len(backups))
	}

	return backups, false, nil
}

// List backups from GCS
func listGCSBackups(ctx context.Context, gcsPath string) ([]string, bool, error) {
	if !strings.HasPrefix(gcsPath, "gs://") {
		return nil, false, fmt.Errorf("invalid GCS path: must start with gs://")
	}

	backups, err := backup.ListBackups(ctx, gcsPath)
	if err != nil {
		return nil, false, err
	}

	// Check metadata for granular restore possibility
	granularRestorePossible := false
	if len(backups) > 0 {
		// Assume the first backup's metadata is representative
		metadataFile := backups[0] + ".overall_export_metadata"
		exists, err := utils.FileExistsInGCS(ctx, metadataFile)
		if err != nil {
			return nil, false, err
		}
		granularRestorePossible = !exists // Granular if .overall_export_metadata doesn't exist
	}

	return backups, granularRestorePossible, nil
}

func granularityToInt(granularity string) int {
	switch granularity {
	case "collection":
		return 1
	case "document":
		return 2
	default:
		return 0 // "full" or any other value
	}
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP connection to WebSocket
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow all origins for development
		},
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade connection to WebSocket: %v", err)
		return
	}
	defer conn.Close()

	// Create a custom log writer for this connection
	logWriter := NewLogWriter()
	defer logWriter.Close()

	// Set up a goroutine to send log messages to the WebSocket
	go func() {
		for msg := range logWriter.messages {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
				log.Printf("Error writing to WebSocket: %v", err)
				return
			}
		}
	}()

	// Set the log output to our custom writer
	originalLogger := log.Default()
	log.SetOutput(logWriter)
	defer log.SetOutput(originalLogger.Writer())

	// Handle WebSocket messages
	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading WebSocket message: %v", err)
			break
		}

		// Parse the request based on type
		var baseRequest struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(message, &baseRequest); err != nil {
			log.Printf("Error parsing WebSocket message: %v", err)
			if err := conn.WriteMessage(messageType, []byte(fmt.Sprintf("Error: %v", err))); err != nil {
				log.Printf("Error sending error message: %v", err)
			}
			continue
		}

		switch baseRequest.Type {
		case "restore":
			var request struct {
				Type                    string `json:"type"`
				SourceProjectID         string `json:"sourceProjectId"`
				SourceDb                string `json:"sourceDb"`
				DestProjectID           string `json:"destProjectId"`
				DestDb                  string `json:"destDb"`
				Source                  string `json:"source"`
				BackupSource            string `json:"backupSource"`
				Granularity             string `json:"granularity"`
				CollectionID            string `json:"collectionId"`
				DocumentID              string `json:"documentId"`
				UserEmail               string `json:"userEmail"`
				SourceServiceAccountKey string `json:"sourceServiceAccountKey"`
				DestServiceAccountKey   string `json:"destServiceAccountKey"`
			}
			if err := json.Unmarshal(message, &request); err != nil {
				log.Printf("Error parsing restore request: %v", err)
				continue
			}

			// Set default destination database if not specified
			if request.DestDb == "" || request.DestDb == "(default)" {
				request.DestDb = "korl-backup-db"
			}

			// Set up credentials for source operations
			if err := utils.SetupCredentials(request.SourceServiceAccountKey, request.DestServiceAccountKey); err != nil {
				log.Printf("Error setting up credentials: %v", err)
				if err := conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("Error: Failed to setup credentials: %v", err))); err != nil {
					log.Printf("Error sending error message: %v", err)
				}
				continue
			}

			ctx, cancel := context.WithCancel(context.Background())

			// Store the cancel function in the restore operation
			restoreOp := &RestoreOperation{
				ID:            uuid.New().String(),
				StartTime:     time.Now(),
				Status:        "running",
				SourceProject: request.SourceProjectID,
				DestProject:   request.DestProjectID,
				DestDB:        request.DestDb,
				LogChan:       make(chan string, 100),
				Done:          make(chan bool),
				Cancel:        cancel,
			}

			// Add to active restores
			restoreMutex.Lock()
			activeRestores[restoreOp.ID] = restoreOp
			restoreMutex.Unlock()

			// Start restore operation in a goroutine
			go func() {
				defer cancel() // Ensure context is cancelled when done
				defer close(restoreOp.Done)

				var err error
				if request.Source == "pitr" {
					pitrTime, err := time.Parse(time.RFC3339, request.BackupSource)
					if err != nil {
						errMsg := fmt.Sprintf("Invalid PITR timestamp: %v", err)
						log.Printf(errMsg)
						restoreOp.Status = "failed"
						restoreOp.Error = errMsg
						return
					}

					// Default to full backup (granularityInt = 0) if no collection is selected
					granularityInt := 0
					if request.CollectionID != "" && request.Granularity == "collection" {
						granularityInt = 1
					}

					// Set up destination credentials if provided
					cleanup, err := utils.SetupDestinationCredentials(request.DestServiceAccountKey)
					if err != nil {
						errMsg := fmt.Sprintf("Failed to setup destination credentials: %v", err)
						log.Printf(errMsg)
						restoreOp.Status = "failed"
						restoreOp.Error = errMsg
						return
					}
					defer cleanup()

					err = restore.RestoreFromPITR(ctx, request.SourceProjectID, request.DestProjectID, request.DestDb, pitrTime, granularityInt, request.CollectionID)
				} else {
					// Default to full backup (granularityInt = 0) if no collection is selected
					granularityInt := 0
					if request.CollectionID != "" && request.Granularity == "collection" {
						granularityInt = 1
					}

					// Set up destination credentials if provided
					cleanup, err := utils.SetupDestinationCredentials(request.DestServiceAccountKey)
					if err != nil {
						errMsg := fmt.Sprintf("Failed to setup destination credentials: %v", err)
						log.Printf(errMsg)
						restoreOp.Status = "failed"
						restoreOp.Error = errMsg
						return
					}
					defer cleanup()

					err = restore.RestoreFromGCS(ctx, request.SourceProjectID, request.DestProjectID, request.DestDb, request.BackupSource, granularityInt, request.CollectionID, request.DocumentID, request.UserEmail)
				}

				if err != nil {
					var errMsg string
					if restoreErr, ok := err.(*restore.RestoreError); ok {
						errMsg = fmt.Sprintf("Restore failed: %s\n%s", restoreErr.Message, restoreErr.Commands)
					} else {
						errMsg = fmt.Sprintf("Restore failed: %v", err)
					}
					log.Printf(errMsg)
					restoreOp.Status = "failed"
					restoreOp.Error = errMsg
					if err := conn.WriteMessage(websocket.TextMessage, []byte(errMsg)); err != nil {
						log.Printf("Error sending error message: %v", err)
					}
				} else {
					successMsg := "Restore completed successfully"
					log.Printf(successMsg)
					restoreOp.Status = "completed"
					if err := conn.WriteMessage(websocket.TextMessage, []byte(successMsg)); err != nil {
						log.Printf("Error sending success message: %v", err)
					}
				}
			}()

		case "transfer":
			var request TransferRequest
			if err := json.Unmarshal(message, &request); err != nil {
				log.Printf("Error parsing transfer request: %v", err)
				continue
			}

			// Create a custom log writer for this connection
			logWriter := NewLogWriter()
			defer logWriter.Close()

			// Set up a goroutine to send log messages to the WebSocket
			go func() {
				for msg := range logWriter.messages {
					if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
						log.Printf("Error writing to WebSocket: %v", err)
						return
					}
				}
			}()

			// Set the log output to our custom writer
			originalLogger := log.Default()
			log.SetOutput(logWriter)
			defer log.SetOutput(originalLogger.Writer())

			// Start transfer in a goroutine
			go func() {
				ctx := context.Background()
				transferred, err := utils.TransferData(ctx, request.SourceProjectID, request.SourceDb,
					request.DestProjectID, request.DestDb, request.Paths,
					request.IncludeSubcollections, request.OverwriteExisting)

				if err != nil {
					errMsg := fmt.Sprintf("Error: %v", err)
					log.Print(errMsg)
					if err := conn.WriteMessage(websocket.TextMessage, []byte(errMsg)); err != nil {
						log.Printf("Error sending error message: %v", err)
					}
				} else {
					// Send completion message with transfer count
					completionMsg := fmt.Sprintf("Transfer completed. Total items transferred: %d", transferred)
					log.Print(completionMsg)
					if err := conn.WriteMessage(websocket.TextMessage, []byte(completionMsg)); err != nil {
						log.Printf("Error sending completion message: %v", err)
					}
				}
			}()

		case "cancel":
			// Find the active restore operation and cancel it
			restoreMutex.RLock()
			for _, op := range activeRestores {
				if op.Status == "running" {
					if op.Cancel != nil {
						op.Cancel() // This will trigger context cancellation
						op.Status = "cancelled"
						log.Printf("Restore operation %s cancelled", op.ID)
						if err := conn.WriteMessage(messageType, []byte("Restore operation cancelled")); err != nil {
							log.Printf("Error sending cancel confirmation: %v", err)
						}
					}
					break
				}
			}
			restoreMutex.RUnlock()
		}
	}
}
