package utils

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const (
	maxRetries = 3
	batchSize  = 500
	retryDelay = 1 * time.Second
)

// DatabaseStructure represents the hierarchical structure of collections and documents
type DatabaseStructure map[string]interface{}

// SetupCredentials configures the Google Cloud credentials based on the provided service account key paths
func SetupCredentials(sourceKeyPath, destKeyPath string) error {
	// If no keys provided, rely on GOOGLE_APPLICATION_CREDENTIALS env var
	if sourceKeyPath == "" && destKeyPath == "" {
		return nil
	}

	// Check source key if provided
	if sourceKeyPath != "" {
		if _, err := os.Stat(sourceKeyPath); os.IsNotExist(err) {
			return fmt.Errorf("source service account key file not found: %s", sourceKeyPath)
		}
	}

	// Check destination key if provided
	if destKeyPath != "" {
		if _, err := os.Stat(destKeyPath); os.IsNotExist(err) {
			return fmt.Errorf("destination service account key file not found: %s", destKeyPath)
		}
	}

	// Set the environment variable to the source key by default
	if sourceKeyPath != "" {
		if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", sourceKeyPath); err != nil {
			return fmt.Errorf("failed to set source credentials: %v", err)
		}
	}

	return nil
}

// SetupDestinationCredentials temporarily sets the destination credentials
func SetupDestinationCredentials(destKeyPath string) (func(), error) {
	if destKeyPath == "" {
		return func() {}, nil
	}

	// Store the current credentials
	prevCreds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")

	// Set the destination credentials
	if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", destKeyPath); err != nil {
		return nil, fmt.Errorf("failed to set destination credentials: %v", err)
	}

	// Return a cleanup function that restores the previous credentials
	return func() {
		if prevCreds != "" {
			os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", prevCreds)
		}
	}, nil
}

// FileExistsInGCS checks if a file exists in Google Cloud Storage.
func FileExistsInGCS(ctx context.Context, filePath string) (bool, error) {
	// Parse the GCS path to extract bucket and object name
	parts := strings.SplitN(filePath, "/", 4)
	if len(parts) != 4 || parts[0] != "gs:" || parts[1] != "" {
		return false, fmt.Errorf("invalid GCS path: %s", filePath)
	}
	bucketName := parts[2]
	objectName := parts[3]

	// Create a GCS client
	client, err := storage.NewClient(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to create GCS client: %v", err)
	}
	defer client.Close()

	// Get a handle to the bucket
	bucket := client.Bucket(bucketName)

	// Check if the object exists
	_, err = bucket.Object(objectName).Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failed to check object existence: %v", err)
	}

	return true, nil
}

func ListDatabaseStructure(ctx context.Context, projectID, dbID string) (DatabaseStructure, error) {
	client, err := firestore.NewClientWithDatabase(ctx, projectID, dbID)
	if err != nil {
		return nil, fmt.Errorf("failed to create Firestore client: %v", err)
	}
	defer client.Close()

	structure := make(DatabaseStructure)
	collections := client.Collections(ctx)

	for {
		col, err := collections.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate collections: %v", err)
		}

		// Get collection structure
		colStruct, err := getCollectionStructure(ctx, col)
		if err != nil {
			log.Printf("Error getting structure for collection %s: %v", col.ID, err)
			continue
		}
		structure[col.ID] = colStruct
	}

	return structure, nil
}

func getCollectionStructure(ctx context.Context, col *firestore.CollectionRef) (map[string]interface{}, error) {
	structure := make(map[string]interface{})
	docs := col.Documents(ctx)

	for {
		doc, err := docs.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate documents: %v", err)
		}

		// Add document to structure
		structure[doc.Ref.ID] = nil // nil indicates it's a document

		// Get subcollections
		subCollections := doc.Ref.Collections(ctx)
		for {
			subCol, err := subCollections.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Printf("Error getting subcollections for doc %s: %v", doc.Ref.ID, err)
				continue
			}

			// Recursively get subcollection structure
			subStruct, err := getCollectionStructure(ctx, subCol)
			if err != nil {
				log.Printf("Error getting structure for subcollection %s: %v", subCol.ID, err)
				continue
			}
			structure[doc.Ref.ID+"/"+subCol.ID] = subStruct
		}
	}

	return structure, nil
}

func normalizeFirestorePath(path string) string {
	// Remove the "projects/[PROJECT]/databases/[DB]/documents/" prefix if it exists
	if parts := strings.Split(path, "/documents/"); len(parts) > 1 {
		return parts[1]
	}
	return path
}

func TransferData(ctx context.Context, sourceProjectID, sourceDb, destProjectID, destDb string, paths []string, includeSubcollections, overwriteExisting bool) (int, error) {
	log.Printf("Starting data transfer: source=%s/%s, dest=%s/%s", sourceProjectID, sourceDb, destProjectID, destDb)

	sourceClient, err := firestore.NewClientWithDatabase(ctx, sourceProjectID, sourceDb)
	if err != nil {
		log.Printf("Error creating source client: %v", err)
		return 0, fmt.Errorf("failed to create source client: %v", err)
	}
	defer sourceClient.Close()

	destClient, err := firestore.NewClientWithDatabase(ctx, destProjectID, destDb)
	if err != nil {
		log.Printf("Error creating destination client: %v", err)
		return 0, fmt.Errorf("failed to create destination client: %v", err)
	}
	defer destClient.Close()

	transferred := 0
	for _, path := range paths {
		normalizedPath := normalizeFirestorePath(path)
		log.Printf("Processing path: %s (normalized: %s)", path, normalizedPath)

		// Check if this is a collection or document path based on segments
		segments := strings.Split(normalizedPath, "/")
		isCollection := len(segments)%2 == 1 // Odd number of segments means it's a collection path

		if isCollection {
			// Try to get the collection
			log.Printf("Detected collection path: %s", normalizedPath)
			colRef := sourceClient.Collection(normalizedPath)
			// Check if collection exists by trying to get one document
			docs := colRef.Limit(1).Documents(ctx)
			_, err := docs.Next()
			if err != nil && err != iterator.Done {
				log.Printf("Error accessing collection %s: %v", normalizedPath, err)
				continue
			}

			count, err := transferCollection(ctx, sourceClient, destClient, normalizedPath, includeSubcollections, overwriteExisting)
			if err != nil {
				log.Printf("Error transferring collection %s: %v", normalizedPath, err)
				continue
			}
			log.Printf("Successfully transferred collection %s with %d items", normalizedPath, count)
			transferred += count
		} else {
			// Try to get the document
			log.Printf("Detected document path: %s", normalizedPath)
			docRef := sourceClient.Doc(normalizedPath)
			_, err := docRef.Get(ctx) // We only need to check if it exists
			if err != nil {
				log.Printf("Error accessing document %s: %v", normalizedPath, err)
				continue
			}

			count, err := transferDocument(ctx, sourceClient, destClient, normalizedPath, includeSubcollections, overwriteExisting)
			if err != nil {
				log.Printf("Error transferring document %s: %v", normalizedPath, err)
				continue
			}
			log.Printf("Successfully transferred document %s with %d items", normalizedPath, count)
			transferred += count
		}
	}

	log.Printf("Transfer completed. Total items transferred: %d", transferred)
	return transferred, nil
}

func withRetry(operation string, f func() error) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			log.Printf("Retrying %s (attempt %d/%d) after error: %v", operation, i+1, maxRetries, lastErr)
			time.Sleep(retryDelay * time.Duration(i)) // Exponential backoff
		}

		if err := f(); err != nil {
			lastErr = err
			if isTransientError(err) {
				continue
			}
			return err // Non-transient error, return immediately
		}
		return nil // Success
	}
	return fmt.Errorf("failed after %d retries: %v", maxRetries, lastErr)
}

func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "UNAVAILABLE") ||
		strings.Contains(errStr, "DEADLINE_EXCEEDED") ||
		strings.Contains(errStr, "INTERNAL") ||
		strings.Contains(errStr, "RESOURCE_EXHAUSTED")
}

func transferDocument(ctx context.Context, sourceClient, destClient *firestore.Client, path string, includeSubcollections, overwriteExisting bool) (int, error) {
	log.Printf("Getting source document: %s", path)
	docRef := sourceClient.Doc(path)

	var doc *firestore.DocumentSnapshot
	err := withRetry("get source document", func() error {
		var err error
		doc, err = docRef.Get(ctx)
		return err
	})
	if err != nil {
		log.Printf("Error getting source document %s: %v", path, err)
		return 0, fmt.Errorf("failed to get source document: %v", err)
	}

	destRef := destClient.Doc(path)

	// Check if document exists and respect overwriteExisting flag
	if !overwriteExisting {
		var destDoc *firestore.DocumentSnapshot
		err := withRetry("check destination document", func() error {
			var err error
			destDoc, err = destRef.Get(ctx)
			return err
		})
		if err == nil && destDoc.Exists() {
			log.Printf("Skipping existing document: %s", path)
			return 0, nil
		}
	}

	log.Printf("Writing document to destination: %s", path)
	err = withRetry("write document", func() error {
		_, err := destRef.Set(ctx, doc.Data())
		return err
	})
	if err != nil {
		log.Printf("Error writing document %s: %v", path, err)
		return 0, fmt.Errorf("failed to write destination document: %v", err)
	}

	transferred := 1

	// Transfer subcollections if requested
	if includeSubcollections {
		log.Printf("Checking for subcollections in document: %s", path)
		var subCollections []*firestore.CollectionRef
		err := withRetry("list subcollections", func() error {
			iter := docRef.Collections(ctx)
			subCollections = nil // Reset in case of retry
			for {
				col, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return err
				}
				subCollections = append(subCollections, col)
			}
			return nil
		})
		if err != nil {
			log.Printf("Error listing subcollections: %v", err)
			return transferred, nil
		}

		for _, subCol := range subCollections {
			log.Printf("Found subcollection: %s", subCol.ID)
			count, err := transferCollection(ctx, sourceClient, destClient, path+"/"+subCol.ID, includeSubcollections, overwriteExisting)
			if err != nil {
				log.Printf("Error transferring subcollection %s: %v", subCol.ID, err)
				continue
			}
			log.Printf("Successfully transferred subcollection %s with %d items", subCol.ID, count)
			transferred += count
		}
	}

	return transferred, nil
}

func transferCollection(ctx context.Context, sourceClient, destClient *firestore.Client, path string, includeSubcollections, overwriteExisting bool) (int, error) {
	log.Printf("Starting collection transfer: %s", path)
	colRef := sourceClient.Collection(path)

	transferred := 0
	batch := destClient.Batch()
	count := 0

	// Get all documents first to handle retries better
	var documents []*firestore.DocumentSnapshot
	err := withRetry("list documents", func() error {
		docs := colRef.Documents(ctx)
		documents = nil // Reset in case of retry
		for {
			doc, err := docs.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return fmt.Errorf("failed to iterate documents: %v", err)
			}
			documents = append(documents, doc)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	// Process documents in batches
	for i, doc := range documents {
		destRef := destClient.Collection(path).Doc(doc.Ref.ID)

		// Check if document exists and respect overwriteExisting flag
		if !overwriteExisting {
			var destDoc *firestore.DocumentSnapshot
			err := withRetry("check destination document", func() error {
				var err error
				destDoc, err = destRef.Get(ctx)
				return err
			})
			if err == nil && destDoc.Exists() {
				log.Printf("Skipping existing document: %s/%s", path, doc.Ref.ID)
				continue
			}
		}

		log.Printf("Adding document to batch: %s/%s", path, doc.Ref.ID)
		batch.Set(destRef, doc.Data())
		count++
		transferred++

		// Commit batch when it reaches the size limit or is the last document
		if count == batchSize || i == len(documents)-1 {
			if count > 0 {
				log.Printf("Committing batch of %d documents for collection %s", count, path)
				err := withRetry("commit batch", func() error {
					_, err := batch.Commit(ctx)
					return err
				})
				if err != nil {
					log.Printf("Error committing batch for %s: %v", path, err)
					return transferred - count, fmt.Errorf("failed to commit batch: %v", err)
				}
			}
			batch = destClient.Batch()
			count = 0
		}

		// Handle subcollections if requested
		if includeSubcollections {
			log.Printf("Checking for subcollections in document: %s/%s", path, doc.Ref.ID)
			var subCollections []*firestore.CollectionRef
			err := withRetry("list subcollections", func() error {
				iter := doc.Ref.Collections(ctx)
				subCollections = nil // Reset in case of retry
				for {
					col, err := iter.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return err
					}
					subCollections = append(subCollections, col)
				}
				return nil
			})
			if err != nil {
				log.Printf("Error listing subcollections: %v", err)
				continue
			}

			for _, subCol := range subCollections {
				log.Printf("Found subcollection: %s in document %s", subCol.ID, doc.Ref.ID)
				subCount, err := transferCollection(ctx, sourceClient, destClient, path+"/"+doc.Ref.ID+"/"+subCol.ID, includeSubcollections, overwriteExisting)
				if err != nil {
					log.Printf("Error transferring subcollection %s: %v", subCol.ID, err)
					continue
				}
				log.Printf("Successfully transferred subcollection %s with %d items", subCol.ID, subCount)
				transferred += subCount
			}
		}
	}

	log.Printf("Completed collection transfer: %s, transferred %d items", path, transferred)
	return transferred, nil
}
