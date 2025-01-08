package backup

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// ListBackups lists the available backups in a given GCS path
func ListBackups(ctx context.Context, gcsPath string) ([]string, error) {
	// Extract bucket and prefix from the GCS path
	parts := strings.SplitN(gcsPath, "/", 4)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid GCS path: %s", gcsPath)
	}
	bucketName := parts[2]
	prefix := ""
	if len(parts) > 3 {
		prefix = parts[3]
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %v", err)
	}
	defer client.Close()

	it := client.Bucket(bucketName).Objects(ctx, &storage.Query{
		Prefix:    prefix,
		Delimiter: "/", // Treat as directory listing
	})

	var backups []string
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in bucket %s: %v", bucketName, err)
		}

		if attrs.Prefix != "" {
			// Extract the backup name from the prefix
			backupName := strings.TrimSuffix(attrs.Prefix, "/")
			backups = append(backups, "gs://"+bucketName+"/"+backupName)
		}
	}

	return backups, nil
}
