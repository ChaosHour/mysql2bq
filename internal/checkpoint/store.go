package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// Checkpoint represents a replication checkpoint
type Checkpoint struct {
	// Position-based checkpoint
	Position *mysql.Position `json:"position,omitempty"`

	// GTID-based checkpoint
	GTIDSet string `json:"gtid_set,omitempty"`

	// Timestamp of last update
	Timestamp int64 `json:"timestamp"`
}

// Store defines the interface for checkpoint storage
type Store interface {
	Load() (*Checkpoint, error)
	Save(*Checkpoint) error
}

// FileStore implements Store using a JSON file
type FileStore struct {
	path string
	mu   sync.RWMutex
}

// NewFileStore creates a new file-based checkpoint store
func NewFileStore(path string) *FileStore {
	return &FileStore{
		path: path,
	}
}

// Load reads the checkpoint from file
func (s *FileStore) Load() (*Checkpoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			// Return nil if file doesn't exist (first run)
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read checkpoint file: %w", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
	}

	return &cp, nil
}

// Save writes the checkpoint to file
func (s *FileStore) Save(cp *Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	// Write to temporary file first, then rename for atomicity
	tempPath := s.path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint file: %w", err)
	}

	if err := os.Rename(tempPath, s.path); err != nil {
		return fmt.Errorf("failed to rename checkpoint file: %w", err)
	}

	return nil
}
