package format

import (
	"fmt"
	"os"
	"path/filepath"
)

// FlushPartition performs the v1 durability sequence:
// 1) fsync touched data files
// 2) optionally write temp metadata/stats
// 3) optionally fsync temp metadata/stats
// 4) optionally atomic rename temp -> final
// 5) fsync partition directory
func FlushPartition(partitionDir string, touchedFiles []*os.File, metaFileName string, metaPayload []byte) error {
	for _, f := range touchedFiles {
		if f == nil {
			continue
		}
		if err := f.Sync(); err != nil {
			return fmt.Errorf("fsync data file %s: %w", f.Name(), err)
		}
	}

	if err := os.MkdirAll(partitionDir, 0755); err != nil {
		return fmt.Errorf("creating partition dir: %w", err)
	}

	if metaFileName != "" {
		finalPath := filepath.Join(partitionDir, metaFileName)
		tmpPath := finalPath + ".tmp"

		tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("open temp metadata file: %w", err)
		}
		if _, err := tmp.Write(metaPayload); err != nil {
			_ = tmp.Close()
			return fmt.Errorf("write temp metadata file: %w", err)
		}
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			return fmt.Errorf("fsync temp metadata file: %w", err)
		}
		if err := tmp.Close(); err != nil {
			return fmt.Errorf("close temp metadata file: %w", err)
		}

		if err := os.Rename(tmpPath, finalPath); err != nil {
			return fmt.Errorf("atomic rename metadata file: %w", err)
		}
	}

	dir, err := os.Open(partitionDir)
	if err != nil {
		return fmt.Errorf("open partition dir: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("fsync partition dir: %w", err)
	}

	return nil
}
