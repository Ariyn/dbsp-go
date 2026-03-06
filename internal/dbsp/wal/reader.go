package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// LogReader scans WAL segments and yields records for replay.
type LogReader struct {
	dir   string
	files []string
}

func NewLogReader(dir string) (*LogReader, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".wal") {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}

	// Sort by filename (timestamp) to ensure chronological replay
	sort.Strings(files)

	return &LogReader{
		dir:   dir,
		files: files,
	}, nil
}

// Replay calls fn for every valid record found in the WAL.
func (lr *LogReader) Replay(fn func(*Record) error) error {
	for _, path := range lr.files {
		if err := lr.replayFile(path, fn); err != nil {
			return fmt.Errorf("error replaying %s: %w", path, err)
		}
	}
	return nil
}

func (lr *LogReader) replayFile(path string, fn func(*Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	headerBuf := make([]byte, RecordHeaderSize)
	for {
		_, err := io.ReadFull(f, headerBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		length, _, recType, seq, err := DecodeHeader(headerBuf)
		if err != nil {
			return err
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			return err
		}

		rec := &Record{
			Type:     recType,
			Sequence: seq,
			Payload:  payload,
		}

		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}
