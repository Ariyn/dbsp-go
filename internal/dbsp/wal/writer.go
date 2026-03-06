package wal

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LogWriter is responsible for writing records into segmented WAL files
type LogWriter struct {
	mu          sync.Mutex
	dir         string
	segmentSize int64
	maxTotal    int64
	currentFile *os.File
	buffered    *bufio.Writer
}

func NewLogWriter(dir string, segmentSize int64, maxTotal int64) (*LogWriter, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal dir: %w", err)
	}

	lw := &LogWriter{
		dir:         dir,
		segmentSize: segmentSize,
		maxTotal:    maxTotal,
	}

	if err := lw.rotate(); err != nil {
		return nil, err
	}

	return lw, nil
}

func (lw *LogWriter) rotate() error {
	if lw.currentFile != nil {
		lw.buffered.Flush()
		lw.currentFile.Close()
	}

	if lw.maxTotal > 0 {
		lw.enforceLimit()
	}

	filename := fmt.Sprintf("%d.wal", time.Now().UnixNano())
	path := filepath.Join(lw.dir, filename)

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open wal file: %w", err)
	}

	lw.currentFile = f
	lw.buffered = bufio.NewWriterSize(f, 64*1024)
	return nil
}

func (lw *LogWriter) enforceLimit() {
	files, err := filepath.Glob(filepath.Join(lw.dir, "*.wal"))
	if err != nil || len(files) == 0 {
		return
	}

	var total int64
	for _, f := range files {
		if s, err := os.Stat(f); err == nil {
			total += s.Size()
		}
	}

	if total <= lw.maxTotal {
		return
	}

	// Simple FIFO deletion for now (needs improvements for checkpoint safety later)
	for i := 0; i < len(files)-1 && total > lw.maxTotal; i++ {
		if s, err := os.Stat(files[i]); err == nil {
			total -= s.Size()
			os.Remove(files[i])
		}
	}
}

func (lw *LogWriter) Append(record *Record) error {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	data, err := record.Encode()
	if err != nil {
		return err
	}

	// Check rotation
	info, _ := lw.currentFile.Stat()
	if info.Size()+int64(len(data)) > lw.segmentSize {
		if err := lw.rotate(); err != nil {
			return err
		}
	}

	n, err := lw.buffered.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("partial write to wal")
	}

	return lw.buffered.Flush() // Flush or Sync depending on durability requirements
}

func (lw *LogWriter) Close() error {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	if lw.currentFile != nil {
		lw.buffered.Flush()
		return lw.currentFile.Close()
	}
	return nil
}
