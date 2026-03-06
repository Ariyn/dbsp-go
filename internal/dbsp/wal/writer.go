package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
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
	currentPath string
	currentSize int64
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
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to stat wal file: %w", err)
	}

	lw.currentFile = f
	lw.buffered = bufio.NewWriterSize(f, 64*1024)
	lw.currentPath = path
	lw.currentSize = info.Size()
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
	_, err := lw.AppendRef(record)
	return err
}

func (lw *LogWriter) AppendRef(record *Record) (*RecordRef, error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	recordSize := RecordHeaderSize + len(record.Payload)

	// Check rotation
	if lw.currentSize+int64(recordSize) > lw.segmentSize {
		if err := lw.rotate(); err != nil {
			return nil, err
		}
	}

	ref := &RecordRef{
		Path:     lw.currentPath,
		Offset:   lw.currentSize,
		Type:     record.Type,
		Sequence: record.Sequence,
		Length:   uint32(len(record.Payload)),
	}

	var header [RecordHeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(record.Payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(record.Payload))
	header[8] = record.Type
	binary.LittleEndian.PutUint64(header[9:17], record.Sequence)

	n, err := lw.buffered.Write(header[:])
	if err != nil {
		return nil, err
	}
	if n != len(header) {
		return nil, fmt.Errorf("partial write to wal")
	}
	if len(record.Payload) > 0 {
		n, err = lw.buffered.Write(record.Payload)
		if err != nil {
			return nil, err
		}
		if n != len(record.Payload) {
			return nil, fmt.Errorf("partial write to wal payload")
		}
	}
	lw.currentSize += int64(recordSize)

	if err := lw.buffered.Flush(); err != nil {
		return nil, err
	}
	return ref, nil // Flush or Sync depending on durability requirements
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
