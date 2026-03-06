package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const replayCursorFileName = ".replay.cursor"

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
	return lr.ReplayFrom(0, fn)
}

// ReplayFrom calls fn for every valid record with sequence > afterSeq.
func (lr *LogReader) ReplayFrom(afterSeq uint64, fn func(*Record) error) error {
	for _, path := range lr.files {
		if err := lr.replayFile(path, afterSeq, fn); err != nil {
			return fmt.Errorf("error replaying %s: %w", path, err)
		}
	}
	return nil
}

// ReplayRefsFrom calls fn for every record reference with sequence > afterSeq.
func (lr *LogReader) ReplayRefsFrom(afterSeq uint64, fn func(*RecordRef) error) error {
	for _, path := range lr.files {
		if err := lr.replayFileRefs(path, afterSeq, fn); err != nil {
			return fmt.Errorf("error replaying refs from %s: %w", path, err)
		}
	}
	return nil
}

func (lr *LogReader) replayFile(path string, afterSeq uint64, fn func(*Record) error) error {
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
		if afterSeq > 0 && rec.Sequence <= afterSeq {
			continue
		}

		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

func (lr *LogReader) replayFileRefs(path string, afterSeq uint64, fn func(*RecordRef) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	headerBuf := make([]byte, RecordHeaderSize)
	offset := int64(0)
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
		ref := &RecordRef{
			Path:     path,
			Offset:   offset,
			Type:     recType,
			Sequence: seq,
			Length:   length,
		}
		offset += int64(RecordHeaderSize) + int64(length)
		if _, err := f.Seek(int64(length), io.SeekCurrent); err != nil {
			return err
		}
		if afterSeq > 0 && seq <= afterSeq {
			continue
		}
		if err := fn(ref); err != nil {
			return err
		}
	}
	return nil
}

func LoadRecord(ref *RecordRef) (*Record, error) {
	f, err := os.Open(ref.Path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Seek(ref.Offset, io.SeekStart); err != nil {
		return nil, err
	}
	headerBuf := make([]byte, RecordHeaderSize)
	if _, err := io.ReadFull(f, headerBuf); err != nil {
		return nil, err
	}
	length, _, recType, seq, err := DecodeHeader(headerBuf)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(f, payload); err != nil {
		return nil, err
	}
	return &Record{Type: recType, Sequence: seq, Payload: payload}, nil
}

func LoadReplayCursor(dir string) (uint64, error) {
	payload, err := os.ReadFile(filepath.Join(dir, replayCursorFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	text := strings.TrimSpace(string(payload))
	if text == "" {
		return 0, nil
	}
	seq, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse replay cursor: %w", err)
	}
	return seq, nil
}

func SaveReplayCursor(dir string, seq uint64) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	target := filepath.Join(dir, replayCursorFileName)
	tmp := target + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatUint(seq, 10)), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, target)
}
