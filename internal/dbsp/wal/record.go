package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// WAL Record format (Little Endian):
// [4 bytes] Length (N)
// [4 bytes] CRC32 of Payload
// [1 byte]  Type (0=Data, 1=Checkpoint)
// [8 bytes] Sequence / ID
// [N bytes] Payload (JSON/MsgPack/Protobuf)

const (
	RecordHeaderSize = 4 + 4 + 1 + 8
	RecordTypeData   = 0x00
	RecordTypeBatch  = 0x02
)

// Record represents a single entry in the WAL
type Record struct {
	Type     uint8
	Sequence uint64
	Payload  []byte
}

// RecordRef points at a persisted record inside a WAL segment.
type RecordRef struct {
	Path     string
	Offset   int64
	Type     uint8
	Sequence uint64
	Length   uint32
}

// Encode serializes a Record into a byte slice
func (r *Record) Encode() ([]byte, error) {
	n := len(r.Payload)
	buf := make([]byte, RecordHeaderSize+n)

	binary.LittleEndian.PutUint32(buf[0:4], uint32(n))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(r.Payload))
	buf[8] = r.Type
	binary.LittleEndian.PutUint64(buf[9:17], r.Sequence)
	copy(buf[17:], r.Payload)

	return buf, nil
}

// DecodeHeader minimal check for corruption or partial writes
func DecodeHeader(header []byte) (length uint32, crc uint32, recType uint8, seq uint64, err error) {
	if len(header) < RecordHeaderSize {
		return 0, 0, 0, 0, fmt.Errorf("header too short")
	}
	length = binary.LittleEndian.Uint32(header[0:4])
	crc = binary.LittleEndian.Uint32(header[4:8])
	recType = header[8]
	seq = binary.LittleEndian.Uint64(header[9:17])
	return
}
