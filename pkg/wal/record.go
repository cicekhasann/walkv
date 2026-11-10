package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

var (
	ErrCorruptRecord = errors.New("walkv: record crc32 checksum mismatch")
	ErrKeyTooLarge   = errors.New("walkv: key length exceeds uint16 max (65535 bytes)")
	ErrValTooLarge   = errors.New("walkv: value length exceeds uint32 max")
)

type OpType byte

const (
	OpPut    OpType = 0x01
	OpDelete OpType = 0x02
)

const (
	// Record Header Layout:
	// +-------------------+-----------------+-------------------+-------------------+
	// |  CRC32 (4 Bytes)  | OpType (1 Byte) | KeyLen (2 Bytes)  | ValLen (4 Bytes)  |
	// +-------------------+-----------------+-------------------+-------------------+
	// Total Header Size = 11 Bytes
	headerSize = 11
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type Record struct {
	Op    OpType
	Key   []byte
	Value []byte
}

// EncodeRecord serializes a Record into a contiguous byte buffer with a CRC32 checksum.
func EncodeRecord(op OpType, key, value []byte) ([]byte, error) {
	if len(key) > 0xFFFF {
		return nil, ErrKeyTooLarge
	}
	if len(value) > 0xFFFFFFFF {
		return nil, ErrValTooLarge
	}

	totalLen := headerSize + len(key) + len(value)
	buf := make([]byte, totalLen)

	buf[4] = byte(op)
	binary.BigEndian.PutUint16(buf[5:7], uint16(len(key)))
	binary.BigEndian.PutUint32(buf[7:11], uint32(len(value)))

	copy(buf[11:11+len(key)], key)
	copy(buf[11+len(key):], value)

	// Calculate CRC32 starting from OpType to end of value
	checksum := crc32.Checksum(buf[4:], crcTable)
	binary.BigEndian.PutUint32(buf[0:4], checksum)

	return buf, nil
}

// DecodeRecord reads and decodes the next Record from an io.Reader.
// Returns io.EOF cleanly when end of stream is reached.
// Returns ErrCorruptRecord if CRC32 check fails or partial record is encountered.
func DecodeRecord(r io.Reader) (*Record, error) {
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(r, header); err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, ErrCorruptRecord
		}
		return nil, err
	}

	expectedCRC := binary.BigEndian.Uint32(header[0:4])
	op := OpType(header[4])
	keyLen := int(binary.BigEndian.Uint16(header[5:7]))
	valLen := int(binary.BigEndian.Uint32(header[7:11]))

	payload := make([]byte, keyLen+valLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, ErrCorruptRecord
		}
		return nil, err
	}

	// Compute CRC of header payload + key/val
	h := crc32.New(crcTable)
	h.Write(header[4:])
	h.Write(payload)
	actualCRC := h.Sum32()

	if expectedCRC != actualCRC {
		return nil, ErrCorruptRecord
	}

	key := payload[:keyLen]
	value := payload[keyLen:]

	return &Record{
		Op:    op,
		Key:   key,
		Value: value,
	}, nil
}
