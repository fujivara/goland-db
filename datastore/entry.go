package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"strconv"
)

type Entry struct {
	key       string
	valueType byte
	value     string
}

type typeOperator interface {
	Encode(*Entry) []byte
	Decode([]byte, *Entry)
	Read(*bufio.Reader) (string, error)
}

type stringOperator struct{}

func encodeKey(e *Entry, vl int) ([]byte, int) {
	kl := len(e.key)
	size := kl + TYPE_SIZE + vl + 12
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	return res, kl + 8
}

func (s stringOperator) Encode(e *Entry) []byte {
	res, offset := encodeKey(e, len(e.value))
	vl := len(e.value)
	res[offset] = STRING_TYPE
	binary.LittleEndian.PutUint32(res[offset+TYPE_SIZE:], uint32(vl))
	copy(res[offset+TYPE_SIZE+4:], e.value)
	return res
}

func (s stringOperator) Decode(input []byte, e *Entry) {
	kl := len(e.key)
	vl := binary.LittleEndian.Uint32(input[kl+TYPE_SIZE+8:])
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+TYPE_SIZE+12:kl+TYPE_SIZE+12+int(vl)])
	e.value = string(valBuf)
}

func (s stringOperator) Read(in *bufio.Reader) (string, error) {
	header, err := in.Peek(4)
	if err != nil {
		return "", err
	}
	valSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", err
	}

	data := make([]byte, valSize)
	n, err := in.Read(data)
	if err != nil {
		return "", err
	}
	if n != valSize {
		return "", fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}

	return string(data), nil
}

type int64Operator struct{}

func (s int64Operator) Encode(e *Entry) []byte {
	res, offset := encodeKey(e, 8)
	i, err := strconv.ParseInt(e.value, 10, 64)
	if err != nil {
		panic(err)
	}
	res[offset] = INT64_TYPE
	binary.LittleEndian.PutUint64(res[offset+TYPE_SIZE:], uint64(i))
	return res
}

func (s int64Operator) Decode(input []byte, e *Entry) {
	kl := len(e.key)
	value := binary.LittleEndian.Uint64(input[kl+TYPE_SIZE+8 : kl+TYPE_SIZE+16])
	e.value = fmt.Sprintf("%d", int64(value))
}

func (s int64Operator) Read(in *bufio.Reader) (string, error) {
	data, err := in.Peek(8)
	if err != nil {
		return "", err
	}
	value := binary.LittleEndian.Uint64(data)
	return fmt.Sprintf("%d", int64(value)), nil
}

var typeToByte map[string]byte = map[string]byte{
	"string": STRING_TYPE,
	"int64":  INT64_TYPE,
}

func ToByte(valueType string) byte {
	return typeToByte[valueType]
}

func ToType(value byte) string {
	for k, v := range typeToByte {
		if v == value {
			return k
		}
	}
	return ""
}

var operators map[byte]typeOperator = map[byte]typeOperator{
	STRING_TYPE: stringOperator{},
	INT64_TYPE:  int64Operator{},
}

const (
	TYPE_SIZE        = 1
	STRING_TYPE byte = 0
	INT64_TYPE  byte = 1
)

func (e *Entry) Encode() []byte {
	operator := operators[e.valueType]
	return operator.Encode(e)
}

func (e *Entry) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	e.key = string(keyBuf)

	typeValue := input[kl+8]
	operator := operators[typeValue]

	operator.Decode(input, e)
}

type output struct {
	valueType string
	value     string
}

func readValue(in *bufio.Reader) (output, error) {
	header, err := in.Peek(8)
	if err != nil {
		return output{}, err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 8)
	if err != nil {
		return output{}, err
	}

	valueType, err := in.Peek(1)
	if err != nil {
		return output{}, err
	}
	_, err = in.Discard(1)
	if err != nil {
		return output{}, err
	}

	operator := operators[valueType[0]]
	data, err := operator.Read(in)
	if err != nil {
		return output{}, err
	}
	return output{ToType(valueType[0]), data}, nil
}
