package file

import (
	"os"
	"time"
)

// File is an abstraction over the os.File package.
// This enables handling multiple files that may be carrying the
// data of the KV store using a single struct.
type File struct {
	f *os.File
}

// NewFile returns a new instance of File.
func NewFile() (*File, error) {
	fileName := time.Now().String()
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &File{
		f: f,
	}, nil
}

// Append appends the given string to the end of the file.
func (f *File) Append(s string) error {
	_, err := f.f.WriteString(s)
	return err
}

// ReadAt reads the file at the given offset and for the
// specified length.
func (f *File) ReadAt(offset, length int) (string, error) {
	b := make([]byte, length)
	_, err := f.f.ReadAt(b, int64(offset))
	if err != nil {
		return "", err
	}
	return string(b), nil
}
