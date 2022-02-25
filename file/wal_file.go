package file

type WalFile interface {
	Write([]byte) (int, error)
	Read([]byte) (int, error)
	Size() int64
	Flush() error
	Close() error
	Seek(offset int64, whence int) (ret int64, err error)
}
