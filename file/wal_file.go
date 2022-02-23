package file

type WalFile interface {
	Write([]byte) (int, error)
	Read([]byte) (int, error)
	Flush() error
	Close() error
}
