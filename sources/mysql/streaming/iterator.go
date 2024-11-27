package streaming

import (
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
)

func BuildStreamingIterator(_ config.MySQL) (Iterator, error) {
	// TODO
	return Iterator{}, nil
}

func (i *Iterator) HasNext() bool {
	// TODO
	return true
}

func (i *Iterator) CommitOffset() {
	// TODO
}

func (i *Iterator) Close() error {
	// TODO
	return nil
}

func (i *Iterator) Next() ([]lib.RawMessage, error) {
	// TODO
	return nil, nil
}
