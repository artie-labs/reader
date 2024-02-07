package sources

type BatchRowIterator interface {
	HasNext() bool
	Next() ([]map[string]interface{}, error)
}
