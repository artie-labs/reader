package ptr

func ToUint16(val uint16) *uint16 {
	return &val
}

func ToPtr[T any](val T) *T {
	return &val
}
