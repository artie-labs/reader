package maputil

import (
	"sort"
)

type ItemWrapper[T any] struct {
	ts   int64
	Item T
}

type MostRecentMap[T any] struct {
	Items []ItemWrapper[T]
}

func NewMostRecentMap[T any]() *MostRecentMap[T] {
	return &MostRecentMap[T]{}
}

func (m *MostRecentMap[T]) GetItem(ts int64) (T, bool) {
	if len(m.Items) == 0 {
		var zero T
		return zero, false
	}

	idx := sort.Search(len(m.Items), func(i int) bool {
		return m.Items[i].ts > ts
	})

	if idx == 0 {
		var zero T
		return zero, false
	}

	return m.Items[idx-1].Item, true
}

func (m *MostRecentMap[T]) AddItem(ts int64, item T) {
	if len(m.Items) == 0 {
		m.Items = append(m.Items, ItemWrapper[T]{ts, item})
		return
	}

	idx := sort.Search(len(m.Items), func(i int) bool {
		return m.Items[i].ts > ts
	})

	if idx == len(m.Items) {
		m.Items = append(m.Items, ItemWrapper[T]{ts, item})
	} else {
		m.Items = append(m.Items[:idx], append([]ItemWrapper[T]{{ts, item}}, m.Items[idx:]...)...)
	}
}
