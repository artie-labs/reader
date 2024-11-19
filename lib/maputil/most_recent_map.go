package maputil

import (
	"slices"
	"sort"
	"sync"
)

type ItemWrapper[T any] struct {
	ts   int64
	Item T
}

type MostRecentMap[T any] struct {
	mu    sync.Mutex
	Items []ItemWrapper[T]
}

func NewMostRecentMap[T any]() *MostRecentMap[T] {
	return &MostRecentMap[T]{}
}

func (m *MostRecentMap[T]) GetItem(ts int64) (T, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

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
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.Items) == 0 {
		m.Items = append(m.Items, ItemWrapper[T]{ts, item})
		return
	}

	idx, found := slices.BinarySearchFunc(m.Items, ItemWrapper[T]{ts: ts}, func(a, b ItemWrapper[T]) int {
		if a.ts < b.ts {
			return -1
		}
		if a.ts > b.ts {
			return 1
		}
		return 0
	})

	if !found {
		m.Items = append(m.Items, ItemWrapper[T]{ts, item})
	} else {
		m.Items = slices.Insert(m.Items, idx, ItemWrapper[T]{ts, item})
	}
}
