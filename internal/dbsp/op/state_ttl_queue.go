package op

import (
	"container/heap"
	"time"
)

type ttlExpiryItem struct {
	id        string
	expiresAt time.Time
}

type ttlExpiryHeap []ttlExpiryItem

func (h ttlExpiryHeap) Len() int { return len(h) }

func (h ttlExpiryHeap) Less(i, j int) bool {
	return h[i].expiresAt.Before(h[j].expiresAt)
}

func (h ttlExpiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *ttlExpiryHeap) Push(x any) {
	*h = append(*h, x.(ttlExpiryItem))
}

func (h *ttlExpiryHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type ttlExpiryQueue struct {
	items  ttlExpiryHeap
	latest map[string]time.Time
}

func (q *ttlExpiryQueue) touch(id string, expiresAt time.Time) {
	if q.latest == nil {
		q.latest = make(map[string]time.Time)
	}
	q.latest[id] = expiresAt
	heap.Push(&q.items, ttlExpiryItem{id: id, expiresAt: expiresAt})
}

func (q *ttlExpiryQueue) remove(id string) {
	if q.latest == nil {
		return
	}
	delete(q.latest, id)
}

func (q *ttlExpiryQueue) popExpired(now time.Time, evict func(id string) error) error {
	for q.items.Len() > 0 {
		item := q.items[0]
		current, ok := q.latest[item.id]
		if !ok || !current.Equal(item.expiresAt) {
			heap.Pop(&q.items)
			continue
		}
		if now.Before(item.expiresAt) {
			return nil
		}
		heap.Pop(&q.items)
		delete(q.latest, item.id)
		if err := evict(item.id); err != nil {
			return err
		}
	}
	return nil
}
