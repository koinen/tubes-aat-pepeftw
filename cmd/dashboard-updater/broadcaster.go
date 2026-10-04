package main

import "sync"

const mailboxSize = 64

type broadcaster struct {
	mu        sync.Mutex
	mailboxes map[chan hazardView]struct{}
}

func newBroadcaster() *broadcaster {
	return &broadcaster{mailboxes: map[chan hazardView]struct{}{}}
}

func (b *broadcaster) register() (<-chan hazardView, func()) {
	mailbox := make(chan hazardView, mailboxSize)
	b.mu.Lock()
	b.mailboxes[mailbox] = struct{}{}
	b.mu.Unlock()

	return mailbox, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if _, ok := b.mailboxes[mailbox]; ok {
			delete(b.mailboxes, mailbox)
			close(mailbox)
		}
	}
}

func (b *broadcaster) broadcast(v hazardView) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for mailbox := range b.mailboxes {
		select {
		case mailbox <- v:
		default:
		}
	}
}
