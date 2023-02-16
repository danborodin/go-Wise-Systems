package window

import (
	"sync"
	"time"
)

type TumblingWindow struct {
	sync.Mutex
	size   time.Duration
	in     chan interface{}
	out    chan interface{}
	buffer []interface{}
}

func NewTumblingWindow(size time.Duration) *TumblingWindow {
	window := &TumblingWindow{
		size: size,
		in:   make(chan interface{}),
		out:  make(chan interface{}),
	}
	go window.receive()
	go window.emit()

	return window
}

func (tw *TumblingWindow) Out() <-chan interface{} {
	return tw.out
}

func (tw *TumblingWindow) In() chan<- interface{} {
	return tw.in
}

func (tw *TumblingWindow) Stop() {
	close(tw.in)
	close(tw.out)
}

func (tw *TumblingWindow) receive() {
	for elem := range tw.in {
		tw.Lock()
		tw.buffer = append(tw.buffer, elem)
		tw.Unlock()
	}
	close(tw.out)
}

func (tw *TumblingWindow) emit() {
	ticker := time.NewTicker(tw.size)
	defer ticker.Stop()

	for {
		<-ticker.C
		tw.Lock()
		windowSlice := tw.buffer
		tw.buffer = nil
		tw.Unlock()

		if len(windowSlice) > 0 {
			tw.out <- windowSlice
		}
	}
}
