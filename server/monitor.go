/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
 *          Sendu Bala <sb10@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

package server

import (
	"container/heap"
	"sync"
	"time"

	"github.com/wtsi-hgi/ibackup/set"
)

// MonitorSet is a set with its next discovery time.
type MonitoredSet struct {
	set  *set.Set
	next time.Time
}

// monitorHeap is a time sorted heap of sets.
type monitorHeap struct {
	slice []MonitoredSet
	byID  map[string]int
}

// MonitorCallback receives a set when it is time for it to be discovered.
type MonitorCallback func(*set.Set)

// Monitor represents a time sort heap of sets.
type Monitor struct {
	mu                sync.Mutex
	monitorHeap       monitorHeap
	monitoringStarted bool
	monitorCh         chan struct{}
	callback          MonitorCallback
}

// NewMonitor returns a Monitor which will call your callback every time a set
// you add to this monitor needs to be discovered.
func NewMonitor(fn MonitorCallback) *Monitor {
	m := &Monitor{
		monitorHeap: monitorHeap{
			byID: make(map[string]int),
		},
		monitorCh: make(chan struct{}, 1),
		callback:  fn,
	}

	heap.Init(&m.monitorHeap)

	return m
}

func (m *monitorHeap) Len() int {
	return len(m.slice)
}

func (m *monitorHeap) Less(i, j int) bool {
	return m.slice[i].next.Before(m.slice[j].next)
}

func (m *monitorHeap) Swap(i, j int) {
	m.slice[i], m.slice[j] = m.slice[j], m.slice[i]

	m.byID[m.slice[i].set.ID()] = i
	m.byID[m.slice[j].set.ID()] = j
}

func (m *monitorHeap) Push(x any) {
	s := x.(MonitoredSet) //nolint:errcheck,forcetypeassert
	m.slice = append(m.slice, s)
	m.byID[s.set.ID()] = len(m.slice) - 1
}

func (m *monitorHeap) Pop() any {
	last := m.slice[len(m.slice)-1]
	m.slice = m.slice[:len(m.slice)-1]
	delete(m.byID, last.set.ID())

	return last
}

// Add pushes a set to the Monitor Heap.
func (m *Monitor) Add(s *set.Set) {
	m.mu.Lock()
	defer m.mu.Unlock()

	last := s.LastDiscovery
	if s.LastCompleted.After(last) {
		last = s.LastCompleted
	}

	ms := MonitoredSet{
		set:  s,
		next: last.Add(s.MonitorTime),
	}

	currentIndex, found := m.monitorHeap.byID[s.ID()]
	if found {
		m.monitorHeap.slice[currentIndex] = ms
		heap.Fix(&m.monitorHeap, currentIndex)
	} else {
		heap.Push(&m.monitorHeap, ms)
	}

	nextDiscovery := m.monitorHeap.nextDiscovery()

	if !m.monitoringStarted {
		m.monitoringStarted = true

		go m.monitorSets(nextDiscovery)
	} else {
		m.monitorCh <- struct{}{}
	}
}

func (m *monitorHeap) nextDiscovery() time.Time {
	if len(m.slice) == 0 {
		return time.Time{}
	}

	return m.slice[0].next
}

// NextDiscovery retrieves the discovery time of the next set in the heap.
//
// Returns an empty time.Time if the heap is empty.
func (m *Monitor) NextDiscovery() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.monitorHeap.nextDiscovery()
}

func (m *monitorHeap) nextSet() *set.Set {
	if len(m.slice) == 0 {
		return nil
	}

	return heap.Pop(m).(MonitoredSet).set //nolint:forcetypeassert
}

// NextSet returns the next set in the heap.
//
// If the heap is empty, it returns nil.
func (m *Monitor) NextSet() *set.Set {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.monitorHeap.nextSet()
}

// monitorSet sets up up discovery monitoring on the passed set if set Monitor
// duration is defined.
func (s *Server) monitorSet(given *set.Set) {
	if given.MonitorTime == 0 || given.Status != set.Complete {
		return
	}

	s.Logger.Printf("will monitor.Add")
	s.monitor.Add(given)
}

// monitorSets is called in a goroutine by monitorSet and should not be called
// separately.
func (m *Monitor) monitorSets(nextDiscovery time.Time) {
	timer := time.NewTimer(time.Until(nextDiscovery))

	for {
		select {
		case <-m.monitorCh:
			nextDiscovery = m.NextDiscovery()
			timer.Reset(time.Until(nextDiscovery))
		case <-timer.C:
			m.mu.Lock()

			given := m.monitorHeap.nextSet()
			if given == nil {
				m.monitoringStarted = false
				m.mu.Unlock()

				return
			}

			m.callback(given)

			nextDiscovery = m.monitorHeap.nextDiscovery()
			m.mu.Unlock()
			timer.Reset(time.Until(nextDiscovery))
		}
	}
}
