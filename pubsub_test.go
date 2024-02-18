// MIT License
//
// Copyright (c) 2024 Alain Gilbert
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPublish(t *testing.T) {
	topic := "topic1"
	msg := "msg1"
	ps := NewPubSub[string](nil)
	s1 := ps.Subscribe(topic)
	s2 := ps.Subscribe(topic)
	s3 := ps.Subscribe(topic)
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		s1Topic, s1Msg, s1Err := s1.ReceiveTimeout(time.Second)
		assert.Nil(t, s1Err)
		assert.Equal(t, msg, s1Msg)
		assert.Equal(t, topic, s1Topic)
		wg.Done()
	}()
	go func() {
		s2Topic, s2Msg, s2Err := s2.ReceiveTimeout(time.Second)
		assert.Nil(t, s2Err)
		assert.Equal(t, msg, s2Msg)
		assert.Equal(t, topic, s2Topic)
		wg.Done()
	}()
	go func() {
		s3Topic, s3Msg, s3Err := s3.ReceiveTimeout(time.Second)
		assert.Nil(t, s3Err)
		assert.Equal(t, msg, s3Msg)
		assert.Equal(t, topic, s3Topic)
		wg.Done()
	}()
	time.Sleep(time.Millisecond)
	ps.Pub(topic, msg)
	wg.Wait()
}

func TestCustomTopicType(t *testing.T) {
	topic := 123
	msg := "msg1"
	ps := NewPubSubTopic[int, string](nil)
	s1 := ps.Subscribe(topic)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s1Topic, s1Msg, s1Err := s1.ReceiveTimeout(time.Second)
		assert.Nil(t, s1Err)
		assert.Equal(t, msg, s1Msg)
		assert.Equal(t, topic, s1Topic)
		wg.Done()
	}()
	time.Sleep(time.Millisecond)
	ps.Pub(topic, msg)
	wg.Wait()
}

func TestSubscribe_manyTopics(t *testing.T) {
	topic1 := "topic1"
	topic2 := "topic2"
	msg1 := "msg1"
	msg2 := "msg2"

	ps := NewPubSub[string](nil)

	s := ps.Subscribe(topic1, topic2)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s1Topic1, s1Msg1, s1Err1 := s.ReceiveTimeout(time.Second)
		assert.Equal(t, topic1, s1Topic1)
		assert.Equal(t, msg1, s1Msg1)
		assert.Nil(t, s1Err1)
		s1Topic2, s1Msg2, s1Err2 := s.ReceiveTimeout(time.Second)
		assert.Equal(t, topic2, s1Topic2)
		assert.Equal(t, msg2, s1Msg2)
		assert.Nil(t, s1Err2)
		wg.Done()
	}()
	time.Sleep(time.Millisecond)
	ps.Pub(topic1, msg1)
	time.Sleep(time.Millisecond)
	ps.Pub(topic2, msg2)
	wg.Wait()
}

func TestSubscribe_unbuffered(t *testing.T) {
	topic1 := "topic1"
	msg1 := "msg1"
	msg2 := "msg2"

	ps := NewPubSub[string](&Config{Buffered: 0})

	s := ps.Subscribe(topic1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s1Topic1, s1Msg1, s1Err1 := s.ReceiveTimeout(time.Second)
		assert.Equal(t, topic1, s1Topic1)
		assert.Equal(t, msg1, s1Msg1)
		assert.Nil(t, s1Err1)
		// No buffer, will miss some messages
		s1Topic2, s1Msg2, s1Err2 := s.ReceiveTimeout(time.Millisecond)
		assert.Equal(t, "", s1Topic2)
		assert.Equal(t, "", s1Msg2)
		assert.Error(t, s1Err2)
		wg.Done()
	}()
	time.Sleep(time.Millisecond)
	ps.Pub(topic1, msg1)
	ps.Pub(topic1, msg2)
	wg.Wait()
}

func TestSubscribe_timeout(t *testing.T) {
	topic1 := "topic1"
	ps := NewPubSub[string](nil)
	s := ps.Subscribe(topic1)
	_, _, s1Err1 := s.ReceiveTimeout(time.Millisecond)
	assert.Error(t, s1Err1)
}

func TestReceive(t *testing.T) {
	topic1 := "topic1"
	msg1 := "msg1"
	ps := NewPubSub[string](nil)
	s := ps.Subscribe(topic1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s1Topic1, s1Msg1, s1Err1 := s.Receive()
		assert.Equal(t, topic1, s1Topic1)
		assert.Equal(t, msg1, s1Msg1)
		assert.Nil(t, s1Err1)
		wg.Done()
	}()
	time.Sleep(time.Millisecond)
	ps.Pub(topic1, msg1)
	wg.Wait()
}

func TestReceiveCh(t *testing.T) {
	topic1 := "topic1"
	msg1 := "msg1"
	ps := NewPubSub[string](nil)
	s := ps.Subscribe(topic1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		payload := <-s.ReceiveCh()
		assert.Equal(t, topic1, payload.Topic)
		assert.Equal(t, msg1, payload.Msg)
		wg.Done()
	}()
	time.Sleep(time.Millisecond)
	ps.Pub(topic1, msg1)
	wg.Wait()
}

func TestReceiveContext(t *testing.T) {
	topic1 := "topic1"
	msg1 := "msg1"
	ps := NewPubSub[string](nil)
	s := ps.Subscribe(topic1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s1Topic1, s1Msg1, s1Err1 := s.ReceiveContext(context.Background())
		assert.Equal(t, topic1, s1Topic1)
		assert.Equal(t, msg1, s1Msg1)
		assert.Nil(t, s1Err1)

		s1 := ps.Subscribe(topic1)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _, s1Err1 = s1.ReceiveContext(ctx)
		assert.Error(t, s1Err1)

		s2 := ps.Subscribe(topic1)
		s2.Close()
		_, _, s1Err1 = s2.ReceiveContext(context.Background())
		assert.Error(t, s1Err1)
		wg.Done()
	}()
	time.Sleep(time.Millisecond)
	ps.Pub(topic1, msg1)
	wg.Wait()
}

func TestPublishMarshal(t *testing.T) {
	topic := "topic"
	type Msg struct {
		ID      int64
		Msg     string
		private string
	}
	var msg Msg
	msg.ID = 1
	msg.Msg = "will be sent"
	msg.private = "will not"

	ps := NewPubSub[Msg](nil)

	s1 := ps.Subscribe(topic)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s1Topic, s1Msg, s1Err := s1.ReceiveTimeout(time.Second)
		assert.Nil(t, s1Err)
		assert.Equal(t, int64(1), s1Msg.ID)
		assert.Equal(t, "will be sent", s1Msg.Msg)
		assert.Equal(t, "will not", s1Msg.private)
		assert.Equal(t, topic, s1Topic)
		wg.Done()
	}()
	time.Sleep(time.Millisecond)
	ps.Pub(topic, msg)
	wg.Wait()
}

func TestSub_Close(t *testing.T) {
	topic := "topic1"
	ps := NewPubSub[string](nil)
	s1 := ps.Subscribe(topic)
	s1.Close()
	_, _, s1Err := s1.ReceiveTimeout(time.Second)
	assert.Equal(t, context.Canceled, s1Err)
}

func TestRace(t *testing.T) {
	topic := "topic1"
	ps := NewPubSub[string](nil)
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			s := ps.Subscribe(topic)
			s.Close()
		}()
		go func() {
			ps.Pub(topic, "test")
			wg.Done()
		}()
	}
	wg.Wait()
	assert.Equal(t, 1, 1)
}
