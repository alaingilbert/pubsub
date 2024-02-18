Simple PubSub implementation


```go
ps := NewPubSub[string]()

s1 := ps.Subscribe("topic")
s2 := ps.Subscribe("topic")
defer s1.Close()
defer s2.Close()

ps.Pub("topic", "message")

s1Topic, s1Msg, s1Err := s1.ReceiveTimeout(time.Second)
s2Topic, s2Msg, s2Err := s2.ReceiveTimeout(time.Second)
```