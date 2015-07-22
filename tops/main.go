package main

import (
	"encoding/json"
	"github.com/bitly/go-nsq"
	"log"
	"math/rand"
)

type HashCount struct {
	Text  string
	Count int
}

const top_count = 10

func get_top_HashCounts(tag_map map[string]int) (tops [top_count]HashCount) {
	//var tops [top_count]HashCount
	for tag := range tag_map {
		tag_item := HashCount{tag, tag_map[tag]}
		for idx, top := range tops {
			if tag_item.Count > top.Count {
				tag_item, tops[idx] = tops[idx], tag_item
			}
		}
	}
	return tops
}

func score_hashtags(tag_chan chan string, top_chan chan []byte) {
	tag_map := make(map[string]int)
  total_count := 0
  market_tick := make(chan int)
  next_tick := 100 + rand.Intn(100)
	for {
		select {
		case <- market_tick:
			{
				tops, err := json.Marshal(get_top_HashCounts(tag_map))
	      if err != nil {
		      log.Fatal(err)
	      }
        top_chan <- tops
        tag_map = make(map[string]int)
			}
		case m := <-tag_chan:
			{
				tag_map[m] = tag_map[m] + 1
        total_count++
        if total_count > next_tick {
          total_count = 0;
          go func() { market_tick <- next_tick; }()
        }
			}
		}
	}
}
func main() {
	type Status struct {
		Entities struct {
			Hashtags []struct {
				Text string
			}
		}
	}

	top_chan := make(chan []byte)

  producer, err := nsq.NewProducer("localhost:4150", nsq.NewConfig())
  if err != nil {
    log.Fatalf("failed to create nsq.Producer - %s", err)
  }

  go func() {
    for top := range top_chan {
      err := producer.Publish("top", top)
      if err != nil {
        log.Fatalf("Failed to publish top tags - %s", err);
      }
    }
  }()

	consumer, err := nsq.NewConsumer("tweet", "bank#ephemeral", nsq.NewConfig())
	if err != nil {
		log.Fatal(err)
	}

	tag_chan := make(chan string)

	consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		var ms Status
		json.Unmarshal(m.Body, &ms)
		for _, tag := range ms.Entities.Hashtags {
			tag_chan <- tag.Text
		}
		return nil
	}))

	err = consumer.ConnectToNSQDs([]string{"localhost:4150"})
	score_hashtags(tag_chan, top_chan)
}
