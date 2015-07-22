package main

import (
	"encoding/json"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/sethgrid/curse"
	"log"
	"math/rand"
)

type HashCount struct {
	Text  string
	Count int
}

const top_count = 10

func setup_curser() (curser *curse.Cursor) {
  var err error
	curser, err = curse.New()
	if err != nil {
		log.Fatal(err)
	}
	for line := 0; line < top_count; line++ {
		fmt.Printf("\n")
	}
  return curser
}

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

func score_hashtags(tag_chan chan string) {
	tag_map := make(map[string]int)
  total_count := 0
  market_tick := make(chan int)
	curser := setup_curser()
  next_tick := 100 + rand.Intn(100)
	for {
		select {
		case <- market_tick:
			{
				tops := get_top_HashCounts(tag_map)
				for range tops {
					curser.MoveUp(1).EraseCurrentLine()
				}
				for _, top_hash := range tops {
					fmt.Printf("%s", top_hash.Text)
					x, y, _ := curse.GetCursorPosition()
					curser.Move(x+50, y)
					fmt.Printf("%d\n", top_hash.Count)
				}
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
        curser.MoveUp(1).EraseCurrentLine();
        fmt.Printf("%d\n", total_count);
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
	consumer, err := nsq.NewConsumer("tweet", "bank#ephemeral", nsq.NewConfig())
	if err != nil {
		log.Fatal(err)
	}
	tag_chan := make(chan string)

	//bid_map := make(map[string]string)

	consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		var ms Status
		json.Unmarshal(m.Body, &ms)
		for _, tag := range ms.Entities.Hashtags {
			tag_chan <- tag.Text
		}
		return nil
	}))
	err = consumer.ConnectToNSQDs([]string{"localhost:4150"})
	score_hashtags(tag_chan)
}
