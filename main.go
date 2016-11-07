package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"math"
	"math/rand"
	"time"
)

var messageRate int

type Message struct {
	MessageId int    `json:"message_id"`
	Message   string `json:"message"`
}

func newMessage() *Message {
	messageId := rand.Intn(messageRate)
	message := &Message{MessageId: messageId, Message: fmt.Sprintf("Message id is %d", messageId)}
	return message
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func publishMessage(ch *amqp.Channel, q amqp.Queue, message *Message) {
	bodyBytes, err := json.Marshal(&message)
	failOnError(err, "Unable to marshal message")
	publishing := amqp.Publishing{ContentType: "application/json", Body: bodyBytes}
	ch.Publish("", q.Name, false, false, publishing)
}

func main() {
	var publish bool
	var subscribe bool

	flag.IntVar(&messageRate, "rate", 150, "rate of messages per minute")
	flag.BoolVar(&publish, "publish", false, "start in publish mode")
	flag.BoolVar(&subscribe, "subscribe", false, "start in subscribe mode")

	flag.Parse()

	if !publish && !subscribe {
		panic("start with either the --publish or --subscribe flag")
	}

	if publish && subscribe {
		panic("start with either the --publish or --subscribe flag")
	}

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	fmt.Println("connection estapblished")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		true,    // durable
		false,   // delete when usused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	ratePerSecond := time.Duration(1000/(messageRate/60.0)) * time.Millisecond
	fmt.Printf("running messages at rate %v per second\n", ratePerSecond)
	tickChan := time.Tick(ratePerSecond)
	timeOutChan := time.After(30 * time.Second)

	forever := make(chan bool)
	if publish {
		go func() {
			for {
				select {
				case <-tickChan:
					msg := newMessage()
					fmt.Println("sending message " + msg.Message)
					publishMessage(ch, q, msg)
				case <-timeOutChan:
					forever <- true
					break
				}
			}
		}()
	}
	if subscribe {
		msgs, err := ch.Consume(q.Name, "golang", true, false, false, false, nil)
		failOnError(err, "Failed to register a consumer")
		subscribeRate := int(math.Ceil(float64(messageRate) * 1000 / 6000))
		fmt.Println("subscribe rate is ", subscribeRate)
		go subscribeFn(subscribeRate, msgs, timeOutChan, forever)
	}
	<-forever
}

func subscribeFn(subscribeRate int, msgs <-chan amqp.Delivery, timeOutChan <-chan time.Time, forever chan bool) {
	messages := make([]Message, subscribeRate)
	toChan := time.After(1 * time.Second)
LOOP:
	for loopvar := 0; loopvar < subscribeRate; loopvar++ {
		select {
		case d := <-msgs:
			var msg Message
			err := json.Unmarshal(d.Body, &msg)
			failOnError(err, "Unable to marshal message")
			fmt.Println(msg)
			messages = append(messages, msg)
		case <-toChan:
			break LOOP
		case <-timeOutChan:
			forever <- true
			break LOOP
		}
	}
	fmt.Printf("Printing batch of %d messages\n", len(messages))
	fmt.Println("===================================")
	for _, msg2 := range messages {
		fmt.Println(msg2.Message)
	}
	go subscribeFn(subscribeRate, msgs, timeOutChan, forever)
}
