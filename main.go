package main

import (
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/zzzhr1990/go-amqp-common/config"
	"github.com/zzzhr1990/go-amqp-common/consumer"
	"github.com/zzzhr1990/go-amqp-common/producer"
)

func main() {
	config := &config.AmqpConfig{
		ConnectString:    "",
		Name:             "file-queue",
		Durable:          false,
		DeleteWhenUnused: false,
		Exclusive:        false,
		NoWait:           false,
		RoutingKey:       "",
	}
	producer, err := producer.CreateNew(config)
	if err != nil {
		log.Errorf("Cannot init AMQP producer: %v", err)
	} else {
		// producer.Close()
	}
	// wait 2

	consumer, err := consumer.CreateNew(config)

	if err != nil {
		log.Errorf("Cannot init AMQP producer: %v", err)
	} else {
		// consumer.Close()
	}

	log.Printf("Send Message...")
	go func() {
		for d := range consumer.Deliveries {
			ret := string(d.Body[:])
			log.Printf("Recv Message...%v", ret)
		}
	}()

	sum := 1
	for sum < 10 {
		sum++
		str := strconv.Itoa(sum)
		producer.PublishMessage("", "text", []byte(str))
		time.Sleep(time.Duration(1) * time.Second)
		if sum == 5 {
			consumer.FakeClose()
		}
	}

	log.Printf("FIN")
	//
	// config.ConnectString = ""
	// producer.Close()
	// consumer.Close()
	time.Sleep(time.Duration(10) * time.Second)
}
