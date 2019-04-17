package consumer

import (
	"os"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/zzzhr1990/go-amqp-common/config"
)

// AutoReconnectConsumer Rec
// This producer can auto reconnect MQ
type AutoReconnectConsumer struct {
	// OPT
	//
	shutdown   int32
	Connected  bool
	config     *config.AmqpConfig
	Queue      *amqp.Queue   // amqp queue
	Channel    *amqp.Channel //amqp channel
	connection *amqp.Connection
	Deliveries chan amqp.Delivery // Deliveries
}

//CreateNew new instance for AutoReconnectProducer.
func CreateNew(config *config.AmqpConfig) (*AutoReconnectConsumer, error) {
	serv := &AutoReconnectConsumer{config: config}
	serv.Deliveries = make(chan amqp.Delivery)
	return serv, serv.connect()
}

//FakeClose fake..
func (s *AutoReconnectConsumer) FakeClose() {
	err := s.Channel.Close()
	if err != nil {
		log.Printf("Close channel error %v", err)
	}
	err = s.connection.Close()
	if err != nil {
		log.Printf("Close mq connection error %v", err)
	}
}

//Close close connection
func (s *AutoReconnectConsumer) Close() {
	s.Connected = false
	atomic.StoreInt32(&s.shutdown, 1)
	if s.Channel != nil {
		err := s.Channel.Close()
		if err != nil {
			log.Printf("Close channel error %v", err)
		}
	}
	if s.connection != nil {
		err := s.connection.Close()
		if err != nil {
			log.Printf("Close mq connection error %v", err)
		}
	}
}

func (s *AutoReconnectConsumer) connect() error {
	s.Connected = false
	if s.Channel != nil {
		s.Channel.Close()
	}
	if s.connection != nil {
		s.connection.Close()
	}
	mqConn, err := amqp.Dial(s.config.ConnectString)
	if err != nil {
		log.Errorf("Cannot connect to AMQP: %v %v", s.config.ConnectString, err)
		return err
	}
	s.connection = mqConn

	// init channel
	channel, err := mqConn.Channel()
	if err != nil {
		log.Errorf("Cannot init to AMQP channel: %v %v", s.config.ConnectString, err)
		return err
	}

	s.Channel = channel
	queue, err := channel.QueueDeclare(
		s.config.Name,             // name
		s.config.Durable,          // durable
		s.config.DeleteWhenUnused, // delete when unused
		s.config.Exclusive,        // exclusive
		s.config.NoWait,           // no-wait
		nil,                       // arguments
	)
	if err != nil {
		log.Errorf("Cannot init to AMQP queue: %v %v", s.config.ConnectString, err)
		return err
	}

	// this is queue..
	s.Queue = &queue
	s.Connected = true
	rabbitCloseError := make(chan *amqp.Error)
	mqConn.NotifyClose(rabbitCloseError)
	go func() {
		rabbitErr := <-rabbitCloseError
		log.Infof("Connect to rabbitMQ %v closed", s.config.ConnectString)
		if rabbitErr != nil {
			log.Infof("Connect to rabbitMQ error %v", rabbitErr)
		}
		if atomic.LoadInt32(&s.shutdown) > 0 {
			log.Info("Connection to AMQP Closed. exit")
		} else {
			log.Error("Connection to AMQP Closed, RECONNECT.")
			err := s.connect()
			for err != nil && atomic.LoadInt32(&s.shutdown) <= 0 {
				err = s.connect()
				if err != nil {
					log.Error("Connection to AMQP failed, RECONNECT.")
				}
			}
		}
	}()

	// queue...
	msgs, err := channel.Consume(
		queue.Name,         // queue
		"",                 // consumer
		true,               // auto-ack
		s.config.Exclusive, // exclusive
		false,              // no-local
		s.config.NoWait,    // no-wait
		nil,                // args
	)

	if err != nil {
		log.Fatalf("Setup Amqp Consume error: %v", err)
		os.Exit(1)
	}
	go func() {
		for d := range msgs {
			s.Deliveries <- d
		}
	}()
	return nil
}