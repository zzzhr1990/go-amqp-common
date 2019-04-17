package producer

import (
	"sync/atomic"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/zzzhr1990/go-amqp-common/config"
)

// AutoReconnectProducer Rec
// This producer can auto reconnect MQ
type AutoReconnectProducer struct {
	// OPT
	//
	shutdown   int32
	Connected  bool
	config     *config.AmqpConfig
	Queue      *amqp.Queue   // amqp queue
	Channel    *amqp.Channel //amqp channel
	connection *amqp.Connection
}

//CreateNew new instance for AutoReconnectProducer.
func CreateNew(config *config.AmqpConfig) (*AutoReconnectProducer, error) {
	serv := &AutoReconnectProducer{config: config}
	return serv, serv.connect()
}

//Close close connection
func (s *AutoReconnectProducer) Close() {
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

//SendProtobuf spd
func (s *AutoReconnectProducer) SendProtobuf(exchange string, bytes []byte) error {
	return s.PublishMessage(exchange, "application/protobuf", bytes)
}

//PublishMessage spd
func (s *AutoReconnectProducer) PublishMessage(exchange string, contentType string, bytes []byte) error {
	err := s.Channel.Publish(
		exchange,     // exchang
		s.Queue.Name, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: contentType,
			Body:        bytes,
		})
	if err != nil {
		log.Printf("sendNotifyToQueue error %v", err)
	}
	return err
}

func (s *AutoReconnectProducer) connect() error {
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
	log.Infof("Connect to AMQP: %v", s.config.ConnectString)
	return nil
}
