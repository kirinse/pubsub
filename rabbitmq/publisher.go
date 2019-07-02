package rabbitmq

import (
	"log"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
)

const (
	exchangeType    = "direct"
	durable         = true
	internal        = false
	noWait          = false
	autoDelete      = false
	contentType     = "application/json"
	contentEncoding = ""
	priority        = 0
	deliveryMode    = 1
	mandatory       = false
	immediate       = false
)

//Pub ...
type Pub struct {
	conn    *amqp.Connection
	url     string
	queue   string
	headers map[string]string
	retry   bool
	mu      *sync.RWMutex
}

//InitRabbitPublisher ...
func InitRabbitPublisher(url, queue string, headers map[string]string, retry bool) (*Pub, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Println("rabbit err conn", err)
		return nil, err
	}
	return &Pub{
		conn:    conn,
		url:     url,
		queue:   queue,
		headers: headers,
		retry:   retry,
		mu:      &sync.RWMutex{},
	}, nil
}

//PublishRaw ...
func (p *Pub) PublishRaw(id string, body []byte) error {
	oc, err := p.conn.Channel()
	if err != nil {
		log.Println("rabbit err channel ", err)
		if p.retry {
			p.mu.RLock()
			if p.conn != nil {
				err = p.conn.Close()
				if err != nil {
					log.Println("err closing connection ", err)
				}
			}
			p.conn, err = amqp.Dial(p.url)
			p.mu.RUnlock()
			if err != nil {
				return err
			}
			oc, err = p.conn.Channel()
			if err != nil {
				return err
			}
		}
	}

	headers := amqp.Table{}
	for k, v := range p.headers {
		headers[k] = v
	}

	err = oc.ExchangeDeclare(p.queue, exchangeType, durable, autoDelete, internal, noWait, headers)
	if err != nil {
		return err
	}
	err = oc.Publish(p.queue, p.queue, mandatory, immediate, amqp.Publishing{
		Headers:         headers,
		ContentType:     contentType,
		ContentEncoding: contentEncoding,
		Body:            body,
		DeliveryMode:    deliveryMode,
		Priority:        priority,
	},
	)
	errClose := oc.Close()
	if errClose != nil {
		log.Println("rabbit err closing channel ", errClose)
	}
	return err
}

//Publish ...
func (p *Pub) Publish(string, proto.Message) error {
	return nil
}

//Stop ...
func (p *Pub) Stop() error {
	return p.conn.Close()
}
