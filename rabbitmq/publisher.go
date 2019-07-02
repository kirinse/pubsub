package rabbitmq

import (
	"log"
	"sync"

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
	conn  *amqp.Connection
	url   string
	queue string
	mu    *sync.RWMutex
}

//InitRabbitPublisher ...
func InitRabbitPublisher(url, queue string) (*Pub, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Println("rabbit err conn", err)
		return nil, err
	}
	return &Pub{
		conn:  conn,
		url:   url,
		queue: queue,
		mu:    &sync.RWMutex{},
	}, nil
}

//PublishRaw ...
func (p *Pub) PublishRaw(body []byte, headersMap map[string]string, retry bool) error {
	oc, err := p.conn.Channel()
	if err != nil {
		log.Println("rabbit err channel ", err)
		if retry {
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
	for k, v := range headersMap {
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
