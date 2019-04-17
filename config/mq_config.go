package config

//AmqpConfig config MQ setting
type AmqpConfig struct {
	ConnectString    string `yaml:"connect-string"`
	Name             string
	Durable          bool
	DeleteWhenUnused bool `yaml:"delete-when-unused"`
	Exclusive        bool
	NoWait           bool   `yaml:"no-wait"`
	RoutingKey       string `yaml:"routing-key"`
}
