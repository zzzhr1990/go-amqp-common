package config

//AmqpConfig config MQ setting
type AmqpConfig struct {
	ConnectString    string `yaml:"connect-string"`
	Name             string
	Exchange         *ExchangeConfig
	Bind             *BindConfig
	Durable          bool
	DeleteWhenUnused bool `yaml:"delete-when-unused"`
	Exclusive        bool
	NoWait           bool   `yaml:"no-wait"`
	RoutingKey       string `yaml:"routing-key"`
}

// ExchangeConfig exchange
type ExchangeConfig struct {
	Name             string
	Durable          bool
	DeleteWhenUnused bool `yaml:"delete-when-unused"`
	Exclusive        bool
	NoWait           bool `yaml:"no-wait"`
	Internal         bool
	Kind             string
}

// ExchangeBind(destination string, key string, source string, noWait bool

// BindConfig cfg
type BindConfig struct {
	Destination string
	Key         string
	NoWait      bool `yaml:"no-wait"`
	Source      string
}
