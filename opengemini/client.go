package opengemini

import (
	"crypto/tls"
	"errors"
)

var _ OpenGemini = (*Client)(nil)

type Options = func(client *Client) error

type Client struct {
	Addresses   []*Address
	AuthConfig  *AuthConfig
	BatchConfig *BatchConfig
	GzipEnable  bool
	TlsConfig   *tls.Config
}

func NewClient(addresses []*Address, options ...Options) (*Client, error) {
	client := new(Client)
	if len(addresses) == 0 {
		return nil, errors.New("must have at least one address")
	}
	client.Addresses = addresses
	for _, op := range options {
		err := op(client)
		if err != nil {
			return nil, err
		}
	}
	return client, nil
}

func WithPassword(username, password string) Options {
	return func(client *Client) error {
		client.AuthConfig = &AuthConfig{
			AuthType: Password,
			UserName: username,
			Password: password,
		}
		return nil
	}
}

func WithToken(token string) Options {
	return func(client *Client) error {
		client.AuthConfig = &AuthConfig{
			AuthType: Token,
			Token:    token,
		}
		return nil
	}
}

func WithGzipEnable(enable bool) Options {
	return func(client *Client) error {
		client.GzipEnable = enable
		return nil
	}
}

func WithBatchConfig(enable bool, size, interval int) Options {
	return func(client *Client) error {
		if enable && (size <= 0 || interval <= 0) {
			return errors.New("if batch enable , size and interval must more than 0")
		}
		client.BatchConfig = &BatchConfig{
			enable,
			interval,
			size,
		}
		return nil
	}
}

func WithTlsConfig(tls *tls.Config) Options {
	return func(client *Client) error {
		client.TlsConfig = tls
		return nil
	}
}

type Address struct {
	Host string
	Port int
}

type AuthType int

const (
	Password AuthType = iota
	Token
)

type AuthConfig struct {
	AuthType AuthType
	UserName string
	Password string
	Token    string
}

type BatchConfig struct {
	BatchEnable   bool
	BatchInterval int
	BatchSize     int
}

type OpenGemini interface {
	WriteBatchPoint(points BatchPoints)
	WritePoint(point Point)
	//WriteLineProtocol()
	//Query()
}

func (c *Client) WriteBatchPoint(points BatchPoints) {
	//TODO implement me
	panic("implement me")
}

func (c *Client) WritePoint(point Point) {
	//TODO implement me
	panic("implement me")
}
