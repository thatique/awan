package redissession

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/ory/dockertest"
	"github.com/thatique/awan/session/driver"
	"github.com/thatique/awan/session/drivertest"
)

func TestConformance(t *testing.T) {
	cleanup, addr := prepareRedisServer()
	defer cleanup()

	pool := createRedisPool(addr)
	ss := &storage{
		pool:            pool,
		serializer:      driver.GobSerializer,
		defaultExpire:   604800,  // 7 days
		idleTimeout:     604800,  // 7 days
		absoluteTimeout: 5184000, // 60 days
	}
	drivertest.RunConformanceTests(t, ss)
}

func dial(network, address string) (redis.Conn, error) {
	c, err := redis.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return c, err
}

func createRedisPool(address string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 240 * time.Second,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Dial: func() (redis.Conn, error) {
			return dial("tcp", address)
		},
	}
}

func prepareRedisServer() (func(), string) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatal(err)
	}

	resource, err := pool.Run("redis", "5.0-alpine", []string{})
	if err != nil {
		log.Fatal(err)
	}
	cleanup := func() {
		pool.Purge(resource)
	}
	setup := func() error {
		pool := createRedisPool("localhost:6379")
		conn := pool.Get()

		if err := conn.Err(); err != nil {
			return err
		}

		if err != nil {
			return err
		}
		defer conn.Close()

		data, err := conn.Do("PING")
		if err != nil || data == nil {
			return err
		}

		if data != "PONG" {
			return fmt.Errorf("Expected PONG from server, but got: %s", data)
		}
		return nil
	}

	if pool.Retry(setup); err != nil {
		cleanup()
		log.Fatal(err)
	}
	return cleanup, fmt.Sprintf("127.0.0.1:%s", resource.GetPort("6379/tcp"))
}
