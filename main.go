package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Shopify/sarama"
)

// Sarma configuration options
var (
	brokers   = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "Kafka brokers to connect to, as a comma separated list")
	version   = flag.String("version", "2.1.1", "Kafka cluster version")
	group     = flag.String("group", "", "Kafka consumer group definition")
	topics    = flag.String("topics", "", "Kafka topics to be consumed, as a comma seperated list")
	verbose   = flag.Bool("verbose", false, "Verbose Sarama logging")
	certFile  = flag.String("certificate", "", "The optional certificate file for client authentication")
	keyFile   = flag.String("key", "", "The optional key file for client authentication")
	caFile    = flag.String("ca", "", "The optional certificate authority file for TLS client authentication")
	verifySsl = flag.Bool("verify", false, "Optional verify ssl certificates chain")
)

func init() {
	flag.Parse()

	if len(*brokers) == 0 {
		panic("no Kafka brokers defined, please set the -brokers flag or the KAFKA_PEERS environment variable")
	}

	if len(*group) == 0 {
		panic("no Kafka consumer group defined, please set the -group flag")
	}

	if len(*topics) == 0 {
		panic("no topics defined, please set the -topics flag")
	}
}

func main() {
	log.Println("Starting Sarama consumer")

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(*version)
	if err != nil {
		panic(err)
	}

	config := sarama.NewConfig()
	tlsConfig := createTLSConfiguration()
	if tlsConfig != nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}
	config.Version = version

	consumer := Consumer{
		ready: make(chan bool, 0),
	}

	ctx := context.Background()
	client, err := sarama.NewConsumerGroup(strings.Split(*brokers, ","), *group, config)
	if err != nil {
		panic(err)
	}

	go client.Consume(ctx, strings.Split(*topics, ","), &consumer)

	<-consumer.ready // Wait till the consumer has been set up
	log.Println("Sarama consumer up and running")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	<-sigterm

	client.Close()
}

func createTLSConfiguration() (t *tls.Config) {
	if *certFile != "" && *keyFile != "" && *caFile != "" {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := ioutil.ReadFile(*caFile)
		if err != nil {
			log.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: *verifySsl,
		}
	}
	// will be nil by default if nothing is provided
	return t
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}

	return nil
}
