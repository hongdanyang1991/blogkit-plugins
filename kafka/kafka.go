package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/hongdanyang1991/blogkit-plugins/common/conf"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf/agent"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf/models"
	"github.com/hongdanyang1991/blogkit-plugins/common/utils"
	"github.com/qiniu/log"
	"github.com/ryanuber/go-glob"
	"sort"
	"sync"
	"time"
)

var kafkaConf = flag.String("f", "conf/kafka.conf", "configuration file to load")
var logPath = flag.String("l", "log/kafka", "configuration file to log")

var kafka = &Kafka{}

func init() {
	flag.Parse()
	utils.RouteLog(*logPath)
	if err := conf.LoadEx(kafka, *kafkaConf); err != nil {
		log.Fatal("config.Load failed:", err)
	}
}

func main() {
	log.Info("start collect kafka metric data")
	metrics := []telegraf.Metric{}
	input := models.NewRunningInput(kafka, &models.InputConfig{})
	acc := agent.NewAccumulator(input, metrics)
	err := kafka.Gather(acc)
	if err != nil {
		log.Errorf("collect kafka metric error:", err)
	}
	datas := []map[string]interface{}{}

	/*for _, metric := range acc.Metrics {
		datas = append(datas, metric.Fields())
	}*/

	for _, metric := range acc.Metrics {
		fields := metric.Fields()
		for tagKey, tagVal := range metric.Tags() {
			fields[tagKey] = tagVal
		}
		datas = append(datas, fields)
	}

	data, err := json.Marshal(datas)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(data))
}

type Kafka struct {
	//ZkAddr		string `json:"zookeeper_address"`
	//BasePath	string `json:"base_path"`
	Brokers []string `json:"brokers"`
	Topics  []string `json:"topics""`
	Groups  []string `json:"groups""`
	Version string   `json:"version"`
	client  sarama.Client
}

func (k *Kafka) Gather(acc telegraf.Accumulator) error {
	brokers := k.Brokers
	sort.Sort(sort.StringSlice(brokers))
	kafkaVersions := KafkaVersion()
	k.client = newSaramaClient(brokers, kafkaVersions[k.Version])
	k.gatherBrokerOffsets(acc)
	k.gatherBrokerMetadata(acc)
	k.gatherConsumerOffsets(acc)
	return nil
}

// getBrokerOffsets gets all broker topic offsets and sends them to the store.
func (k *Kafka) gatherBrokerOffsets(acc telegraf.Accumulator) {
	topicMap := k.getTopics()

	requests := make(map[int32]map[int64]*sarama.OffsetRequest)
	brokers := make(map[int32]*sarama.Broker)
	for topic, partitions := range topicMap {
		if !containsString(k.Topics, topic) {
			continue
		}

		for i := 0; i < partitions; i++ {
			broker, err := k.client.Leader(topic, int32(i))
			if err != nil {
				log.Error(fmt.Sprintf("topic leader error on %s:%v: %v", topic, int32(i), err))
				return
			}

			if _, ok := requests[broker.ID()]; !ok {
				brokers[broker.ID()] = broker
				requests[broker.ID()] = make(map[int64]*sarama.OffsetRequest)
				requests[broker.ID()][sarama.OffsetOldest] = &sarama.OffsetRequest{}
				requests[broker.ID()][sarama.OffsetNewest] = &sarama.OffsetRequest{}
			}

			requests[broker.ID()][sarama.OffsetOldest].AddBlock(topic, int32(i), sarama.OffsetOldest, 1)
			requests[broker.ID()][sarama.OffsetNewest].AddBlock(topic, int32(i), sarama.OffsetNewest, 1)
		}
	}

	var wg sync.WaitGroup
	getBrokerOffsets := func(brokerID int32, position int64, request *sarama.OffsetRequest) {
		defer wg.Done()

		response, err := brokers[brokerID].GetAvailableOffsets(request)
		if err != nil {
			log.Error(fmt.Sprintf("cannot fetch offsets from broker %v: %v", brokerID, err))

			brokers[brokerID].Close()

			return
		}

		ts := time.Now().Unix() * 1000
		for topic, partitions := range response.Blocks {
			for partition, offsetResp := range partitions {
				if offsetResp.Err != sarama.ErrNoError {
					if offsetResp.Err == sarama.ErrUnknownTopicOrPartition ||
						offsetResp.Err == sarama.ErrNotLeaderForPartition {
						// If we get this, the metadata is likely off, force a refresh for this topic
						k.refreshMetadata(topic)
						log.Info(fmt.Sprintf("metadata for topic %s refreshed due to OffsetResponse error", topic))
						continue
					}

					log.Warn(fmt.Sprintf("error in OffsetResponse for %s:%v from broker %v: %s", topic, partition, brokerID, offsetResp.Err.Error()))
					continue
				}

				offset := map[string]interface{}{
					"topic":               topic,
					"partition":           partition,
					"oldest":              position == sarama.OffsetOldest,
					"offset":              offsetResp.Offsets[0],
					"timestamp":           ts,
					"topicPartitionCount": topicMap[topic],
				}
				tags := map[string]string{}
				acc.AddFields("brokerOffsets", offset, tags)
			}
		}
	}

	for brokerID, requests := range requests {
		for position, request := range requests {
			wg.Add(1)

			go getBrokerOffsets(brokerID, position, request)
		}
	}

	wg.Wait()
}

// getBrokerMetadata gets all broker topic metadata and sends them to the store.
func (k *Kafka) gatherBrokerMetadata(acc telegraf.Accumulator) {
	var broker *sarama.Broker
	brokers := k.client.Brokers()
	for _, b := range brokers {
		if ok, _ := b.Connected(); ok {
			broker = b
			break
		}
	}

	if broker == nil {
		log.Error("monitor: no connected brokers found to collect metadata")
		return
	}

	response, err := broker.GetMetadata(&sarama.MetadataRequest{})

	/*	for _, topic := range response.Topics {
			fmt.Println(topic)
		}
		for _, broker := range response.Brokers {
			fmt.Println(broker)
		}*/

	if err != nil {
		log.Error(fmt.Sprintf("monitor: cannot get metadata: %v", err))
		return
	}

	ts := time.Now().Unix() * 1000
	for _, topic := range response.Topics {
		if !containsString(k.Topics, topic.Name) {
			continue
		}
		if topic.Err != sarama.ErrNoError {
			log.Error(fmt.Sprintf("monitor: cannot get topic metadata %s: %v", topic.Name, topic.Err.Error()))
			continue
		}

		partitionCount := len(topic.Partitions)
		for _, partition := range topic.Partitions {
			if partition.Err != sarama.ErrNoError {
				log.Error(fmt.Sprintf("monitor: cannot get topic partition metadata %s %d: %v", topic.Name, partition.ID, topic.Err.Error()))
				continue
			}

			meta := map[string]interface{}{
				"topic":               topic.Name,
				"partition":           partition.ID,
				"topicPartitionCount": partitionCount,
				"leader":              partition.Leader,
				"replicas":            partition.Replicas,
				"Isr":                 partition.Isr,
				"timestamp":           ts,
			}
			tags := map[string]string{}
			acc.AddFields("brokerOffsets", meta, tags)
		}
	}
}

// getConsumerOffsets gets all the consumer offsets and send them to the store.
func (k *Kafka) gatherConsumerOffsets(acc telegraf.Accumulator) {
	topicMap := k.getTopics()
	requests := make(map[int32]map[string]*sarama.OffsetFetchRequest)
	coordinators := make(map[int32]*sarama.Broker)

	brokers := k.client.Brokers()
	for _, broker := range brokers {
		if ok, err := broker.Connected(); !ok {
			if err != nil {
				log.Error(fmt.Sprintf("monitor: failed to connect to broker broker %v: %v", broker.ID(), err))
				continue
			}

			if err := broker.Open(k.client.Config()); err != nil {
				log.Error(fmt.Sprintf("monitor: failed to connect to broker broker %v: %v", broker.ID(), err))
				continue
			}
		}

		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			log.Error(fmt.Sprintf("monitor: cannot fetch consumer groups on broker %v: %v", broker.ID(), err))
			continue
		}

		for group := range groups.Groups {
			if !containsString(k.Groups, group) {
				continue
			}

			coordinator, err := k.client.Coordinator(group)
			if err != nil {
				log.Error(fmt.Sprintf("monitor: cannot fetch co-ordinator for group %s: %v", group, err))
				continue
			}

			if _, ok := requests[coordinator.ID()]; !ok {
				coordinators[coordinator.ID()] = coordinator
				requests[coordinator.ID()] = make(map[string]*sarama.OffsetFetchRequest)
			}

			if _, ok := requests[coordinator.ID()][group]; !ok {
				requests[coordinator.ID()][group] = &sarama.OffsetFetchRequest{ConsumerGroup: group, Version: 1}
			}

			for topic, partitions := range topicMap {
				for i := 0; i < partitions; i++ {
					requests[coordinator.ID()][group].AddPartition(topic, int32(i))
				}
			}
		}
	}

	var wg sync.WaitGroup
	getConsumerOffsets := func(brokerID int32, group string, request *sarama.OffsetFetchRequest) {
		defer wg.Done()

		coordinator := coordinators[brokerID]

		offsets, err := coordinator.FetchOffset(request)
		if err != nil {
			log.Error(fmt.Sprintf("monitor: cannot get group topic offsets %v: %v", brokerID, err))

			return
		}

		ts := time.Now().Unix() * 1000
		for topic, partitions := range offsets.Blocks {
			for partition, block := range partitions {
				if block.Err != sarama.ErrNoError {
					log.Error(fmt.Sprintf("monitor: cannot get group topic offsets %v: %v", brokerID, block.Err.Error()))
					continue
				}

				if block.Offset == -1 {
					// We don't have an offset for this topic partition, ignore.
					continue
				}

				offset := map[string]interface{}{
					"group":     group,
					"topic":     topic,
					"partition": partition,
					"offset":    block.Offset,
					"timestamp": ts,
				}
				tags := map[string]string{}
				acc.AddFields("brokerOffsets", offset, tags)

			}
		}
	}

	for brokerID, groups := range requests {
		for group, request := range groups {
			wg.Add(1)

			go getConsumerOffsets(brokerID, group, request)
		}
	}

	wg.Wait()
}

// getTopics gets the topics for the Kafka cluster.
func (k *Kafka) getTopics() map[string]int {
	// If auto create topics is on, trying to fetch metadata for a missing
	// topic will recreate it. To get around this we refresh the metadata
	// before getting topics and partitions.
	k.client.RefreshMetadata()

	topics, _ := k.client.Topics()

	topicMap := make(map[string]int)
	for _, topic := range topics {
		partitions, _ := k.client.Partitions(topic)

		topicMap[topic] = len(partitions)
	}

	return topicMap
}

// containsString determines if the string matches any of the provided patterns.
func containsString(patterns []string, subject string) bool {
	for _, pattern := range patterns {
		if glob.Glob(pattern, subject) {
			return true
		}
	}

	return false
}

// refreshMetadata refreshes the broker metadata
func (k *Kafka) refreshMetadata(topics ...string) {
	if err := k.client.RefreshMetadata(topics...); err != nil {
		log.Error(fmt.Sprintf("could not refresh topic metadata: %v", err))
	}
}

func (k *Kafka) SampleConfig() string {

	return ""
}

func (k *Kafka) Description() string {

	return ""
}

func KafkaVersion() map[string]sarama.KafkaVersion {
	m := make(map[string]sarama.KafkaVersion)
	m["0.8.2.0"] = sarama.V0_8_2_0
	m["0.8.2.1"] = sarama.V0_8_2_1
	m["0.8.2.2"] = sarama.V0_8_2_2
	m["0.9.0.0"] = sarama.V0_9_0_0
	m["0.9.0.1"] = sarama.V0_9_0_1
	m["0.10.0.0"] = sarama.V0_10_0_0
	m["0.10.0.1"] = sarama.V0_10_0_1
	m["0.10.1.0"] = sarama.V0_10_1_0
	m["0.10.2.0"] = sarama.V0_10_2_0
	m["0.11.0.0"] = sarama.V0_11_0_0
	m["0.11.0.1"] = sarama.V0_11_0_1
	m["0.11.0.2"] = sarama.V0_11_0_2
	m["1.0.0.0"] = sarama.V1_0_0_0
	m["1.1.0.0"] = sarama.V1_1_0_0
	return m
}

func newSaramaClient(brokers []string, version sarama.KafkaVersion) sarama.Client {
	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Return.Errors = true
	config.Metadata.RefreshFrequency = 1 * time.Minute
	config.Metadata.Retry.Max = 10
	config.Net.MaxOpenRequests = 10
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second

	client, err := sarama.NewClient(brokers, config)

	if err != nil {
		panic("Failed to start client: " + err.Error())
	}

	return client
}
