package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/hongdanyang1991/blogkit-plugins/common"
	"github.com/hongdanyang1991/blogkit-plugins/common/conf"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf/agent"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf/models"
	jsonparser "github.com/hongdanyang1991/blogkit-plugins/common/telegraf/plugins/parsers/json"
	"github.com/hongdanyang1991/blogkit-plugins/common/utils"
	"github.com/qiniu/log"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

var elasticConf = flag.String("f", "conf/elasticsearch.conf", "configuration file to load")
var logPath = flag.String("l", "log/elastic", "configuration file to log")

var elastic = &Elasticsearch{}

func init() {
	flag.Parse()
	utils.RouteLog(*logPath)
	if err := conf.LoadEx(elastic, *elasticConf); err != nil {
		log.Fatal("config.Load failed:", err)
	}
}

func main() {
	flag.Parse()
	log.Info("start collect elasticsearch metric data")
	metrics := []telegraf.Metric{}
	input := models.NewRunningInput(elastic, &models.InputConfig{})
	acc := agent.NewAccumulator(input, metrics)
	err := elastic.Gather(acc)
	if err != nil {
		log.Errorf("collect elasticsearch metric error:", err)
	}
	datas := []map[string]interface{}{}

	//log.Println(acc.Metrics)
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

// mask for masking username/password from error messages
var mask = regexp.MustCompile(`https?:\/\/\S+:\S+@`)

// Nodestats are always generated, so simply define a constant for these endpoints
const statsPath = "/_nodes/stats"
const statsPathLocal = "/_nodes/_local/stats"

type nodeStat struct {
	Host       string            `json:"host"`
	Name       string            `json:"name"`
	Attributes map[string]string `json:"attributes"`
	Indices    interface{}       `json:"indices"`
	OS         interface{}       `json:"os"`
	Process    interface{}       `json:"process"`
	JVM        interface{}       `json:"jvm"`
	ThreadPool interface{}       `json:"thread_pool"`
	FS         interface{}       `json:"fs"`
	Transport  interface{}       `json:"transport"`
	HTTP       interface{}       `json:"http"`
	Breakers   interface{}       `json:"breakers"`
}

type clusterHealth struct {
	ClusterName         string                 `json:"cluster_name"`
	Status              string                 `json:"status"`
	TimedOut            bool                   `json:"timed_out"`
	NumberOfNodes       int                    `json:"number_of_nodes"`
	NumberOfDataNodes   int                    `json:"number_of_data_nodes"`
	ActivePrimaryShards int                    `json:"active_primary_shards"`
	ActiveShards        int                    `json:"active_shards"`
	RelocatingShards    int                    `json:"relocating_shards"`
	InitializingShards  int                    `json:"initializing_shards"`
	UnassignedShards    int                    `json:"unassigned_shards"`
	Indices             map[string]indexHealth `json:"indices"`
}

type indexHealth struct {
	Status              string `json:"status"`
	NumberOfShards      int    `json:"number_of_shards"`
	NumberOfReplicas    int    `json:"number_of_replicas"`
	ActivePrimaryShards int    `json:"active_primary_shards"`
	ActiveShards        int    `json:"active_shards"`
	RelocatingShards    int    `json:"relocating_shards"`
	InitializingShards  int    `json:"initializing_shards"`
	UnassignedShards    int    `json:"unassigned_shards"`
}

type clusterStats struct {
	NodeName    string      `json:"node_name"`
	ClusterName string      `json:"cluster_name"`
	Status      string      `json:"status"`
	Indices     interface{} `json:"indices"`
	Nodes       interface{} `json:"nodes"`
}

type catMaster struct {
	NodeID   string `json:"id"`
	NodeIP   string `json:"ip"`
	NodeName string `json:"node"`
}

const sampleConfig = `
  ## specify a list of one or more Elasticsearch servers
  # you can add username and password to your url to use basic authentication:
  # servers = ["http://user:pass@localhost:9200"]
  servers = ["http://localhost:9200"]

  ## Timeout for HTTP requests to the elastic search server(s)
  http_timeout = "5s"

  ## When local is true (the default), the node will read only its own stats.
  ## Set local to false when you want to read the node stats from all nodes
  ## of the cluster.
  local = true

  ## Set cluster_health to true when you want to also obtain cluster health stats
  cluster_health = false

  ## Adjust cluster_health_level when you want to also obtain detailed health stats
  ## The options are
  ##  - indices (default)
  ##  - cluster
  # cluster_health_level = "indices"

  ## Set cluster_stats to true when you want to also obtain cluster stats from the
  ## Master node.
  cluster_stats = false

  ## node_stats is a list of sub-stats that you want to have gathered. Valid options
  ## are "indices", "os", "process", "jvm", "thread_pool", "fs", "transport", "http",
  ## "breaker". Per default, all stats are gathered.
  # node_stats = ["jvm", "http"]

  ## Optional SSL Config
  # ssl_ca = "/etc/telegraf/ca.pem"
  # ssl_cert = "/etc/telegraf/cert.pem"
  # ssl_key = "/etc/telegraf/key.pem"
  ## Use SSL but skip chain & host verification
  # insecure_skip_verify = false
`

// Elasticsearch is a plugin to read stats from one or many Elasticsearch
// servers.
type Elasticsearch struct {
	Local                   bool
	Servers                 []string
	HttpTimeout             common.Duration
	ClusterHealth           bool
	ClusterHealthLevel      string
	ClusterStats            bool
	NodeStats               []string
	SSLCA                   string `json:"ssl_ca"`   // Path to CA file
	SSLCert                 string `json:"ssl_cert"` // Path to host cert file
	SSLKey                  string `json:"ssl_key"`  // Path to cert key file
	InsecureSkipVerify      bool   // Use SSL but skip chain & host verification
	client                  *http.Client
	catMasterResponseTokens []string
	isMaster                bool
}

// NewElasticsearch return a new instance of Elasticsearch
func NewElasticsearch() *Elasticsearch {
	return &Elasticsearch{
		HttpTimeout:        common.Duration{Duration: time.Second * 5},
		ClusterHealthLevel: "indices",
	}
}

// perform status mapping
func mapHealthStatusToCode(s string) int {
	switch strings.ToLower(s) {
	case "green":
		return 1
	case "yellow":
		return 2
	case "red":
		return 3
	}
	return 0
}

// SampleConfig returns sample configuration for this plugin.
func (e *Elasticsearch) SampleConfig() string {
	return sampleConfig
}

// Description returns the plugin description.
func (e *Elasticsearch) Description() string {
	return "Read stats from one or more Elasticsearch servers or clusters"
}

// Gather reads the stats from Elasticsearch and writes it to the
// Accumulator.
func (e *Elasticsearch) Gather(acc telegraf.Accumulator) error {
	if e.client == nil {
		client, err := e.createHttpClient()

		if err != nil {
			return err
		}
		e.client = client
	}

	var wg sync.WaitGroup
	wg.Add(len(e.Servers))

	for _, serv := range e.Servers {
		go func(s string, acc telegraf.Accumulator) {
			defer wg.Done()
			url := e.nodeStatsUrl(s)
			e.isMaster = false

			if e.ClusterStats {
				// get cat/master information here so NodeStats can determine
				// whether this node is the Master
				if err := e.setCatMaster(s + "/_cat/master"); err != nil {
					acc.AddError(fmt.Errorf(mask.ReplaceAllString(err.Error(), "http(s)://XXX:XXX@")))
					return
				}
			}

			// Always gather node states
			if err := e.gatherNodeStats(url, acc); err != nil {
				acc.AddError(fmt.Errorf(mask.ReplaceAllString(err.Error(), "http(s)://XXX:XXX@")))
				return
			}

			if e.ClusterHealth {
				url = s + "/_cluster/health"
				if e.ClusterHealthLevel != "" {
					url = url + "?level=" + e.ClusterHealthLevel
				}
				if err := e.gatherClusterHealth(url, acc); err != nil {
					acc.AddError(fmt.Errorf(mask.ReplaceAllString(err.Error(), "http(s)://XXX:XXX@")))
					return
				}
			}

			if e.ClusterStats && e.isMaster {
				if err := e.gatherClusterStats(s+"/_cluster/stats", acc); err != nil {
					acc.AddError(fmt.Errorf(mask.ReplaceAllString(err.Error(), "http(s)://XXX:XXX@")))
					return
				}
			}
		}(serv, acc)
	}

	wg.Wait()
	return nil
}

func (e *Elasticsearch) createHttpClient() (*http.Client, error) {
	tlsCfg, err := common.GetTLSConfig(e.SSLCert, e.SSLKey, e.SSLCA, e.InsecureSkipVerify)
	if err != nil {
		return nil, err
	}
	tr := &http.Transport{
		ResponseHeaderTimeout: e.HttpTimeout.Duration,
		TLSClientConfig:       tlsCfg,
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   e.HttpTimeout.Duration,
	}

	return client, nil
}

func (e *Elasticsearch) nodeStatsUrl(baseUrl string) string {
	var url string

	if e.Local {
		url = baseUrl + statsPathLocal
	} else {
		url = baseUrl + statsPath
	}

	if len(e.NodeStats) == 0 {
		return url
	}

	return fmt.Sprintf("%s/%s", url, strings.Join(e.NodeStats, ","))
}

func (e *Elasticsearch) gatherNodeStats(url string, acc telegraf.Accumulator) error {
	nodeStats := &struct {
		ClusterName string               `json:"cluster_name"`
		Nodes       map[string]*nodeStat `json:"nodes"`
	}{}
	if err := e.gatherJsonData(url, nodeStats); err != nil {
		return err
	}

	for id, n := range nodeStats.Nodes {
		tags := map[string]string{
			"node_id":      id,
			"node_host":    n.Host,
			"node_name":    n.Name,
			"cluster_name": nodeStats.ClusterName,
		}

		if e.ClusterStats {
			// check for master
			e.isMaster = (id == e.catMasterResponseTokens[0])
		}

		for k, v := range n.Attributes {
			tags["node_attribute_"+k] = v
		}

		stats := map[string]interface{}{
			"indices":     n.Indices,
			"os":          n.OS,
			"process":     n.Process,
			"jvm":         n.JVM,
			"thread_pool": n.ThreadPool,
			"fs":          n.FS,
			"transport":   n.Transport,
			"http":        n.HTTP,
			"breakers":    n.Breakers,
		}

		now := time.Now()
		for p, s := range stats {
			// if one of the individual node stats is not even in the
			// original result
			if s == nil {
				continue
			}
			f := jsonparser.JSONFlattener{}
			// parse Json, ignoring strings and bools
			err := f.FlattenJSON("", s)
			if err != nil {
				return err
			}
			acc.AddFields("elasticsearch_"+p, f.Fields, tags, now)
		}
	}
	return nil
}

func (e *Elasticsearch) gatherClusterHealth(url string, acc telegraf.Accumulator) error {
	healthStats := &clusterHealth{}
	if err := e.gatherJsonData(url, healthStats); err != nil {
		return err
	}
	measurementTime := time.Now()
	clusterFields := map[string]interface{}{
		"status":                healthStats.Status,
		"status_code":           mapHealthStatusToCode(healthStats.Status),
		"timed_out":             healthStats.TimedOut,
		"number_of_nodes":       healthStats.NumberOfNodes,
		"number_of_data_nodes":  healthStats.NumberOfDataNodes,
		"active_primary_shards": healthStats.ActivePrimaryShards,
		"active_shards":         healthStats.ActiveShards,
		"relocating_shards":     healthStats.RelocatingShards,
		"initializing_shards":   healthStats.InitializingShards,
		"unassigned_shards":     healthStats.UnassignedShards,
	}
	acc.AddFields(
		"elasticsearch_cluster_health",
		clusterFields,
		map[string]string{"name": healthStats.ClusterName},
		measurementTime,
	)

	for name, health := range healthStats.Indices {
		indexFields := map[string]interface{}{
			"status":                health.Status,
			"status_code":           mapHealthStatusToCode(health.Status),
			"number_of_shards":      health.NumberOfShards,
			"number_of_replicas":    health.NumberOfReplicas,
			"active_primary_shards": health.ActivePrimaryShards,
			"active_shards":         health.ActiveShards,
			"relocating_shards":     health.RelocatingShards,
			"initializing_shards":   health.InitializingShards,
			"unassigned_shards":     health.UnassignedShards,
		}
		acc.AddFields(
			"elasticsearch_indices",
			indexFields,
			map[string]string{"index": name},
			measurementTime,
		)
	}
	return nil
}

func (e *Elasticsearch) gatherClusterStats(url string, acc telegraf.Accumulator) error {
	clusterStats := &clusterStats{}
	if err := e.gatherJsonData(url, clusterStats); err != nil {
		return err
	}
	now := time.Now()
	tags := map[string]string{
		"node_name":    clusterStats.NodeName,
		"cluster_name": clusterStats.ClusterName,
		"status":       clusterStats.Status,
	}

	stats := map[string]interface{}{
		"nodes":   clusterStats.Nodes,
		"indices": clusterStats.Indices,
	}

	for p, s := range stats {
		f := jsonparser.JSONFlattener{}
		// parse json, including bools and strings
		err := f.FullFlattenJSON("", s, true, true)
		if err != nil {
			return err
		}
		acc.AddFields("elasticsearch_clusterstats_"+p, f.Fields, tags, now)
	}

	return nil
}

func (e *Elasticsearch) setCatMaster(url string) error {
	r, err := e.client.Get(url)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		// NOTE: we are not going to read/discard r.Body under the assumption we'd prefer
		// to let the underlying transport close the connection and re-establish a new one for
		// future calls.
		return fmt.Errorf("elasticsearch: Unable to retrieve master node information. API responded with status-code %d, expected %d", r.StatusCode, http.StatusOK)
	}
	response, err := ioutil.ReadAll(r.Body)

	if err != nil {
		return err
	}

	e.catMasterResponseTokens = strings.Split(string(response), " ")

	return nil
}

func (e *Elasticsearch) gatherJsonData(url string, v interface{}) error {
	r, err := e.client.Get(url)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		// NOTE: we are not going to read/discard r.Body under the assumption we'd prefer
		// to let the underlying transport close the connection and re-establish a new one for
		// future calls.
		return fmt.Errorf("elasticsearch: API responded with status-code %d, expected %d",
			r.StatusCode, http.StatusOK)
	}

	if err = json.NewDecoder(r.Body).Decode(v); err != nil {
		return err
	}

	return nil
}
