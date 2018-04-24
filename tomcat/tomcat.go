package main

import (
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"github.com/qiniu/log"
	"net/http"
	"net/url"
	"strconv"

	"github.com/hongdanyang1991/blogkit-plugins/common"
	"github.com/hongdanyang1991/blogkit-plugins/common/conf"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf/agent"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf/models"
	"github.com/hongdanyang1991/blogkit-plugins/common/utils"
)

var tomcatConf = flag.String("f", "conf/tomcat.conf", "configuration file to load")
var logPath = flag.String("l", "log/tomcat", "configuration file to log")

var tomcat = &Tomcat{}

func init() {
	flag.Parse()
	utils.RouteLog(*logPath)
	if err := conf.LoadEx(tomcat, *tomcatConf); err != nil {
		log.Fatal("config.Load failed:", err)
	}
}

func main() {
	log.Info("start collect tomcat metric data")
	metrics := []telegraf.Metric{}
	input := models.NewRunningInput(tomcat, &models.InputConfig{})
	acc := agent.NewAccumulator(input, metrics)
	tomcat.Gather(acc)
	datas := []map[string]interface{}{}

	for _, metric := range acc.Metrics {
		datas = append(datas, metric.Fields())
	}
	data, err := json.Marshal(datas)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(data))

}

type TomcatStatus struct {
	TomcatJvm        TomcatJvm         `xml:"jvm"`
	TomcatConnectors []TomcatConnector `xml:"connector"`
}

type TomcatJvm struct {
	JvmMemory      JvmMemoryStat       `xml:"memory"`
	JvmMemoryPools []JvmMemoryPoolStat `xml:"memorypool"`
}

type JvmMemoryStat struct {
	Free  int64 `xml:"free,attr"`
	Total int64 `xml:"total,attr"`
	Max   int64 `xml:"max,attr"`
}

type JvmMemoryPoolStat struct {
	Name           string `xml:"name,attr"`
	Type           string `xml:"type,attr"`
	UsageInit      int64  `xml:"usageInit,attr"`
	UsageCommitted int64  `xml:"usageCommitted,attr"`
	UsageMax       int64  `xml:"usageMax,attr"`
	UsageUsed      int64  `xml:"usageUsed,attr"`
}

type TomcatConnector struct {
	Name        string      `xml:"name,attr"`
	ThreadInfo  ThreadInfo  `xml:"threadInfo"`
	RequestInfo RequestInfo `xml:"requestInfo"`
}

type ThreadInfo struct {
	MaxThreads         int64 `xml:"maxThreads,attr"`
	CurrentThreadCount int64 `xml:"currentThreadCount,attr"`
	CurrentThreadsBusy int64 `xml:"currentThreadsBusy,attr"`
}
type RequestInfo struct {
	MaxTime        int   `xml:"maxTime,attr"`
	ProcessingTime int   `xml:"processingTime,attr"`
	RequestCount   int   `xml:"requestCount,attr"`
	ErrorCount     int   `xml:"errorCount,attr"`
	BytesReceived  int64 `xml:"bytesReceived,attr"`
	BytesSent      int64 `xml:"bytesSent,attr"`
}

type Tomcat struct {
	URL      string `json:"url"`
	Username string `json:"user_name"`
	Password string `json:"password"`
	Timeout  common.Duration

	SSLCA              string `json:"ssl_ca"`
	SSLCert            string `json:"ssl_cert"`
	SSLKey             string `json:"ssl_key"`
	InsecureSkipVerify bool

	client  *http.Client
	request *http.Request
}

var sampleconfig = `
  ## URL of the Tomcat server status
  # url = "http://127.0.0.1:8080/manager/status/all?XML=true"

  ## HTTP Basic Auth Credentials
  # username = "tomcat"
  # password = "s3cret"

  ## Request timeout
  # timeout = "5s"

  ## Optional SSL Config
  # ssl_ca = "/etc/telegraf/ca.pem"
  # ssl_cert = "/etc/telegraf/cert.pem"
  # ssl_key = "/etc/telegraf/key.pem"
  ## Use SSL but skip chain & host verification
  # insecure_skip_verify = false
`

func (s *Tomcat) Description() string {
	return "Gather metrics from the Tomcat server status page."
}

func (s *Tomcat) SampleConfig() string {
	return sampleconfig
}

func (s *Tomcat) Gather(acc telegraf.Accumulator) error {
	if s.client == nil {
		client, err := s.createHttpClient()
		if err != nil {
			return err
		}
		s.client = client
	}

	if s.request == nil {
		_, err := url.Parse(s.URL)
		if err != nil {
			return err
		}
		request, err := http.NewRequest("GET", s.URL, nil)
		if err != nil {
			return err
		}
		request.SetBasicAuth(s.Username, s.Password)
		s.request = request
	}

	resp, err := s.client.Do(s.request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received HTTP status code %d from %q; expected 200",
			resp.StatusCode, s.URL)
	}

	var status TomcatStatus
	xml.NewDecoder(resp.Body).Decode(&status)

	// add tomcat_jvm_memory measurements
	tcm := map[string]interface{}{
		"free":  status.TomcatJvm.JvmMemory.Free,
		"total": status.TomcatJvm.JvmMemory.Total,
		"max":   status.TomcatJvm.JvmMemory.Max,
	}
	acc.AddFields("tomcat_jvm_memory", tcm, nil)

	// add tomcat_jvm_memorypool measurements
	for _, mp := range status.TomcatJvm.JvmMemoryPools {
		tcmpTags := map[string]string{
			"name": mp.Name,
			"type": mp.Type,
		}

		tcmpFields := map[string]interface{}{
			"init":      mp.UsageInit,
			"committed": mp.UsageCommitted,
			"max":       mp.UsageMax,
			"used":      mp.UsageUsed,
		}

		acc.AddFields("tomcat_jvm_memorypool", tcmpFields, tcmpTags)
	}

	// add tomcat_connector measurements
	for _, c := range status.TomcatConnectors {
		name, err := strconv.Unquote(c.Name)
		if err != nil {
			name = c.Name
		}

		tccTags := map[string]string{
			"name": name,
		}

		tccFields := map[string]interface{}{
			"max_threads":          c.ThreadInfo.MaxThreads,
			"current_thread_count": c.ThreadInfo.CurrentThreadCount,
			"current_threads_busy": c.ThreadInfo.CurrentThreadsBusy,
			"max_time":             c.RequestInfo.MaxTime,
			"processing_time":      c.RequestInfo.ProcessingTime,
			"request_count":        c.RequestInfo.RequestCount,
			"error_count":          c.RequestInfo.ErrorCount,
			"bytes_received":       c.RequestInfo.BytesReceived,
			"bytes_sent":           c.RequestInfo.BytesSent,
		}

		acc.AddFields("tomcat_connector", tccFields, tccTags)
	}

	return nil
}

func (s *Tomcat) createHttpClient() (*http.Client, error) {
	tlsConfig, err := common.GetTLSConfig(
		s.SSLCert, s.SSLKey, s.SSLCA, s.InsecureSkipVerify)
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
		Timeout: s.Timeout.Duration,
	}

	return client, nil
}
