package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"context"
	"crypto/tls"
	"github.com/hongdanyang1991/blogkit-plugins/common"
	"github.com/hongdanyang1991/blogkit-plugins/common/conf"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf/agent"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf/models"
	"github.com/hongdanyang1991/blogkit-plugins/common/utils"
	"github.com/qiniu/log"
)

var zookeeperConf = flag.String("f", "conf/zookeeper.conf", "configuration file to load")
var logPath = flag.String("l", "log/zookeeper", "configuration file to log")

var zookeeper = &Zookeeper{}

func init() {
	flag.Parse()
	utils.RouteLog(*logPath)
	if err := conf.LoadEx(zookeeper, *zookeeperConf); err != nil {
		log.Fatal("config.Load failed:", err)
	}
}

func main() {
	log.Info("start collect zookeeper metric data")
	metrics := []telegraf.Metric{}
	input := models.NewRunningInput(zookeeper, &models.InputConfig{})
	acc := agent.NewAccumulator(input, metrics)
	zookeeper.Gather(acc)
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

// Zookeeper is a zookeeper plugin
type Zookeeper struct {
	Servers []string
	Timeout common.Duration

	EnableSSL          bool   `json:"enable_ssl"`
	SSLCA              string `json:"ssl_ca"`
	SSLCert            string `json:"ssl_cert"`
	SSLKey             string `json:"ssl_key"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify"`

	initialized bool
	tlsConfig   *tls.Config
}

var sampleConfig = `
  ## An array of address to gather stats about. Specify an ip or hostname
  ## with port. ie localhost:2181, 10.0.0.1:2181, etc.

  ## If no servers are specified, then localhost is used as the host.
  ## If no port is specified, 2181 is used
  servers = [":2181"]

  ## Timeout for metric collections from all servers.  Minimum timeout is "1s".
  # timeout = "5s"

  ## Optional SSL Config
  # enable_ssl = true
  # ssl_ca = "/etc/telegraf/ca.pem"
  # ssl_cert = "/etc/telegraf/cert.pem"
  # ssl_key = "/etc/telegraf/key.pem"
  ## If false, skip chain & host verification
  # insecure_skip_verify = true
`

var defaultTimeout = 5 * time.Second

// SampleConfig returns sample configuration message
func (z *Zookeeper) SampleConfig() string {
	return sampleConfig
}

// Description returns description of Zookeeper plugin
func (z *Zookeeper) Description() string {
	return `Reads 'mntr' stats from one or many zookeeper servers`
}

func (z *Zookeeper) dial(ctx context.Context, addr string) (net.Conn, error) {
	var dialer net.Dialer
	if z.EnableSSL {
		deadline, ok := ctx.Deadline()
		if ok {
			dialer.Deadline = deadline
		}
		return tls.DialWithDialer(&dialer, "tcp", addr, z.tlsConfig)
	} else {
		return dialer.DialContext(ctx, "tcp", addr)
	}
}

// Gather reads stats from all configured servers accumulates stats
func (z *Zookeeper) Gather(acc telegraf.Accumulator) error {
	ctx := context.Background()

	if !z.initialized {
		tlsConfig, err := common.GetTLSConfig(
			z.SSLCert, z.SSLKey, z.SSLCA, z.InsecureSkipVerify)
		if err != nil {
			return err
		}
		z.tlsConfig = tlsConfig
		z.initialized = true
	}

	if z.Timeout.Duration < 1*time.Second {
		z.Timeout.Duration = defaultTimeout
	}

	ctx, cancel := context.WithTimeout(ctx, z.Timeout.Duration)
	defer cancel()

	if len(z.Servers) == 0 {
		z.Servers = []string{":2181"}
	}

	for _, serverAddress := range z.Servers {
		acc.AddError(z.gatherServer(ctx, serverAddress, acc))
	}
	return nil
}

func (z *Zookeeper) gatherServer(ctx context.Context, address string, acc telegraf.Accumulator) error {
	var zookeeper_state string
	_, _, err := net.SplitHostPort(address)
	if err != nil {
		address = address + ":2181"
	}

	c, err := z.dial(ctx, address)
	if err != nil {
		return err
	}
	defer c.Close()

	// Apply deadline to connection
	deadline, ok := ctx.Deadline()
	if ok {
		c.SetDeadline(deadline)
	}

	fmt.Fprintf(c, "%s\n", "mntr")
	rdr := bufio.NewReader(c)
	scanner := bufio.NewScanner(rdr)

	service := strings.Split(address, ":")
	if len(service) != 2 {
		return fmt.Errorf("Invalid service address: %s", address)
	}

	fields := make(map[string]interface{})
	for scanner.Scan() {
		line := scanner.Text()

		re := regexp.MustCompile(`^zk_(\w+)\s+([\w\.\-]+)`)
		parts := re.FindStringSubmatch(string(line))

		if len(parts) != 3 {
			return fmt.Errorf("unexpected line in mntr response: %q", line)
		}

		measurement := strings.TrimPrefix(parts[1], "zk_")
		if measurement == "server_state" {
			zookeeper_state = parts[2]
		} else {
			sValue := string(parts[2])

			iVal, err := strconv.ParseInt(sValue, 10, 64)
			if err == nil {
				fields[measurement] = iVal
			} else {
				fields[measurement] = sValue
			}
		}
	}
	tags := map[string]string{
		"server": service[0],
		"port":   service[1],
		"state":  zookeeper_state,
	}
	acc.AddFields("zookeeper", fields, tags)

	return nil
}
