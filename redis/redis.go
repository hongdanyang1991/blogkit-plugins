package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"encoding/json"

	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf"
	"flag"
	"github.com/qiniu/log"
	"github.com/hongdanyang1991/blogkit-plugins/common/conf"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf/models"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf/agent"
	"github.com/hongdanyang1991/blogkit-plugins/common/utils"
)

//var redisConf = flag.String("f", "plugins/redis/conf/redis.conf", "configuration file to load")
//var logPath = "plugins/redis/log/redis_"

var redisConf = flag.String("f", "redis.conf", "configuration file to load")
var logPath = flag.String("l", "redis", "configuration file to log")
var redis = &Redis{}

const defaultPort = "6379"

func init() {
	flag.Parse()
	utils.RouteLog(*logPath)
	if err := conf.LoadEx(redis, *redisConf); err != nil {
		log.Fatal("config.Load failed:", err)
	}
}

func main() {
	log.Info("start collect redis metric data")
	metrics := []telegraf.Metric{}
	input := models.NewRunningInput(redis, &models.InputConfig{})
	acc := agent.NewAccumulator(input, metrics)
	redis.Gather(acc)
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

type Redis struct {
	Servers []string `json:"servers"`
}

var sampleConfig = `
  ## specify servers via a url matching:
  ##  [protocol://][:password]@address[:port]
  ##  e.g.
  ##    tcp://localhost:6379
  ##    tcp://:password@192.168.99.100
  ##    unix:///var/run/redis.sock
  ##
  ## If no servers are specified, then localhost is used as the host.
  ## If no port is specified, 6379 is used
  servers = ["tcp://localhost:6379"]
`

var defaultTimeout = 5 * time.Second

func (r *Redis) SampleConfig() string {
	return sampleConfig
}

func (r *Redis) Description() string {
	return "Read metrics from one or many redis servers"
}

var Tracking = map[string]string{
	"uptime_in_seconds": "uptime",
	"connected_clients": "clients",
	"role":              "replication_role",
}

var ErrProtocolError = errors.New("redis protocol error")



// Reads stats from all configured servers accumulates stats.
// Returns one of the errors encountered while gather stats (if any).
func (r *Redis) Gather(acc telegraf.Accumulator) error {
	if len(r.Servers) == 0 {
		url := &url.URL{
			Scheme: "tcp",
			Host:   defaultPort,
		}
		r.gatherServer(url, acc)
		return nil
	}

	var wg sync.WaitGroup
	for _, serv := range r.Servers {
		if !strings.HasPrefix(serv, "tcp://") && !strings.HasPrefix(serv, "unix://") {
			serv = "tcp://" + serv
		}

		u, err := url.Parse(serv)
		if err != nil {
			acc.AddError(fmt.Errorf("Unable to parse to address '%s': %s", serv, err))
			continue
		} else if u.Scheme == "" {
			// fallback to simple string based address (i.e. "10.0.0.1:10000")
			u.Scheme = "tcp"
			u.Host = serv
			u.Path = ""
		}
		if u.Scheme == "tcp" {
			_, _, err := net.SplitHostPort(u.Host)
			if err != nil {
				u.Host = u.Host + ":" + defaultPort
			}
		}

		wg.Add(1)
		go func(serv string) {
			defer wg.Done()
			acc.AddError(r.gatherServer(u, acc))
		}(serv)
	}

	wg.Wait()
	return nil
}

func (r *Redis) gatherServer(addr *url.URL, acc telegraf.Accumulator) error {
	var address string

	if addr.Scheme == "unix" {
		address = addr.Path
	} else {
		address = addr.Host
	}
	c, err := net.DialTimeout(addr.Scheme, address, defaultTimeout)
	if err != nil {
		return fmt.Errorf("Unable to connect to redis server '%s': %s", address, err)
	}
	defer c.Close()

	// Extend connection
	c.SetDeadline(time.Now().Add(defaultTimeout))

	if addr.User != nil {
		pwd, set := addr.User.Password()
		if set && pwd != "" {
			c.Write([]byte(fmt.Sprintf("AUTH %s\r\n", pwd)))

			rdr := bufio.NewReader(c)

			line, err := rdr.ReadString('\n')
			if err != nil {
				return err
			}
			if line[0] != '+' {
				return fmt.Errorf("%s", strings.TrimSpace(line)[1:])
			}
		}
	}

	c.Write([]byte("INFO\r\n"))
	c.Write([]byte("EOF\r\n"))
	rdr := bufio.NewReader(c)

	var tags map[string]string

	if addr.Scheme == "unix" {
		tags = map[string]string{"socket": addr.Path}
	} else {
		// Setup tags for all redis metrics
		host, port := "unknown", "unknown"
		// If there's an error, ignore and use 'unknown' tags
		host, port, _ = net.SplitHostPort(addr.Host)
		tags = map[string]string{"server": host, "port": port}
	}
	return gatherInfoOutput(rdr, acc, tags)
}

// gatherInfoOutput gathers
func gatherInfoOutput(
	rdr *bufio.Reader,
	acc telegraf.Accumulator,
	tags map[string]string,
) error {
	var section string
	var keyspace_hits, keyspace_misses int64

	scanner := bufio.NewScanner(rdr)
	fields := make(map[string]interface{})
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "ERR") {
			break
		}

		if len(line) == 0 {
			continue
		}
		if line[0] == '#' {
			if len(line) > 2 {
				section = line[2:]
			}
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) < 2 {
			continue
		}
		name := string(parts[0])

		if section == "Server" {
			if name != "lru_clock" && name != "uptime_in_seconds" && name != "redis_version" {
				continue
			}
		}

		if name == "mem_allocator" {
			continue
		}

		if strings.HasSuffix(name, "_human") {
			continue
		}

		metric, ok := Tracking[name]
		if !ok {
			if section == "Keyspace" {
				kline := strings.TrimSpace(string(parts[1]))
				gatherKeyspaceLine(name, kline, acc, tags)
				continue
			}
			metric = name
		}

		val := strings.TrimSpace(parts[1])

		// Try parsing as int
		if ival, err := strconv.ParseInt(val, 10, 64); err == nil {
			switch name {
			case "keyspace_hits":
				keyspace_hits = ival
			case "keyspace_misses":
				keyspace_misses = ival
			case "rdb_last_save_time":
				// influxdb can't calculate this, so we have to do it
				fields["rdb_last_save_time_elapsed"] = time.Now().Unix() - ival
			}
			fields[metric] = ival
			continue
		}

		// Try parsing as a float
		if fval, err := strconv.ParseFloat(val, 64); err == nil {
			fields[metric] = fval
			continue
		}

		// Treat it as a string

		if name == "role" {
			tags["replication_role"] = val
			continue
		}

		fields[metric] = val
	}
	var keyspace_hitrate float64 = 0.0
	if keyspace_hits != 0 || keyspace_misses != 0 {
		keyspace_hitrate = float64(keyspace_hits) / float64(keyspace_hits+keyspace_misses)
	}
	fields["keyspace_hitrate"] = keyspace_hitrate
	acc.AddFields("redis", fields, tags)
	return nil
}

// Parse the special Keyspace line at end of redis stats
// This is a special line that looks something like:
//     db0:keys=2,expires=0,avg_ttl=0
// And there is one for each db on the redis instance
func gatherKeyspaceLine(
	name string,
	line string,
	acc telegraf.Accumulator,
	global_tags map[string]string,
) {
	if strings.Contains(line, "keys=") {
		fields := make(map[string]interface{})
		tags := make(map[string]string)
		for k, v := range global_tags {
			tags[k] = v
		}
		tags["database"] = name
		dbparts := strings.Split(line, ",")
		for _, dbp := range dbparts {
			kv := strings.Split(dbp, "=")
			ival, err := strconv.ParseInt(kv[1], 10, 64)
			if err == nil {
				fields[kv[0]] = ival
			}
		}
		acc.AddFields("redis_keyspace", fields, tags)
	}
}
