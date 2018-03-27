package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/qiniu/log"
	"strconv"
	"strings"
	"sync"
	"time"
	"github.com/json-iterator/go"


	"github.com/hongdanyang1991/blogkit-plugins/common"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf"

	"encoding/json"
	"flag"
	"github.com/go-sql-driver/mysql"
	"github.com/hongdanyang1991/blogkit-plugins/common/conf"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf/agent"
	"github.com/hongdanyang1991/blogkit-plugins/common/telegraf/models"
	"io/ioutil"
	"github.com/hongdanyang1991/blogkit-plugins/common/utils"
)

//var mysqlConf = flag.String("f", "plugins/mysql/conf/mysql.conf", "configuration file to load")
//var logPath = "plugins/mysql/log/mysql_"

var mysqlConf = flag.String("f", "mysql.conf", "configuration file to load")
var logPath = flag.String("l", "mysql", "configuration file to log")
var m = &Mysql{}


func init() {
	flag.Parse()
	utils.RouteLog(*logPath)
	if err := conf.LoadEx(m, *mysqlConf); err != nil {
		log.Fatal("config.Load failed:", err)
	}
}

func main() {
	currentTime := time.Now()
	timeStr := currentTime.Format("2006-01-02 15:04:05.999999")
	if m.LastGatherStatementsEndTime == "" {
		m.LastGatherStatementsEndTime = timeStr
	}
	if m.LastGatherStagesEndTime == "" {
		m.LastGatherStagesEndTime = timeStr
	}
	log.Info("start collect mysql metric data")
	metrics := []telegraf.Metric{}
	input := models.NewRunningInput(m, &models.InputConfig{})
	acc := agent.NewAccumulator(input, metrics)
	m.Gather(acc)
	datas := []map[string]interface{}{}

	//log.Println(acc.Metrics)
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

type Mysql struct {
	Servers                             []string `json:"servers"`
	PerfEventsStatementsDigestTextLimit int64    `json:"perf_events_statements_digest_text_limit"`
	PerfEventsStatementsLimit           int64    `json:"perf_events_statements_limit"`
	PerfEventsStatementsTimeLimit       int64    `json:"perf_events_statements_time_limit"`
	TableSchemaDatabases                []string `json:"table_schema_databases"`
	GatherGlobalStatus					bool      `json:"gather_global_status"`
	GatherProcessList                   bool     `json:"gather_process_list"`
	GatherUserStatistics                bool     `json:"gather_user_statistics"`
	GatherInfoSchemaAutoInc             bool     `json:"gather_info_schema_auto_inc"`
	GatherInnoDBMetrics                 bool     `json:"gather_innodb_metrics"`
	GatherSlaveStatus                   bool     `json:"gather_slave_status"`
	GatherBinaryLogs                    bool     `json:"gather_binary_logs"`
	GatherTableIOWaits                  bool     `json:"gather_table_io_waits"`
	GatherTableLockWaits                bool     `json:"gather_table_lock_waits"`
	GatherIndexIOWaits                  bool     `json:"gather_index_io_waits"`
	GatherEventWaits                    bool     `json:"gather_event_waits"`
	GatherTableSchema                   bool     `json:"gather_table_schema"`
	GatherFileEventsStats               bool     `json:"gather_file_events_stats"`
	GatherPerfEventsStatements          bool     `json:"gather_perf_events_statements"`

	IntervalSlow                        string   `json:"interval_slow"`
	SSLCA                               string   `json:"ssl_ca"`
	SSLCert                             string   `json:"ssl_cert"`
	SSLKey                              string   `json:"ssl_key"`


	Version								string	 `json:"version"`
	EventCollectionEnable				bool	 `json:"event_collection_enable"`
	GatherEventsStatements			    bool     `json:"gather_events_statements"`
	GatherEventsStages					bool     `json:"gather_events_stages"`

	LastGatherStatementsEndTime		string	 `json:"last_gather_statements_end_time"`
	LastGatherStagesEndTime			string	 `json:"last_gather_stages_end_time"`
}

var sampleConfig = `
  ## specify servers via a url matching:
  ##  [username[:password]@][protocol[(address)]]/[?tls=[true|false|skip-verify|custom]]
  ##  see https://github.com/go-sql-driver/mysql#dsn-data-source-name
  ##  e.g.
  ##    servers = ["user:passwd@tcp(127.0.0.1:3306)/?tls=false"]
  ##    servers = ["user@tcp(127.0.0.1:3306)/?tls=false"]
  #
  ## If no servers are specified, then localhost is used as the host.
  servers = ["tcp(127.0.0.1:3306)/"]
  ## the limits for metrics form perf_events_statements
  perf_events_statements_digest_text_limit  = 120
  perf_events_statements_limit              = 250
  perf_events_statements_time_limit         = 86400
  #
  ## if the list is empty, then metrics are gathered from all databasee tables
  table_schema_databases                    = []
  #
  ## gather metrics from INFORMATION_SCHEMA.TABLES for databases provided above list
  gather_table_schema                       = false
  #
  ## gather thread state counts from INFORMATION_SCHEMA.PROCESSLIST
  gather_process_list                       = true
  #
  ## gather thread state counts from INFORMATION_SCHEMA.USER_STATISTICS
  gather_user_statistics                    = true
  #
  ## gather auto_increment columns and max values from information schema
  gather_info_schema_auto_inc               = true
  #
  ## gather metrics from INFORMATION_SCHEMA.INNODB_METRICS
  gather_innodb_metrics                     = true
  #
  ## gather metrics from SHOW SLAVE STATUS command output
  gather_slave_status                       = true
  #
  ## gather metrics from SHOW BINARY LOGS command output
  gather_binary_logs                        = false
  #
  ## gather metrics from PERFORMANCE_SCHEMA.TABLE_IO_WAITS_SUMMARY_BY_TABLE
  gather_table_io_waits                     = false
  #
  ## gather metrics from PERFORMANCE_SCHEMA.TABLE_LOCK_WAITS
  gather_table_lock_waits                   = false
  #
  ## gather metrics from PERFORMANCE_SCHEMA.TABLE_IO_WAITS_SUMMARY_BY_INDEX_USAGE
  gather_index_io_waits                     = false
  #
  ## gather metrics from PERFORMANCE_SCHEMA.EVENT_WAITS
  gather_event_waits                        = false
  #
  ## gather metrics from PERFORMANCE_SCHEMA.FILE_SUMMARY_BY_EVENT_NAME
  gather_file_events_stats                  = false
  #
  ## gather metrics from PERFORMANCE_SCHEMA.EVENTS_STATEMENTS_SUMMARY_BY_DIGEST
  gather_perf_events_statements             = false
  #
  ## Some queries we may want to run less often (such as SHOW GLOBAL VARIABLES)
  interval_slow                   = "30m"

  ## Optional SSL Config (will be used if tls=custom parameter specified in server uri)
  ssl_ca = "/etc/telegraf/ca.pem"
  ssl_cert = "/etc/telegraf/cert.pem"
  ssl_key = "/etc/telegraf/key.pem"
`

var defaultTimeout = time.Second * time.Duration(5)

func (m *Mysql) SampleConfig() string {
	return sampleConfig
}

func (m *Mysql) Description() string {
	return "Read metrics from one or many mysql servers"
}

var (
	localhost        = ""
	lastT            time.Time
	initDone         = false
	scanIntervalSlow uint32
	measurement      = "measurement"
)

func (m *Mysql) InitMysql() {
	if len(m.IntervalSlow) > 0 {
		interval, err := time.ParseDuration(m.IntervalSlow)
		if err == nil && interval.Seconds() >= 1.0 {
			scanIntervalSlow = uint32(interval.Seconds())
		}
	}
	initDone = true
}

func (m *Mysql) Gather(acc telegraf.Accumulator) error {
	if len(m.Servers) == 0 {
		// default to localhost if nothing specified.
		return m.gatherServer(localhost, acc)
	}
	// Initialise additional query intervals
	if !initDone {
		m.InitMysql()
	}

	tlsConfig, err := common.GetTLSConfig(m.SSLCert, m.SSLKey, m.SSLCA, false)
	if err != nil {
		log.Printf("E! MySQL Error registering TLS config: %s", err)
	}

	if tlsConfig != nil {
		mysql.RegisterTLSConfig("custom", tlsConfig)
	}

	var wg sync.WaitGroup

	// Loop through each server and collect metrics
	for _, server := range m.Servers {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			acc.AddError(m.gatherServer(s, acc))
		}(server)
	}

	wg.Wait()
	return nil
}

type mapping struct {
	onServer string
	inExport string
}

var mappings = []*mapping{
	{
		onServer: "Aborted_",
		inExport: "aborted_",
	},
	{
		onServer: "Bytes_",
		inExport: "bytes_",
	},
	{
		onServer: "Com_",
		inExport: "commands_",
	},
	{
		onServer: "Created_",
		inExport: "created_",
	},
	{
		onServer: "Handler_",
		inExport: "handler_",
	},
	{
		onServer: "Innodb_",
		inExport: "innodb_",
	},
	{
		onServer: "Key_",
		inExport: "key_",
	},
	{
		onServer: "Open_",
		inExport: "open_",
	},
	{
		onServer: "Opened_",
		inExport: "opened_",
	},
	{
		onServer: "Qcache_",
		inExport: "qcache_",
	},
	{
		onServer: "Table_",
		inExport: "table_",
	},
	{
		onServer: "Tokudb_",
		inExport: "tokudb_",
	},
	{
		onServer: "Threads_",
		inExport: "threads_",
	},
	{
		onServer: "Access_",
		inExport: "access_",
	},
	{
		onServer: "Aria__",
		inExport: "aria_",
	},
	{
		onServer: "Binlog__",
		inExport: "binlog_",
	},
	{
		onServer: "Busy_",
		inExport: "busy_",
	},
	{
		onServer: "Connection_",
		inExport: "connection_",
	},
	{
		onServer: "Delayed_",
		inExport: "delayed_",
	},
	{
		onServer: "Empty_",
		inExport: "empty_",
	},
	{
		onServer: "Executed_",
		inExport: "executed_",
	},
	{
		onServer: "Executed_",
		inExport: "executed_",
	},
	{
		onServer: "Feature_",
		inExport: "feature_",
	},
	{
		onServer: "Flush_",
		inExport: "flush_",
	},
	{
		onServer: "Last_",
		inExport: "last_",
	},
	{
		onServer: "Master_",
		inExport: "master_",
	},
	{
		onServer: "Max_",
		inExport: "max_",
	},
	{
		onServer: "Memory_",
		inExport: "memory_",
	},
	{
		onServer: "Not_",
		inExport: "not_",
	},
	{
		onServer: "Performance_",
		inExport: "performance_",
	},
	{
		onServer: "Prepared_",
		inExport: "prepared_",
	},
	{
		onServer: "Rows_",
		inExport: "rows_",
	},
	{
		onServer: "Rpl_",
		inExport: "rpl_",
	},
	{
		onServer: "Select_",
		inExport: "select_",
	},
	{
		onServer: "Slave_",
		inExport: "slave_",
	},
	{
		onServer: "Slow_",
		inExport: "slow_",
	},
	{
		onServer: "Sort_",
		inExport: "sort_",
	},
	{
		onServer: "Subquery_",
		inExport: "subquery_",
	},
	{
		onServer: "Tc_",
		inExport: "tc_",
	},
	{
		onServer: "Threadpool_",
		inExport: "threadpool_",
	},
	{
		onServer: "wsrep_",
		inExport: "wsrep_",
	},
	{
		onServer: "Uptime_",
		inExport: "uptime_",
	},
}

var (
	// status counter
	generalThreadStates = map[string]uint32{
		"after create":              uint32(0),
		"altering table":            uint32(0),
		"analyzing":                 uint32(0),
		"checking permissions":      uint32(0),
		"checking table":            uint32(0),
		"cleaning up":               uint32(0),
		"closing tables":            uint32(0),
		"converting heap to myisam": uint32(0),
		"copying to tmp table":      uint32(0),
		"creating sort index":       uint32(0),
		"creating table":            uint32(0),
		"creating tmp table":        uint32(0),
		"deleting":                  uint32(0),
		"executing":                 uint32(0),
		"execution of init_command": uint32(0),
		"end":                     uint32(0),
		"freeing items":           uint32(0),
		"flushing tables":         uint32(0),
		"fulltext initialization": uint32(0),
		"idle":                      uint32(0),
		"init":                      uint32(0),
		"killed":                    uint32(0),
		"waiting for lock":          uint32(0),
		"logging slow query":        uint32(0),
		"login":                     uint32(0),
		"manage keys":               uint32(0),
		"opening tables":            uint32(0),
		"optimizing":                uint32(0),
		"preparing":                 uint32(0),
		"reading from net":          uint32(0),
		"removing duplicates":       uint32(0),
		"removing tmp table":        uint32(0),
		"reopen tables":             uint32(0),
		"repair by sorting":         uint32(0),
		"repair done":               uint32(0),
		"repair with keycache":      uint32(0),
		"replication master":        uint32(0),
		"rolling back":              uint32(0),
		"searching rows for update": uint32(0),
		"sending data":              uint32(0),
		"sorting for group":         uint32(0),
		"sorting for order":         uint32(0),
		"sorting index":             uint32(0),
		"sorting result":            uint32(0),
		"statistics":                uint32(0),
		"updating":                  uint32(0),
		"waiting for tables":        uint32(0),
		"waiting for table flush":   uint32(0),
		"waiting on cond":           uint32(0),
		"writing to net":            uint32(0),
		"other":                     uint32(0),
	}
	// plaintext statuses
	stateStatusMappings = map[string]string{
		"user sleep":                               "idle",
		"creating index":                           "altering table",
		"committing alter table to storage engine": "altering table",
		"discard or import tablespace":             "altering table",
		"rename":                                   "altering table",
		"setup":                                    "altering table",
		"renaming result table":                    "altering table",
		"preparing for alter table":                "altering table",
		"copying to group table":                   "copying to tmp table",
		"copy to tmp table":                        "copying to tmp table",
		"query end":                                "end",
		"update":                                   "updating",
		"updating main table":                      "updating",
		"updating reference tables":                "updating",
		"system lock":                              "waiting for lock",
		"user lock":                                "waiting for lock",
		"table lock":                               "waiting for lock",
		"deleting from main table":                 "deleting",
		"deleting from reference tables":           "deleting",
	}
)

// Math constants
const (
	picoSeconds = 1e12
)

// metric queries
const (
	globalStatusQuery          = `SHOW GLOBAL STATUS`
	globalVariablesQuery       = `SHOW GLOBAL VARIABLES`
	slaveStatusQuery           = `SHOW SLAVE STATUS`
	binaryLogsQuery            = `SHOW BINARY LOGS`
	infoSchemaProcessListQuery = `
        SELECT COALESCE(command,''),COALESCE(state,''),count(*)
        FROM information_schema.processlist
        WHERE ID != connection_id()
        GROUP BY command,state
        ORDER BY null
	`
	infoSchemaUserStatisticsQuery = `
        SELECT *,count(*)
        FROM information_schema.user_statistics
	GROUP BY user`
	infoSchemaAutoIncQuery = `
        SELECT table_schema, table_name, column_name, auto_increment,
          CAST(pow(2, case data_type
            when 'tinyint'   then 7
            when 'smallint'  then 15
            when 'mediumint' then 23
            when 'int'       then 31
            when 'bigint'    then 63
            end+(column_type like '% unsigned'))-1 as decimal(19)) as max_int
          FROM information_schema.tables t
          JOIN information_schema.columns c USING (table_schema,table_name)
          WHERE c.extra = 'auto_increment' AND t.auto_increment IS NOT NULL
    `
	innoDBMetricsQuery = `
        SELECT NAME, COUNT
        FROM information_schema.INNODB_METRICS
        WHERE status='enabled'
    `
	perfTableIOWaitsQuery = `
        SELECT OBJECT_SCHEMA, OBJECT_NAME, COUNT_FETCH, COUNT_INSERT, COUNT_UPDATE, COUNT_DELETE,
        SUM_TIMER_FETCH, SUM_TIMER_INSERT, SUM_TIMER_UPDATE, SUM_TIMER_DELETE
        FROM performance_schema.table_io_waits_summary_by_table
        WHERE OBJECT_SCHEMA NOT IN ('mysql', 'performance_schema')
    `
	perfIndexIOWaitsQuery = `
        SELECT OBJECT_SCHEMA, OBJECT_NAME, ifnull(INDEX_NAME, 'NONE') as INDEX_NAME,
        COUNT_FETCH, COUNT_INSERT, COUNT_UPDATE, COUNT_DELETE,
        SUM_TIMER_FETCH, SUM_TIMER_INSERT, SUM_TIMER_UPDATE, SUM_TIMER_DELETE
        FROM performance_schema.table_io_waits_summary_by_index_usage
        WHERE OBJECT_SCHEMA NOT IN ('mysql', 'performance_schema')
    `
	perfTableLockWaitsQuery = `
        SELECT
            OBJECT_SCHEMA,
            OBJECT_NAME,
            COUNT_READ_NORMAL,
            COUNT_READ_WITH_SHARED_LOCKS,
            COUNT_READ_HIGH_PRIORITY,
            COUNT_READ_NO_INSERT,
            COUNT_READ_EXTERNAL,
            COUNT_WRITE_ALLOW_WRITE,
            COUNT_WRITE_CONCURRENT_INSERT,
            COUNT_WRITE_LOW_PRIORITY,
            COUNT_WRITE_NORMAL,
            COUNT_WRITE_EXTERNAL,
            SUM_TIMER_READ_NORMAL,
            SUM_TIMER_READ_WITH_SHARED_LOCKS,
            SUM_TIMER_READ_HIGH_PRIORITY,
            SUM_TIMER_READ_NO_INSERT,
            SUM_TIMER_READ_EXTERNAL,
            SUM_TIMER_WRITE_ALLOW_WRITE,
            SUM_TIMER_WRITE_CONCURRENT_INSERT,
            SUM_TIMER_WRITE_LOW_PRIORITY,
            SUM_TIMER_WRITE_NORMAL,
            SUM_TIMER_WRITE_EXTERNAL
        FROM performance_schema.table_lock_waits_summary_by_table
        WHERE OBJECT_SCHEMA NOT IN ('mysql', 'performance_schema', 'information_schema')
    `
	perfEventsStatementsQuery = `
        SELECT
            ifnull(SCHEMA_NAME, 'NONE') as SCHEMA_NAME,
            DIGEST,
            LEFT(DIGEST_TEXT, %d) as DIGEST_TEXT,
            COUNT_STAR,
            SUM_TIMER_WAIT,
            SUM_ERRORS,
            SUM_WARNINGS,
            SUM_ROWS_AFFECTED,
            SUM_ROWS_SENT,
            SUM_ROWS_EXAMINED,
            SUM_CREATED_TMP_DISK_TABLES,
            SUM_CREATED_TMP_TABLES,
            SUM_SORT_MERGE_PASSES,
            SUM_SORT_ROWS,
            SUM_NO_INDEX_USED
        FROM performance_schema.events_statements_summary_by_digest
        WHERE SCHEMA_NAME NOT IN ('mysql', 'performance_schema', 'information_schema')
            AND last_seen > DATE_SUB(NOW(), INTERVAL %d SECOND)
        ORDER BY SUM_TIMER_WAIT DESC
        LIMIT %d
    `
	perfEventWaitsQuery = `
        SELECT EVENT_NAME, COUNT_STAR, SUM_TIMER_WAIT
        FROM performance_schema.events_waits_summary_global_by_event_name
    `
	perfFileEventsQuery = `
        SELECT
            EVENT_NAME,
            COUNT_READ, SUM_TIMER_READ, SUM_NUMBER_OF_BYTES_READ,
            COUNT_WRITE, SUM_TIMER_WRITE, SUM_NUMBER_OF_BYTES_WRITE,
            COUNT_MISC, SUM_TIMER_MISC
        FROM performance_schema.file_summary_by_event_name
    `
	tableSchemaQuery = `
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            TABLE_TYPE,
            ifnull(ENGINE, 'NONE') as ENGINE,
            ifnull(VERSION, '0') as VERSION,
            ifnull(ROW_FORMAT, 'NONE') as ROW_FORMAT,
            ifnull(TABLE_ROWS, '0') as TABLE_ROWS,
            ifnull(DATA_LENGTH, '0') as DATA_LENGTH,
            ifnull(INDEX_LENGTH, '0') as INDEX_LENGTH,
            ifnull(DATA_FREE, '0') as DATA_FREE,
            ifnull(CREATE_OPTIONS, 'NONE') as CREATE_OPTIONS
        FROM information_schema.tables
        WHERE TABLE_SCHEMA = '%s'
    `
	dbListQuery = `
        SELECT
            SCHEMA_NAME
            FROM information_schema.schemata
        WHERE SCHEMA_NAME NOT IN ('mysql', 'performance_schema', 'information_schema')
    `
	perfSchemaTablesQuery = `
		SELECT
			table_name
			FROM information_schema.tables
		WHERE table_schema = 'performance_schema' AND table_name = ?
	`


	versionQuery = `
		select version() version
	`
	eventCollectionInstrumentsEnableQuery = `
	UPDATE performance_schema.setup_instruments SET ENABLED = 'YES', TIMED = 'YES'
		WHERE NAME LIKE 'stage/%' ||  NAME LIKE 'statement/%';
	`

	eventCollectionConsumersEnableQuery = `
		UPDATE performance_schema.setup_consumers SET ENABLED = 'YES'
			WHERE NAME LIKE '%stages%' || NAME LIKE '%statements%';
	`
	eventStagesQuery_old = `
		select statements.*,stages.event_name,stages.timer_wait stage_time from
			(select a.start_time,a.end_time,a.thread_id,a.event_id,a.sql_time,a.digest_text,a.digest from
				(select date_sub(now(),INTERVAL (select VARIABLE_VALUE from information_schema.global_status where variable_name='UPTIME')-TIMER_START*10e-13 second) start_time
				,date_sub(now(),INTERVAL (select VARIABLE_VALUE from information_schema.global_status where variable_name='UPTIME')-TIMER_END*10e-13 second) end_time
				,thread_id
				,event_id
				,timer_wait  sql_time
				,digest_text
				,current_schema
				,digest
			from performance_schema.events_statements_history_long where digest_text is not null and current_schema is not null) a where a.end_time > ? ) statements left join performance_schema.events_stages_history_long stages
		on statements.event_id = stages.nesting_event_id where stages.event_name is not null and stages.timer_wait is not null order by statements.end_time
	`
	eventStatementsQuery_old = `
		select statements.*,pro.processlist_user,pro.processlist_host,pro.processlist_db from
			(select a.start_time,a.end_time,a.thread_id,a.event_id,a.sql_time,a.digest_text,a.digest from
				(select date_sub(now(),INTERVAL (select VARIABLE_VALUE from information_schema.global_status where variable_name='UPTIME')-TIMER_START*10e-13 second) start_time
				,date_sub(now(),INTERVAL (select VARIABLE_VALUE from information_schema.global_status where variable_name='UPTIME')-TIMER_END*10e-13 second) end_time
				,thread_id
				,event_id
				,timer_wait  sql_time
				,digest_text
				,current_schema
				,digest
			from performance_schema.events_statements_history_long where digest_text is not null and current_schema is not null) a where a.end_time > ? ) statements left join performance_schema.threads pro
		on statements.thread_id = pro.thread_id where pro.processlist_user is not null order by statements.end_time
	`

	eventStagesQuery = `
		select statements.*,stages.event_name,stages.timer_wait stage_time from
			(select a.start_time,a.end_time,a.thread_id,a.event_id,a.sql_time,a.digest_text,a.digest from
				(select date_sub(now(),INTERVAL (select VARIABLE_VALUE from performance_schema.global_status where variable_name='UPTIME')-TIMER_START*10e-13 second) start_time
				,date_sub(now(),INTERVAL (select VARIABLE_VALUE from performance_schema.global_status where variable_name='UPTIME')-TIMER_END*10e-13 second) end_time
				,thread_id
				,event_id
				,timer_wait  sql_time
				,digest_text
				,current_schema
				,digest
			from performance_schema.events_statements_history_long where digest_text is not null and current_schema is not null) a where a.end_time > ? ) statements left join performance_schema.events_stages_history_long stages
		on statements.event_id = stages.nesting_event_id where stages.event_name is not null and stages.timer_wait is not null order by statements.end_time
	`
	eventStatementsQuery = `
		select statements.*,pro.processlist_user,pro.processlist_host,pro.processlist_db from
			(select a.start_time,a.end_time,a.thread_id,a.event_id,a.sql_time,a.digest_text,a.digest from
				(select date_sub(now(),INTERVAL (select VARIABLE_VALUE from performance_schema.global_status where variable_name='UPTIME')-TIMER_START*10e-13 second) start_time
				,date_sub(now(),INTERVAL (select VARIABLE_VALUE from performance_schema.global_status where variable_name='UPTIME')-TIMER_END*10e-13 second) end_time
				,thread_id
				,event_id
				,timer_wait  sql_time
				,digest_text
				,current_schema
				,digest
			from performance_schema.events_statements_history_long where digest_text is not null and current_schema is not null) a where a.end_time > ? ) statements left join performance_schema.threads pro
		on statements.thread_id = pro.thread_id where pro.processlist_user is not null order by statements.end_time
	`
)

func (m *Mysql) gatherServer(serv string, acc telegraf.Accumulator) error {
	serv, err := dsnAddTimeout(serv)
	if err != nil {
		return err
	}

	db, err := sql.Open("mysql", serv)
	if err != nil {
		return err
	}

	defer db.Close()

	if m.GatherGlobalStatus {
		err = m.gatherGlobalStatuses(db, serv, acc)
		if err != nil {
			log.Debugf("gatherGlobalStatuses error:", err)
		} else {
			log.Debugf("gatherGlobalStatuses success")
		}
	}

	// Global Variables may be gathered less often
	if len(m.IntervalSlow) > 0 {
		if uint32(time.Since(lastT).Seconds()) >= scanIntervalSlow {
			err = m.gatherGlobalVariables(db, serv, acc)
			if err != nil {
				return err
			}
			lastT = time.Now()
		}
	}


	if m.GatherEventsStatements {
		err = m.gatherEventsStatements(db, serv, acc)
		if err != nil {
			log.Debugf("GatherEventsStatements error:", err)
		} else {
			log.Debugf("GatherEventsStatements success")
		}
	}

	if m.GatherEventsStages {
		err = m.gatherEventsStages(db, serv, acc)
		if err != nil {
			log.Debugf("gatherEventsStages error:", err)
		} else {
			log.Debugf("gatherEventsStages success")
		}
	}

	if m.GatherEventsStages || m.GatherEventsStatements {
		m.updateMysqlConfig()
	}

	if m.GatherBinaryLogs {
		err = m.gatherBinaryLogs(db, serv, acc)
		if err != nil {
			//return err
			log.Debugf("gatherBinaryLogs error: ", err)
		} else {
			log.Debugf("gatherBinaryLogs success")
		}
	}

	if m.GatherProcessList {
		err = m.GatherProcessListStatuses(db, serv, acc)
		if err != nil {
			//return err
			log.Debugf("GatherProcessList error: ", err)
		} else {
			log.Debugf("GatherProcessList success")
		}
	}

	if m.GatherUserStatistics {
		err = m.GatherUserStatisticsStatuses(db, serv, acc)
		if err != nil {
			//return err
			log.Debugf("GatherUserStatistics error: ", err)
		} else {
			log.Debugf("GatherUserStatistics success")
		}
	}

	if m.GatherSlaveStatus {
		err = m.gatherSlaveStatuses(db, serv, acc)
		if err != nil {
			//return err
			log.Debugf("GatherSlaveStatus error: ", err)
		} else {
			log.Debugf("GatherSlaveStatus success")
		}
	}

	if m.GatherInfoSchemaAutoInc {
		err = m.gatherInfoSchemaAutoIncStatuses(db, serv, acc)
		if err != nil {
			//return err
			log.Debugf("GatherInfoSchemaAutoInc error: ", err)
		} else {
			log.Debugf("GatherInfoSchemaAutoInc success")
		}
	}

	if m.GatherInnoDBMetrics {
		err = m.gatherInnoDBMetrics(db, serv, acc)
		if err != nil {
			//return err
			log.Debugf("GatherInnoDBMetrics error: ", err)
		} else {
			log.Debugf("GatherInnoDBMetrics success")
		}
	}

	if m.GatherTableIOWaits {
		err = m.gatherPerfTableIOWaits(db, serv, acc)
		if err != nil {
			//return err
			log.Debugf("GatherTableIOWaits error: ", err)
		} else {
			log.Debugf("GatherTableIOWaits success")
		}
	}

	if m.GatherIndexIOWaits {
		err = m.gatherPerfIndexIOWaits(db, serv, acc)
		if err != nil {
			//return err
			log.Debugf("GatherIndexIOWaits error: ", err)
		} else {
			log.Debugf("GatherIndexIOWaits success")
		}
	}

	if m.GatherTableLockWaits {
		err = m.gatherPerfTableLockWaits(db, serv, acc)
		if err != nil {
			//return err
			log.Debugf("GatherTableLockWaits error: ", err)
		} else {
			log.Debugf("GatherTableLockWaits success")
		}
	}

	if m.GatherEventWaits {
		err = m.gatherPerfEventWaits(db, serv, acc)
		if err != nil {
			//return err
			log.Debugf("GatherEventWaits error: ", err)
		} else {
			log.Debugf("GatherEventWaits success")
		}
	}

	if m.GatherFileEventsStats {
		err = m.gatherPerfFileEventsStatuses(db, serv, acc)
		if err != nil {
			//return err
			log.Debugf("GatherFileEventsStats error: ", err)
		} else {
			log.Debugf("GatherFileEventsStats success")
		}
	}

	if m.GatherPerfEventsStatements {
		err = m.gatherPerfEventsStatements(db, serv, acc)
		if err != nil {
			//return err
			log.Debugf("GatherPerfEventsStatements error: ", err)
		} else {
			log.Debugf("GatherPerfEventsStatements success")
		}
	}

	if m.GatherTableSchema {
		err = m.gatherTableSchema(db, serv, acc)
		if err != nil {
			//return err
			log.Debugf("GatherTableSchema error: ", err)
		} else {
			log.Debugf("GatherTableSchema success")
		}
	}
	return nil
}

// gatherGlobalVariables can be used to fetch all global variables from
// MySQL environment.
func (m *Mysql) gatherGlobalVariables(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(globalVariablesQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var key string
	var val sql.RawBytes

	// parse DSN and save server tag
	servtag := getDSNTag(serv)
	tags := map[string]string{"server": servtag}
	//fields := make(map[string]interface{})
	fields := map[string]interface{}{"server": servtag, measurement:"global_variable"}
	for rows.Next() {
		if err := rows.Scan(&key, &val); err != nil {
			return err
		}
		key = strings.ToLower(key)
		// parse mysql version and put into field and tag
		if strings.Contains(key, "version") {
			fields[key] = string(val)
			tags[key] = string(val)
		}
		// parse value, if it is numeric then save, otherwise ignore
		if floatVal, ok := parseValue(val); ok {
			fields[key] = floatVal
		}
		// Send 20 fields at a time
		if len(fields) >= 20 {
			acc.AddFields("mysql_variables", fields, tags)
			fields = make(map[string]interface{})
		}
	}
	// Send any remaining fields
	if len(fields) > 0 {
		acc.AddFields("mysql_variables", fields, tags)
	}
	return nil
}

// gatherSlaveStatuses can be used to get replication analytics
// When the server is slave, then it returns only one row.
// If the multi-source replication is set, then everything works differently
// This code does not work with multi-source replication.
func (m *Mysql) gatherSlaveStatuses(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(slaveStatusQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	servtag := getDSNTag(serv)

	tags := map[string]string{"server": servtag}
	fields := make(map[string]interface{})

	// to save the column names as a field key
	// scanning keys and values separately
	if rows.Next() {
		// get columns names, and create an array with its length
		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		vals := make([]interface{}, len(cols))
		// fill the array with sql.Rawbytes
		for i := range vals {
			vals[i] = &sql.RawBytes{}
		}
		if err = rows.Scan(vals...); err != nil {
			return err
		}
		// range over columns, and try to parse values
		for i, col := range cols {
			// skip unparsable values
			if value, ok := parseValue(*vals[i].(*sql.RawBytes)); ok {
				fields["slave_"+col] = value
			}
		}
		acc.AddFields("mysql", fields, tags)
	}

	return nil
}

// gatherBinaryLogs can be used to collect size and count of all binary files
// binlogs metric requires the MySQL server to turn it on in configuration
func (m *Mysql) gatherBinaryLogs(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(binaryLogsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	// parse DSN and save host as a tag
	servtag := getDSNTag(serv)
	tags := map[string]string{"server": servtag}
	var (
		size     uint64 = 0
		count    uint64 = 0
		fileSize uint64
		fileName string
	)

	// iterate over rows and count the size and count of files
	for rows.Next() {
		if err := rows.Scan(&fileName, &fileSize); err != nil {
			return err
		}
		size += fileSize
		count++
	}
	fields := map[string]interface{}{
		"binary_size_bytes":  size,
		"binary_files_count": count,
	}
	acc.AddFields("mysql", fields, tags)
	return nil
}

func getVersion(db *sql.DB) (string, error) {
	rows, err := db.Query(versionQuery)
	if err != nil {
		return "", nil
	}
	defer rows.Close()
	var version string
	for rows.Next() {
		if err := rows.Scan(&version); err != nil {
			return "", err
		}

	}
	return version, nil
}

/*func (m *Mysql) setVersion(db *sql.DB) error {
	version, err := getVersion(db)
	if err != nil {
		return err
	}
	m.Version = version
	//更新配置文件中version
	err = m.updateMysqlConfig()
	return err
}*/

func (m *Mysql) updateMysqlConfig() error {
	confBytes, err := jsoniter.MarshalIndent(m, "", "    ")
	if err != nil {
		return fmt.Errorf("mysql config %v marshal failed, err is %v", m, err)
	}
	if err := ioutil.WriteFile(*mysqlConf, confBytes, 0644); err != nil {
		return err
	}
	return err
}


func (m *Mysql) prepareEventQuery(db *sql.DB) (float64, error) {
	if !m.EventCollectionEnable {
		if _, err := db.Exec(eventCollectionInstrumentsEnableQuery); err != nil {
			return 0, err
		}
		if _, err := db.Exec(eventCollectionConsumersEnableQuery); err != nil {
			return 0, err
		}
		m.EventCollectionEnable = true
	}

	if m.Version == "" {
		version, err := getVersion(db)
		if err != nil {
			return  0, err
		}
		m.Version = version
	}
	version, err := strconv.ParseFloat(m.Version[0 : 3], 32)
	if err != nil {
		return 0, err
	}
	return version, nil
}

func (m *Mysql) gatherEventsStatements(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	version, err := m.prepareEventQuery(db)
	if err != nil {
		return err
	}
	var rows *sql.Rows
	if version <= 5.6 {
		rows, err = db.Query(eventStatementsQuery_old, m.LastGatherStatementsEndTime)
		//rows, err = db.Query(eventStatementsQuery_old, "2018-03-18 15:14:17.288512")
	} else {
		rows, err = db.Query(eventStatementsQuery, m.LastGatherStatementsEndTime)
	}
	if err != nil {
		return err
	}
	defer rows.Close()
	var (
		start_time		string
		end_time		string
		thread_id		uint64
		event_id		uint64
		sql_time		uint64
		digest_text		string
		digest			string
		processlist_user	string
		processlist_host	string
		processlist_db		string
	)
	var servtag string
	servtag = getDSNTag(serv)

	for rows.Next() {

		err = rows.Scan(&start_time, &end_time, &thread_id, &event_id, &sql_time, &digest_text, &digest, &processlist_user, &processlist_host, &processlist_db)
		if err != nil {
			return err
		}

		fields := map[string]interface{}{
			"server":			servtag,
			measurement:		"events_statement",
			"start_time":		start_time,
			"end_time":			end_time,
			"thread_id":		thread_id,
			"event_id":			event_id,
			"sql_time":			sql_time,
			"digest_text":		digest_text,
			"digest":			digest,
			"processlist_user":	processlist_user,
			"processlist_host":	processlist_host,
			"processlist_db":	processlist_db,
		}
		tags := map[string]string{}
		acc.AddFields("eventStatement", fields, tags)
	}
	if start_time != "" {
		m.LastGatherStatementsEndTime = end_time
	}

	return nil
}

func (m *Mysql) gatherEventsStages(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	version, err := m.prepareEventQuery(db)
	if err != nil {
		return err
	}
	var rows *sql.Rows
	if version <= 5.6 {
		rows, err = db.Query(eventStagesQuery_old, m.LastGatherStagesEndTime)
		//rows, err = db.Query(eventStagesQuery_old, "2018-03-18 15:14:17.288512")
	} else {
		rows, err = db.Query(eventStagesQuery, m.LastGatherStagesEndTime)
	}
	if err != nil {
		return err
	}
	defer rows.Close()
	var (
		start_time		string
		end_time		string
		thread_id		uint64
		event_id		uint64
		sql_time		uint64
		digest_text		string
		digest			string
		event_name		string
		stage_time		uint64
	)
	var servtag string
	servtag = getDSNTag(serv)

	for rows.Next() {
		err = rows.Scan(&start_time,&end_time, &thread_id, &event_id, &sql_time, &digest_text, &digest, &event_name, &stage_time)
		if err != nil {
			return err
		}
		fields := map[string]interface{}{
			"server":		servtag,
			"measurements": "event_stage",
			"start_time":	start_time,
			"end_time":		end_time,
			"thread_id":	thread_id,
			"event_id":		event_id,
			"sql_time":		sql_time,
			"digest_text":	digest_text,
			"digest":		digest,
			"event_name":	event_name,
			"stage_time":	stage_time,
		}
		tags := map[string]string{}
		acc.AddFields("eventStage", fields, tags)
	}
	if end_time != "" {
		m.LastGatherStagesEndTime = end_time
	}
	return nil
}


// gatherGlobalStatuses can be used to get MySQL status metrics
// the mappings of actual names and names of each status to be exported
// to output is provided on mappings variable
func (m *Mysql) gatherGlobalStatuses(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	// If user forgot the '/', add it
	if strings.HasSuffix(serv, ")") {
		serv = serv + "/"
	} else if serv == "localhost" {
		serv = ""
	}

	// run query
	rows, err := db.Query(globalStatusQuery)
	if err != nil {
		return err
	}

	// parse the DSN and save host name as a tag
	servtag := getDSNTag(serv)
	//tags := map[string]string{"server": servtag}
	//fields := make(map[string]interface{})
	tags := map[string]string{"server": servtag}
	fields := map[string]interface{}{"server": servtag, measurement: "global_status"}
	for rows.Next() {
		var name string
		var val interface{}

		err = rows.Scan(&name, &val)
		if err != nil {
			return err
		}

		var found bool

		// iterate over mappings and gather metrics that is provided on mapping
		for _, mapped := range mappings {
			if strings.HasPrefix(name, mapped.onServer) {
				// convert numeric values to integer
				i, _ := strconv.Atoi(string(val.([]byte)))
				fields[mapped.inExport+name[len(mapped.onServer):]] = i
				found = true
			}
		}
		// Send 20 fields at a time
/*		if len(fields) >= 20 {
			acc.AddFields("mysql", fields, tags)
			fields = make(map[string]interface{})
		}*/

		if found {
			continue
		}

		// search for specific values
		switch name {
		case "Queries":
			i, err := strconv.ParseInt(string(val.([]byte)), 10, 64)
			if err != nil {
				acc.AddError(fmt.Errorf("E! Error mysql: parsing %s int value (%s)", name, err))
			} else {
				fields["queries"] = i
			}
		case "Questions":
			i, err := strconv.ParseInt(string(val.([]byte)), 10, 64)
			if err != nil {
				acc.AddError(fmt.Errorf("E! Error mysql: parsing %s int value (%s)", name, err))
			} else {
				fields["questions"] = i
			}
		case "Slow_queries":
			i, err := strconv.ParseInt(string(val.([]byte)), 10, 64)
			if err != nil {
				acc.AddError(fmt.Errorf("E! Error mysql: parsing %s int value (%s)", name, err))
			} else {
				fields["slow_queries"] = i
			}
		case "Connections":
			i, err := strconv.ParseInt(string(val.([]byte)), 10, 64)
			if err != nil {
				acc.AddError(fmt.Errorf("E! Error mysql: parsing %s int value (%s)", name, err))
			} else {
				fields["connections"] = i
			}
		case "Syncs":
			i, err := strconv.ParseInt(string(val.([]byte)), 10, 64)
			if err != nil {
				acc.AddError(fmt.Errorf("E! Error mysql: parsing %s int value (%s)", name, err))
			} else {
				fields["syncs"] = i
			}
		case "Uptime":
			i, err := strconv.ParseInt(string(val.([]byte)), 10, 64)
			if err != nil {
				acc.AddError(fmt.Errorf("E! Error mysql: parsing %s int value (%s)", name, err))
			} else {
				fields["uptime"] = i
			}
		}
	}
	// Send any remaining fields
	if len(fields) > 0 {
		acc.AddFields("mysql", fields, tags)
	}
	// gather connection metrics from processlist for each user
	if m.GatherProcessList {
		conn_rows, err := db.Query("SELECT user, sum(1) FROM INFORMATION_SCHEMA.PROCESSLIST GROUP BY user")
		if err != nil {
			log.Printf("E! MySQL Error gathering process list: %s", err)
		} else {
			for conn_rows.Next() {
				var user string
				var connections int64

				err = conn_rows.Scan(&user, &connections)
				if err != nil {
					return err
				}

				tags := map[string]string{"server": servtag, "user": user}
				fields := map[string]interface{}{"server": servtag, "user": user}

				if err != nil {
					return err
				}
				fields["connections"] = connections
				acc.AddFields("mysql_users", fields, tags)
			}
		}
	}

	// gather connection metrics from user_statistics for each user
/*	if m.GatherUserStatistics {
		conn_rows, err := db.Query("select user, total_connections, concurrent_connections, connected_time, busy_time, cpu_time, bytes_received, bytes_sent, binlog_bytes_written, rows_fetched, rows_updated, table_rows_read, select_commands, update_commands, other_commands, commit_transactions, rollback_transactions, denied_connections, lost_connections, access_denied, empty_queries, total_ssl_connections FROM INFORMATION_SCHEMA.USER_STATISTICS GROUP BY user")
		if err != nil {
			log.Printf("MySQL Error gathering user stats: %s", err)
		} else {
			for conn_rows.Next() {
				var user string
				var total_connections int64
				var concurrent_connections int64
				var connected_time int64
				var busy_time int64
				var cpu_time int64
				var bytes_received int64
				var bytes_sent int64
				var binlog_bytes_written int64
				var rows_fetched int64
				var rows_updated int64
				var table_rows_read int64
				var select_commands int64
				var update_commands int64
				var other_commands int64
				var commit_transactions int64
				var rollback_transactions int64
				var denied_connections int64
				var lost_connections int64
				var access_denied int64
				var empty_queries int64
				var total_ssl_connections int64

				err = conn_rows.Scan(&user, &total_connections, &concurrent_connections,
					&connected_time, &busy_time, &cpu_time, &bytes_received, &bytes_sent, &binlog_bytes_written,
					&rows_fetched, &rows_updated, &table_rows_read, &select_commands, &update_commands, &other_commands,
					&commit_transactions, &rollback_transactions, &denied_connections, &lost_connections, &access_denied,
					&empty_queries, &total_ssl_connections,
				)

				if err != nil {
					return err
				}

				tags := map[string]string{"server": servtag, "user": user}
				fields := map[string]interface{}{
					"total_connections":      total_connections,
					"concurrent_connections": concurrent_connections,
					"connected_time":         connected_time,
					"busy_time":              busy_time,
					"cpu_time":               cpu_time,
					"bytes_received":         bytes_received,
					"bytes_sent":             bytes_sent,
					"binlog_bytes_written":   binlog_bytes_written,
					"rows_fetched":           rows_fetched,
					"rows_updated":           rows_updated,
					"table_rows_read":        table_rows_read,
					"select_commands":        select_commands,
					"update_commands":        update_commands,
					"other_commands":         other_commands,
					"commit_transactions":    commit_transactions,
					"rollback_transactions":  rollback_transactions,
					"denied_connections":     denied_connections,
					"lost_connections":       lost_connections,
					"access_denied":          access_denied,
					"empty_queries":          empty_queries,
					"total_ssl_connections":  total_ssl_connections,
				}

				acc.AddFields("mysql_user_stats", fields, tags)
			}
		}
	}*/

	return nil
}

// GatherProcessList can be used to collect metrics on each running command
// and its state with its running count
func (m *Mysql) GatherProcessListStatuses(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(infoSchemaProcessListQuery)
	if err != nil {
		return err
	}
	defer rows.Close()
	var (
		command string
		state   string
		count   uint32
	)

	var servtag string
	fields := make(map[string]interface{})
	servtag = getDSNTag(serv)

	// mapping of state with its counts
	stateCounts := make(map[string]uint32, len(generalThreadStates))
	// set map with keys and default values
	for k, v := range generalThreadStates {
		stateCounts[k] = v
	}

	for rows.Next() {
		err = rows.Scan(&command, &state, &count)
		if err != nil {
			return err
		}
		// each state has its mapping
		foundState := findThreadState(command, state)
		// count each state
		stateCounts[foundState] += count
	}

	tags := map[string]string{"server": servtag}
	for s, c := range stateCounts {
		fields[newNamespace("threads", s)] = c
	}
	acc.AddFields("mysql_info_schema", fields, tags)
	return nil
}

// GatherUserStatistics can be used to collect metrics on each running command
// and its state with its running count
func (m *Mysql) GatherUserStatisticsStatuses(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(infoSchemaUserStatisticsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()
	var (
		user                   string
		total_connections      int64
		concurrent_connections int64
		connected_time         int64
		busy_time              int64
		cpu_time               int64
		bytes_received         int64
		bytes_sent             int64
		binlog_bytes_written   int64
		rows_fetched           int64
		rows_updated           int64
		table_rows_read        int64
		select_commands        int64
		update_commands        int64
		other_commands         int64
		commit_transactions    int64
		rollback_transactions  int64
		denied_connections     int64
		lost_connections       int64
		access_denied          int64
		empty_queries          int64
		total_ssl_connections  int64
		count                  uint32
	)

	servtag := getDSNTag(serv)
	for rows.Next() {
		err = rows.Scan(&user, &total_connections, &concurrent_connections,
			&connected_time, &busy_time, &cpu_time, &bytes_received, &bytes_sent, &binlog_bytes_written,
			&rows_fetched, &rows_updated, &table_rows_read, &select_commands, &update_commands, &other_commands,
			&commit_transactions, &rollback_transactions, &denied_connections, &lost_connections, &access_denied,
			&empty_queries, &total_ssl_connections, &count,
		)
		if err != nil {
			return err
		}

		tags := map[string]string{"server": servtag, "user": user}
		fields := map[string]interface{}{

			"total_connections":      total_connections,
			"concurrent_connections": concurrent_connections,
			"connected_time":         connected_time,
			"busy_time":              busy_time,
			"cpu_time":               cpu_time,
			"bytes_received":         bytes_received,
			"bytes_sent":             bytes_sent,
			"binlog_bytes_written":   binlog_bytes_written,
			"rows_fetched":           rows_fetched,
			"rows_updated":           rows_updated,
			"table_rows_read":        table_rows_read,
			"select_commands":        select_commands,
			"update_commands":        update_commands,
			"other_commands":         other_commands,
			"commit_transactions":    commit_transactions,
			"rollback_transactions":  rollback_transactions,
			"denied_connections":     denied_connections,
			"lost_connections":       lost_connections,
			"access_denied":          access_denied,
			"empty_queries":          empty_queries,
			"total_ssl_connections":  total_ssl_connections,
		}
		acc.AddFields("mysql_user_stats", fields, tags)
	}
	return nil
}

// gatherPerfTableIOWaits can be used to get total count and time
// of I/O wait event for each table and process
func (m *Mysql) gatherPerfTableIOWaits(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	rows, err := db.Query(perfTableIOWaitsQuery)
	if err != nil {
		return err
	}

	defer rows.Close()
	var (
		objSchema, objName, servtag                       string
		countFetch, countInsert, countUpdate, countDelete float64
		timeFetch, timeInsert, timeUpdate, timeDelete     float64
	)

	servtag = getDSNTag(serv)

	for rows.Next() {
		err = rows.Scan(&objSchema, &objName,
			&countFetch, &countInsert, &countUpdate, &countDelete,
			&timeFetch, &timeInsert, &timeUpdate, &timeDelete,
		)

		if err != nil {
			return err
		}

		tags := map[string]string{
			"server": servtag,
			"schema": objSchema,
			"name":   objName,
		}

		fields := map[string]interface{}{
			"table_io_waits_total_fetch":          countFetch,
			"table_io_waits_total_insert":         countInsert,
			"table_io_waits_total_update":         countUpdate,
			"table_io_waits_total_delete":         countDelete,
			"table_io_waits_seconds_total_fetch":  timeFetch / picoSeconds,
			"table_io_waits_seconds_total_insert": timeInsert / picoSeconds,
			"table_io_waits_seconds_total_update": timeUpdate / picoSeconds,
			"table_io_waits_seconds_total_delete": timeDelete / picoSeconds,
		}

		acc.AddFields("mysql_perf_schema", fields, tags)
	}
	return nil
}

// gatherPerfIndexIOWaits can be used to get total count and time
// of I/O wait event for each index and process
func (m *Mysql) gatherPerfIndexIOWaits(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	rows, err := db.Query(perfIndexIOWaitsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		objSchema, objName, indexName, servtag            string
		countFetch, countInsert, countUpdate, countDelete float64
		timeFetch, timeInsert, timeUpdate, timeDelete     float64
	)

	servtag = getDSNTag(serv)

	for rows.Next() {
		err = rows.Scan(&objSchema, &objName, &indexName,
			&countFetch, &countInsert, &countUpdate, &countDelete,
			&timeFetch, &timeInsert, &timeUpdate, &timeDelete,
		)

		if err != nil {
			return err
		}

		tags := map[string]string{
			"server": servtag,
			"schema": objSchema,
			"name":   objName,
			"index":  indexName,
		}
		fields := map[string]interface{}{
			"index_io_waits_total_fetch":         countFetch,
			"index_io_waits_seconds_total_fetch": timeFetch / picoSeconds,
		}

		// update write columns only when index is NONE
		if indexName == "NONE" {
			fields["index_io_waits_total_insert"] = countInsert
			fields["index_io_waits_total_update"] = countUpdate
			fields["index_io_waits_total_delete"] = countDelete

			fields["index_io_waits_seconds_total_insert"] = timeInsert / picoSeconds
			fields["index_io_waits_seconds_total_update"] = timeUpdate / picoSeconds
			fields["index_io_waits_seconds_total_delete"] = timeDelete / picoSeconds
		}

		acc.AddFields("mysql_perf_schema", fields, tags)
	}
	return nil
}

// gatherInfoSchemaAutoIncStatuses can be used to get auto incremented values of the column
func (m *Mysql) gatherInfoSchemaAutoIncStatuses(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	rows, err := db.Query(infoSchemaAutoIncQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		schema, table, column string
		incValue, maxInt      uint64
	)

	servtag := getDSNTag(serv)

	for rows.Next() {
		if err := rows.Scan(&schema, &table, &column, &incValue, &maxInt); err != nil {
			return err
		}
		tags := map[string]string{
			"server": servtag,
			"schema": schema,
			"table":  table,
			"column": column,
		}
		fields := make(map[string]interface{})
		fields["auto_increment_column"] = incValue
		fields["auto_increment_column_max"] = maxInt

		acc.AddFields("mysql_info_schema", fields, tags)
	}
	return nil
}

// gatherInnoDBMetrics can be used to fetch enabled metrics from
// information_schema.INNODB_METRICS
func (m *Mysql) gatherInnoDBMetrics(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(innoDBMetricsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var key string
	var val sql.RawBytes

	// parse DSN and save server tag
	servtag := getDSNTag(serv)
	tags := map[string]string{"server": servtag}
	fields := make(map[string]interface{})
	for rows.Next() {
		if err := rows.Scan(&key, &val); err != nil {
			return err
		}
		key = strings.ToLower(key)
		// parse value, if it is numeric then save, otherwise ignore
		if floatVal, ok := parseValue(val); ok {
			fields[key] = floatVal
		}
		// Send 20 fields at a time
		if len(fields) >= 20 {
			acc.AddFields("mysql_innodb", fields, tags)
			fields = make(map[string]interface{})
		}
	}
	// Send any remaining fields
	if len(fields) > 0 {
		acc.AddFields("mysql_innodb", fields, tags)
	}
	return nil
}

// gatherPerfTableLockWaits can be used to get
// the total number and time for SQL and external lock wait events
// for each table and operation
// requires the MySQL server to be enabled to save this metric
func (m *Mysql) gatherPerfTableLockWaits(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	// check if table exists,
	// if performance_schema is not enabled, tables do not exist
	// then there is no need to scan them
	var tableName string
	err := db.QueryRow(perfSchemaTablesQuery, "table_lock_waits_summary_by_table").Scan(&tableName)
	switch {
	case err == sql.ErrNoRows:
		return nil
	case err != nil:
		return err
	}

	rows, err := db.Query(perfTableLockWaitsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	servtag := getDSNTag(serv)

	var (
		objectSchema               string
		objectName                 string
		countReadNormal            float64
		countReadWithSharedLocks   float64
		countReadHighPriority      float64
		countReadNoInsert          float64
		countReadExternal          float64
		countWriteAllowWrite       float64
		countWriteConcurrentInsert float64
		countWriteLowPriority      float64
		countWriteNormal           float64
		countWriteExternal         float64
		timeReadNormal             float64
		timeReadWithSharedLocks    float64
		timeReadHighPriority       float64
		timeReadNoInsert           float64
		timeReadExternal           float64
		timeWriteAllowWrite        float64
		timeWriteConcurrentInsert  float64
		timeWriteLowPriority       float64
		timeWriteNormal            float64
		timeWriteExternal          float64
	)

	for rows.Next() {
		err = rows.Scan(
			&objectSchema,
			&objectName,
			&countReadNormal,
			&countReadWithSharedLocks,
			&countReadHighPriority,
			&countReadNoInsert,
			&countReadExternal,
			&countWriteAllowWrite,
			&countWriteConcurrentInsert,
			&countWriteLowPriority,
			&countWriteNormal,
			&countWriteExternal,
			&timeReadNormal,
			&timeReadWithSharedLocks,
			&timeReadHighPriority,
			&timeReadNoInsert,
			&timeReadExternal,
			&timeWriteAllowWrite,
			&timeWriteConcurrentInsert,
			&timeWriteLowPriority,
			&timeWriteNormal,
			&timeWriteExternal,
		)

		if err != nil {
			return err
		}
		tags := map[string]string{
			"server": servtag,
			"schema": objectSchema,
			"table":  objectName,
		}

		sqlLWTags := copyTags(tags)
		sqlLWTags["perf_query"] = "sql_lock_waits_total"
		sqlLWFields := map[string]interface{}{
			"read_normal":             countReadNormal,
			"read_with_shared_locks":  countReadWithSharedLocks,
			"read_high_priority":      countReadHighPriority,
			"read_no_insert":          countReadNoInsert,
			"write_normal":            countWriteNormal,
			"write_allow_write":       countWriteAllowWrite,
			"write_concurrent_insert": countWriteConcurrentInsert,
			"write_low_priority":      countWriteLowPriority,
		}
		acc.AddFields("mysql_perf_schema", sqlLWFields, sqlLWTags)

		externalLWTags := copyTags(tags)
		externalLWTags["perf_query"] = "external_lock_waits_total"
		externalLWFields := map[string]interface{}{
			"read":  countReadExternal,
			"write": countWriteExternal,
		}
		acc.AddFields("mysql_perf_schema", externalLWFields, externalLWTags)

		sqlLWSecTotalTags := copyTags(tags)
		sqlLWSecTotalTags["perf_query"] = "sql_lock_waits_seconds_total"
		sqlLWSecTotalFields := map[string]interface{}{
			"read_normal":             timeReadNormal / picoSeconds,
			"read_with_shared_locks":  timeReadWithSharedLocks / picoSeconds,
			"read_high_priority":      timeReadHighPriority / picoSeconds,
			"read_no_insert":          timeReadNoInsert / picoSeconds,
			"write_normal":            timeWriteNormal / picoSeconds,
			"write_allow_write":       timeWriteAllowWrite / picoSeconds,
			"write_concurrent_insert": timeWriteConcurrentInsert / picoSeconds,
			"write_low_priority":      timeWriteLowPriority / picoSeconds,
		}
		acc.AddFields("mysql_perf_schema", sqlLWSecTotalFields, sqlLWSecTotalTags)

		externalLWSecTotalTags := copyTags(tags)
		externalLWSecTotalTags["perf_query"] = "external_lock_waits_seconds_total"
		externalLWSecTotalFields := map[string]interface{}{
			"read":  timeReadExternal / picoSeconds,
			"write": timeWriteExternal / picoSeconds,
		}
		acc.AddFields("mysql_perf_schema", externalLWSecTotalFields, externalLWSecTotalTags)
	}
	return nil
}

// gatherPerfEventWaits can be used to get total time and number of event waits
func (m *Mysql) gatherPerfEventWaits(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	rows, err := db.Query(perfEventWaitsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		event               string
		starCount, timeWait float64
	)

	servtag := getDSNTag(serv)
	tags := map[string]string{
		"server": servtag,
	}
	for rows.Next() {
		if err := rows.Scan(&event, &starCount, &timeWait); err != nil {
			return err
		}
		tags["event_name"] = event
		fields := map[string]interface{}{
			"events_waits_total":         starCount,
			"events_waits_seconds_total": timeWait / picoSeconds,
		}

		acc.AddFields("mysql_perf_schema", fields, tags)
	}
	return nil
}

// gatherPerfFileEvents can be used to get stats on file events
func (m *Mysql) gatherPerfFileEventsStatuses(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	rows, err := db.Query(perfFileEventsQuery)
	if err != nil {
		return err
	}

	defer rows.Close()

	var (
		eventName                                 string
		countRead, countWrite, countMisc          float64
		sumTimerRead, sumTimerWrite, sumTimerMisc float64
		sumNumBytesRead, sumNumBytesWrite         float64
	)

	servtag := getDSNTag(serv)
	tags := map[string]string{
		"server": servtag,
	}
	for rows.Next() {
		err = rows.Scan(
			&eventName,
			&countRead, &sumTimerRead, &sumNumBytesRead,
			&countWrite, &sumTimerWrite, &sumNumBytesWrite,
			&countMisc, &sumTimerMisc,
		)
		if err != nil {
			return err
		}

		tags["event_name"] = eventName
		fields := make(map[string]interface{})

		miscTags := copyTags(tags)
		miscTags["mode"] = "misc"
		fields["file_events_total"] = countWrite
		fields["file_events_seconds_total"] = sumTimerMisc / picoSeconds
		acc.AddFields("mysql_perf_schema", fields, miscTags)

		readTags := copyTags(tags)
		readTags["mode"] = "read"
		fields["file_events_total"] = countRead
		fields["file_events_seconds_total"] = sumTimerRead / picoSeconds
		fields["file_events_bytes_totals"] = sumNumBytesRead
		acc.AddFields("mysql_perf_schema", fields, readTags)

		writeTags := copyTags(tags)
		writeTags["mode"] = "write"
		fields["file_events_total"] = countWrite
		fields["file_events_seconds_total"] = sumTimerWrite / picoSeconds
		fields["file_events_bytes_totals"] = sumNumBytesWrite
		acc.AddFields("mysql_perf_schema", fields, writeTags)

	}
	return nil
}

// gatherPerfEventsStatements can be used to get attributes of each event
func (m *Mysql) gatherPerfEventsStatements(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	query := fmt.Sprintf(
		perfEventsStatementsQuery,
		m.PerfEventsStatementsDigestTextLimit,
		m.PerfEventsStatementsTimeLimit,
		m.PerfEventsStatementsLimit,
	)

	rows, err := db.Query(query)
	if err != nil {
		return err
	}

	defer rows.Close()

	var (
		schemaName, digest, digest_text      string
		count, queryTime, errors, warnings   float64
		rowsAffected, rowsSent, rowsExamined float64
		tmpTables, tmpDiskTables             float64
		sortMergePasses, sortRows            float64
		noIndexUsed                          float64
	)

	servtag := getDSNTag(serv)
	tags := map[string]string{
		"server": servtag,
	}

	for rows.Next() {
		err = rows.Scan(
			&schemaName, &digest, &digest_text,
			&count, &queryTime, &errors, &warnings,
			&rowsAffected, &rowsSent, &rowsExamined,
			&tmpTables, &tmpDiskTables,
			&sortMergePasses, &sortRows,
			&noIndexUsed,
		)

		if err != nil {
			return err
		}
		tags["schema"] = schemaName
		tags["digest"] = digest
		tags["digest_text"] = digest_text

		fields := map[string]interface{}{
			"events_statements_total":                   count,
			"events_statements_seconds_total":           queryTime / picoSeconds,
			"events_statements_errors_total":            errors,
			"events_statements_warnings_total":          warnings,
			"events_statements_rows_affected_total":     rowsAffected,
			"events_statements_rows_sent_total":         rowsSent,
			"events_statements_rows_examined_total":     rowsExamined,
			"events_statements_tmp_tables_total":        tmpTables,
			"events_statements_tmp_disk_tables_total":   tmpDiskTables,
			"events_statements_sort_merge_passes_total": sortMergePasses,
			"events_statements_sort_rows_total":         sortRows,
			"events_statements_no_index_used_total":     noIndexUsed,
		}

		acc.AddFields("mysql_perf_schema", fields, tags)
	}
	return nil
}

// gatherTableSchema can be used to gather stats on each schema
func (m *Mysql) gatherTableSchema(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	var dbList []string
	servtag := getDSNTag(serv)

	// if the list of databases if empty, then get all databases
	if len(m.TableSchemaDatabases) == 0 {
		rows, err := db.Query(dbListQuery)
		if err != nil {
			return err
		}
		defer rows.Close()

		var database string
		for rows.Next() {
			err = rows.Scan(&database)
			if err != nil {
				return err
			}

			dbList = append(dbList, database)
		}
	} else {
		dbList = m.TableSchemaDatabases
	}

	for _, database := range dbList {
		rows, err := db.Query(fmt.Sprintf(tableSchemaQuery, database))
		if err != nil {
			return err
		}
		defer rows.Close()
		var (
			tableSchema   string
			tableName     string
			tableType     string
			engine        string
			version       float64
			rowFormat     string
			tableRows     float64
			dataLength    float64
			indexLength   float64
			dataFree      float64
			createOptions string
		)
		for rows.Next() {
			err = rows.Scan(
				&tableSchema,
				&tableName,
				&tableType,
				&engine,
				&version,
				&rowFormat,
				&tableRows,
				&dataLength,
				&indexLength,
				&dataFree,
				&createOptions,
			)
			if err != nil {
				return err
			}
			tags := map[string]string{"server": servtag}
			tags["schema"] = tableSchema
			tags["table"] = tableName

			acc.AddFields(newNamespace("info_schema", "table_rows"),
				map[string]interface{}{"value": tableRows}, tags)

			dlTags := copyTags(tags)
			dlTags["component"] = "data_length"
			acc.AddFields(newNamespace("info_schema", "table_size", "data_length"),
				map[string]interface{}{"value": dataLength}, dlTags)

			ilTags := copyTags(tags)
			ilTags["component"] = "index_length"
			acc.AddFields(newNamespace("info_schema", "table_size", "index_length"),
				map[string]interface{}{"value": indexLength}, ilTags)

			dfTags := copyTags(tags)
			dfTags["component"] = "data_free"
			acc.AddFields(newNamespace("info_schema", "table_size", "data_free"),
				map[string]interface{}{"value": dataFree}, dfTags)

			versionTags := copyTags(tags)
			versionTags["type"] = tableType
			versionTags["engine"] = engine
			versionTags["row_format"] = rowFormat
			versionTags["create_options"] = createOptions

			acc.AddFields(newNamespace("info_schema", "table_version"),
				map[string]interface{}{"value": version}, versionTags)
		}
	}
	return nil
}

// parseValue can be used to convert values such as "ON","OFF","Yes","No" to 0,1
func parseValue(value sql.RawBytes) (float64, bool) {
	if bytes.Compare(value, []byte("Yes")) == 0 || bytes.Compare(value, []byte("ON")) == 0 {
		return 1, true
	}

	if bytes.Compare(value, []byte("No")) == 0 || bytes.Compare(value, []byte("OFF")) == 0 {
		return 0, true
	}
	n, err := strconv.ParseFloat(string(value), 64)
	return n, err == nil
}

// findThreadState can be used to find thread state by command and plain state
func findThreadState(rawCommand, rawState string) string {
	var (
		// replace '_' symbol with space
		command = strings.Replace(strings.ToLower(rawCommand), "_", " ", -1)
		state   = strings.Replace(strings.ToLower(rawState), "_", " ", -1)
	)
	// if the state is already valid, then return it
	if _, ok := generalThreadStates[state]; ok {
		return state
	}

	// if state is plain, return the mapping
	if mappedState, ok := stateStatusMappings[state]; ok {
		return mappedState
	}
	// if the state is any lock, return the special state
	if strings.Contains(state, "waiting for") && strings.Contains(state, "lock") {
		return "waiting for lock"
	}

	if command == "sleep" && state == "" {
		return "idle"
	}

	if command == "query" {
		return "executing"
	}

	if command == "binlog dump" {
		return "replication master"
	}
	// if no mappings found and state is invalid, then return "other" state
	return "other"
}

// newNamespace can be used to make a namespace
func newNamespace(words ...string) string {
	return strings.Replace(strings.Join(words, "_"), " ", "_", -1)
}

func copyTags(in map[string]string) map[string]string {
	out := make(map[string]string)
	for k, v := range in {
		out[k] = v
	}
	return out
}

func dsnAddTimeout(dsn string) (string, error) {
	conf, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}

	if conf.Timeout == 0 {
		conf.Timeout = time.Second * 5
	}

	return conf.FormatDSN(), nil
}

func getDSNTag(dsn string) string {
	conf, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "127.0.0.1:3306"
	}
	return conf.Addr
}
