package utils

import (
	"fmt"
	"github.com/qiniu/log"
	"os"
	"path/filepath"
	"time"
)

func RouteLog(logPath string) error {
	dir, pattern, err := GetDirAndFile(logPath)
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, DefaultDirPerm); err != nil {
			err = fmt.Errorf("create  log dir error %v", err)
			return err
		}
	}
	fileName := pattern + time.Now().Format("_20060102") + ".log"
	fullName := filepath.Join(dir, fileName)
	logFile, err := os.OpenFile(fullName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.ModeAppend)
	if err != nil {
		err = fmt.Errorf("rotateLog open newfile %v err %v", logFile, err)
		return err
	}
	log.SetOutput(logFile)
	log.SetOutputLevel(0)
	return nil
}
