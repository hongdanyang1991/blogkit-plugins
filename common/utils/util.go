package utils

import (
	"fmt"
	"path/filepath"
	"strings"
	"unicode"
)

const (
	DefaultDirPerm  = 0755
	DefaultFilePerm = 0600
)

//将文件路径拆分为目录和文件
func GetDirAndFile(path string) (dir, file string, err error) {
	dir, err = filepath.Abs(filepath.Dir(path))
	if err != nil {
		err = fmt.Errorf("get path dir error %v", err)
		return
	}
	file = filepath.Base(path)
	return
}

func SplitSlice(keyStr string) []string {
	keys := strings.FieldsFunc(keyStr, isSeparator)
	return keys
}

func isSeparator(separator rune) bool {
	return separator == ',' || unicode.IsSpace(separator)
}
