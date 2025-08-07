package utils

import (
	"encoding/csv"
	"fmt"
	"github.com/aliyun/kvc-3fs-operator/internal/constant"
	uuid "github.com/satori/go.uuid"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func StrListNotContains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return false
		}
	}
	return true
}

func StrListAllContains(list []string, s string) bool {
	for _, v := range list {
		if v != s {
			return false
		}
	}
	return true
}

func StrListContains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func StrListReplace(list []string, old, new string) []string {
	accessPointNode := make([]string, 0)
	for _, v := range list {
		accessPointNode = append(accessPointNode, strings.ReplaceAll(v, old, new))
	}
	return accessPointNode
}

func StrListRemove(list []string, s string) []string {
	tmp := make([]string, 0)
	for _, v := range list {
		if v == s {
			continue
		}
		tmp = append(tmp, v)
	}
	return tmp
}

func MergeMaps(map1, map2 map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range map1 {
		result[k] = v
	}
	for k, v := range map2 {
		if _, exists := result[k]; !exists {
			result[k] = v
		}
	}
	return result
}

func MergeMapsInPlace(map1, map2 map[string]string) {
	if map1 == nil {
		map1 = make(map[string]string)
	}
	for k, v := range map2 {
		map1[k] = v
	}
}

func GenerateUuid() string {
	return strings.ReplaceAll(uuid.NewV4().String(), "-", "")
}

func GenerateUuidWithLen(length int) string {
	ramdomStr := GenerateUuid()
	if length <= 0 || length > len(ramdomStr) {
		return ""
	}
	return ramdomStr[:length]
}

func GetPrefixFile(dir, prefix string) ([]string, error) {
	pattern := filepath.Join(dir, prefix+"*")
	return filepath.Glob(pattern)
}

func TranslatePlainNodeName3fs(nodeName string) string {
	return strings.ReplaceAll(strings.ReplaceAll(nodeName, "-", "_"), ".", "_")
}

func TranslatePlainNodeNameValid(nodeName string) string {
	return strings.ReplaceAll(strings.ReplaceAll(nodeName, "_", "-"), ".", "-")
}

func FindChanges(old, new []string) (added, removed []string) {
	oldMap := make(map[string]bool)
	newMap := make(map[string]bool)

	for _, v := range old {
		oldMap[v] = true
	}

	for _, v := range new {
		newMap[v] = true
	}

	for k := range newMap {
		if !oldMap[k] {
			added = append(added, k)
		}
	}

	for k := range oldMap {
		if !newMap[k] {
			removed = append(removed, k)
		}
	}

	return
}

func MergeCSVFiles(input1, input2, output string) error {
	f1, err := os.Open(input1)
	if err != nil {
		return err
	}
	defer f1.Close()

	r1 := csv.NewReader(f1)
	records1, err := r1.ReadAll()
	if err != nil {
		return err
	}

	f2, err := os.Open(input2)
	if err != nil {
		return err
	}
	defer f2.Close()

	r2 := csv.NewReader(f2)
	records2, err := r2.ReadAll()
	if err != nil {
		return err
	}

	merged := append(records1, records2[1:]...)

	if _, err := os.Stat(output); err == nil {
		if err := os.Remove(output); err != nil {
			return fmt.Errorf("delete file: %+v", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat file err: %+v", err)
	}

	fOut, err := os.Create(output)
	if err != nil {
		return err
	}
	defer fOut.Close()

	w := csv.NewWriter(fOut)
	err = w.WriteAll(merged)
	if err != nil {
		return err
	}

	return nil
}

func GetUseHostNetworkEnv() bool {
	if os.Getenv(constant.ENVUseHostnetwork) == "true" {
		return true
	}
	// empty or false
	return false
}

func GetFaultDurationEnv() int {
	faultDuration := os.Getenv(constant.ENVFaultDuration)
	var faultDurationTime int
	var err error
	if faultDuration == "" {
		faultDurationTime = 5
	} else {
		faultDurationTime, err = strconv.Atoi(faultDuration)
		if err != nil {
			faultDurationTime = 5
		}
	}
	return faultDurationTime
}

func GetEnableTraceEnv() bool {
	return os.Getenv(constant.ENVEnableTrace) == "true"
}

func ResolveDNS(domain string) (ips []net.IP, err error) {
	return net.LookupIP(domain)
}
