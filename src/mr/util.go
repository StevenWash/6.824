package mr

import (
	"crypto/md5"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"reflect"
	"time"
)

const AKSK  = "github"

var letterRunes = []rune("_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}

	return string(b)
}

func GeneratUUID() string {
	t := time.Now()
	h := md5.New()
	io.WriteString(h, t.String())
	passwd := fmt.Sprintf("%x", h.Sum(nil))
	return passwd
}

func StringToHashCode(str string) int {
	crcCode := int(crc32.ChecksumIEEE([]byte(str)))

	if crcCode >= 0 {
		return crcCode
	}
	if -crcCode >= 0 {
		return -crcCode
	}
	return 0
}

func Contain(val interface{}, arrs interface{}) bool {
	switch reflect.TypeOf(arrs).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(arrs)
		for i := 0; i < s.Len(); i++ {
			if reflect.DeepEqual(val, s.Index(i).Interface()) {
				return true
			}
		}
	}
	return  false
}

