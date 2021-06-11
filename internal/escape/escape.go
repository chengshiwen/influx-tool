package escape

import (
	"bytes"

	"github.com/influxdata/influxdb/models"
)

var (
	measurementEscapeCodes = [...]byte{',', ' '}
	tagEscapeCodes         = [...]byte{',', ' ', '='}
)

func NeedMeasurementEscape(name []byte) bool {
	for i := range measurementEscapeCodes {
		c := measurementEscapeCodes[i]
		if bytes.IndexByte(name, c) != -1 {
			return true
		}
	}
	return false
}

func NeedTagsEscape(tags models.Tags) bool {
	for i := range tags {
		t := &tags[i]
		for j := range tagEscapeCodes {
			c := tagEscapeCodes[j]
			if bytes.IndexByte(t.Key, c) != -1 || bytes.IndexByte(t.Value, c) != -1 {
				return true
			}
		}
	}
	return false
}

func NeedEscape(name []byte, tags models.Tags) bool {
	return NeedMeasurementEscape(name) || NeedTagsEscape(tags)
}
