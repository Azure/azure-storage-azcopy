package common

import (
	"net"
	"net/url"
)

var AzcopyJobPlanFolder string
var AzcopyCurrentJobLogger ILoggerResetable


func VerifyIsURLResolvable(url_string string) (error) {
	url, err := url.Parse(url_string)
	if (err != nil) {
		return err
	}

	_, err = net.LookupIP(url.Hostname())
	return err
}