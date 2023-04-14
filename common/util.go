package common

var AzcopyJobPlanFolder string
var AzcopyCurrentJobLogger ILoggerResetable


func VerifyIsURLResolvable(url_string string) (error) {
	return nil
	/*
	url, err := url.Parse(url_string)
	if (err != nil) {
		return err
	}

	_, err = net.LookupIP(url.Host)
	return err
	*/
}