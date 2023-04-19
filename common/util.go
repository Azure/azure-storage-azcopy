package common

var AzcopyJobPlanFolder string
var AzcopyCurrentJobLogger ILoggerResetable


func VerifyIsURLResolvable(url_string string) (error) {
	/* This function is disabled. But we should still fix this after fixing the below stuff.
	 * We can take this up after migration to new SDK. The pipeline infra may not be same then.
	 * 1. At someplaces we use Blob SDK directly to create pipeline - ex getBlobCredentialType()
	 *    We should create pipeline through helper functions create<Blob/File/blobfs>pipeline, where we
	 *    handle errors appropriately.
	 * 2. We should either do a http.Get or net.Dial instead of lookIP. If we are behind a proxy, we may 
	 *    not resolve this IP. #2144
	 * 3. DNS erros may by temporary, we should try for a minute before we give up.
	 */
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