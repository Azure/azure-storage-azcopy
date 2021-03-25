package common

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/minio/minio-go/pkg/s3utils"
)

// It's not pretty that this one is read directly by credential util.
// But doing otherwise required us passing it around in many places, even though really
// it can be thought of as an "ambient" property. That's the (weak?) justification for implementing
// it as a global
var CmdLineExtraSuffixesAAD string

const TrustedSuffixesNameAAD = "trusted-microsoft-suffixes"
// note: there is a duplicate of this in mgr-jobmgr.go regarding
const TrustedSuffixesAAD = "*.core.windows.net;*.core.chinacloudapi.cn;*.core.cloudapi.de;*.core.usgovcloudapi.net"

// CheckAuthSafeForTarget checks our "implicit" auth types (those that pick up creds from the environment
// or a prior login) to make sure they are only being used in places where we know those auth types are safe.
// This prevents, for example, us accidentally sending OAuth creds to some place they don't belong
func CheckAuthSafeForTarget(ct CredentialType, resource, extraSuffixesAAD string, resourceType Location) error {

	getSuffixes := func(list string, extras string) []string {
		extras = strings.Trim(extras, " ")
		if extras != "" {
			list += ";" + extras
		}
		return strings.Split(list, ";")
	}

	isResourceInSuffixList := func(suffixes []string) (string, bool) {
		u, err := url.Parse(resource)
		if err != nil {
			return "<unparsable>", false
		}
		host := strings.ToLower(u.Host)

		for _, s := range suffixes {
			s = strings.Trim(s, " *") // trim *.foo to .foo
			s = strings.ToLower(s)
			if strings.HasSuffix(host, s) {
				return host, true
			}
		}
		return host, false
	}

	switch ct {
	case ECredentialType.Unknown(),
		ECredentialType.Anonymous():
		// these auth types don't pick up anything from environment vars, so they are not the focus of this routine
		return nil
	case ECredentialType.OAuthToken(),
		ECredentialType.SharedKey():
		// Files doesn't currently support OAuth, but it's a valid azure endpoint anyway, so it'll pass the check.
		if resourceType != ELocation.Blob() && resourceType != ELocation.BlobFS() && resourceType != ELocation.File() {
			// There may be a reason for files->blob to specify this.
			if resourceType == ELocation.Local() {
				return nil
			}

			return fmt.Errorf("azure OAuth authentication to %s is not enabled in AzCopy", resourceType.String())
		}

		// these are Azure auth types, so make sure the resource is known to be in Azure
		domainSuffixes := getSuffixes(TrustedSuffixesAAD, extraSuffixesAAD)
		if host, ok := isResourceInSuffixList(domainSuffixes); !ok {
			return fmt.Errorf(
				"the URL requires authentication. If this URL is in fact an Azure service, you can enable Azure authentication to %s. "+
					"To enable, view the documentation for "+
					"the parameter --%s, by running 'AzCopy copy --help'. BUT if this URL is not an Azure service, do NOT enable Azure authentication to it. "+
					"Instead, see if the URL host supports authentication by way of a token that can be included in the URL's query string",
				// E.g. CDN apparently supports a non-SAS type of token as noted here: https://docs.microsoft.com/en-us/azure/cdn/cdn-token-auth#setting-up-token-authentication
				// Including such a token in the URL will cause AzCopy to see it as a "public" URL (since the URL on its own will pass
				// our "isPublic" access tests, which run before this routine).
				host, TrustedSuffixesNameAAD)
		}

	case ECredentialType.S3AccessKey():
		if resourceType != ELocation.S3() {
			//noinspection ALL
			return fmt.Errorf("S3 access key authentication to %s is not enabled in AzCopy", resourceType.String())
		}

		// just check with minio. No need to have our own list of S3 domains, since minio effectively
		// has that list already, we can't talk to anything outside that list because minio won't let us,
		// and the parsing of s3 URL is non-trivial.  E.g. can't just look for the ending since
		// something like https://someApi.execute-api.someRegion.amazonaws.com is AWS but is a customer-
		// written code, not S3.
		ok := false
		host := "<unparseable url>"
		u, err := url.Parse(resource)
		if err == nil {
			host = u.Host
			parts, err := NewS3URLParts(*u) // strip any leading bucket name from URL, to get an endpoint we can pass to s3utils
			if err == nil {
				u, err := url.Parse("https://" + parts.Endpoint)
				ok = err == nil && s3utils.IsAmazonEndpoint(*u)
			}
		}

		if !ok {
			return fmt.Errorf(
				"s3 authentication to %s is not currently suported in AzCopy", host)
		}
	case ECredentialType.GoogleAppCredentials():
		if resourceType != ELocation.GCP() {
			return fmt.Errorf("Google Application Credentials to %s is not valid", resourceType.String())
		}

		host := "<unparseable url>"
		u, err := url.Parse(resource)
		if err == nil {
			host = u.Host
			_, err := NewGCPURLParts(*u)
			if err != nil {
				return fmt.Errorf("GCP authentication to %s is not currently supported", host)
			}
		}
	default:
		panic("unknown credential type")
	}

	return nil
}
