package ste

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Contents represents the structure of the contents field in Entry
type Contents struct {
	Path string `json:"path"`
	Hash string `json:"hash"`
}

// Entry represents the structure of each entry in the response
type Entry struct {
	Contents      string `json:"contents"`
	SubLedgerID   string `json:"subLedgerId"`
	TransactionID string `json:"transactionId"`
}

// Response represents the structure of the JSON ledger response
type Response struct {
	Entries []Entry `json:"entries"`
	State   string  `json:"state"`
}

// Results from a hash download
type HashResult struct {
	Success bool
	Message string
}

func getLedgerAccessToken() string {

	cmd := "az"
	args := []string{"account", "get-access-token", "--resource", "https://confidential-ledger.azure.com"}
	out, err := exec.Command(cmd, args...).Output()
	if err != nil {
		return "error"
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(out), &data); err != nil {
		return "error"
	}

	// Extract the access token
	accessToken, ok := data["accessToken"].(string)
	if !ok {
		return "error"
	}

	return accessToken
}

func getIdentityCertificate(ledgerUrl string, client *http.Client) string {
	parts := strings.Split(ledgerUrl, ".")
	if len(parts) < 2 {
		fmt.Println("Invalid URL format")
		return ""
	}
	ledgerName := strings.TrimPrefix(parts[0], "https://")

	identityURL := fmt.Sprintf("https://identity.confidential-ledger.core.azure.com/ledgerIdentity/%s", ledgerName)
	response, err := client.Get(identityURL)
	if err != nil {
		return ""
	}
	defer response.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return ""
	}

	ledgerTlsCertificate, ok := result["ledgerTlsCertificate"].(string)
	if !ok {
		fmt.Println("Error: ledgerTlsCertificate not found in response")
		return ""
	}

	return ledgerTlsCertificate
}

func getStorageAccount(storageLocation string) string {

	re := regexp.MustCompile(`https://([^.]+)\.blob\.core\.windows\.net/([^/]+)(?:/([^/]+))?`)

	matches := re.FindStringSubmatch(storageLocation)

	if len(matches) < 3 {
		return "invalid-format"
	}

	storageAccount := matches[1]
	container := matches[2]

	newString := fmt.Sprintf("%s-%s", storageAccount, container)

	if len(matches) >= 4 && matches[3] != "" {
		subdirectory := matches[3]

		if !strings.Contains(subdirectory, ".") {
			newString = fmt.Sprintf("%s-%s", newString, subdirectory)
		}
	}

	return newString
}

func uploadHash(md5Hasher hash.Hash, tamperProofLocation string, storageDestination string) {

	var ledgerUrl = tamperProofLocation

	certPEM := getIdentityCertificate(ledgerUrl, &http.Client{})
	certPool := x509.NewCertPool()
	if ok := certPool.AppendCertsFromPEM([]byte(certPEM)); !ok {
		return
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs: certPool,
		},
	}

	client := &http.Client{
		Transport: transport,
	}

	headers := map[string]string{
		"Authorization":          "Bearer " + getLedgerAccessToken(),
		"Content-Type":           "application/json",
		"x-ms-client-request-id": uuid.New().String(),
	}

	hashSum := md5Hasher.Sum(nil)

	url := fmt.Sprintf("%s/app/transactions?api-version=0.1-preview&subLedgerId=%s", ledgerUrl, getStorageAccount(storageDestination))

	hashSumString := hex.EncodeToString(hashSum)

	hashSumBytes, err := hex.DecodeString(hashSumString)
	if err != nil {
		return
	}

	hashSumBase64 := base64.StdEncoding.EncodeToString(hashSumBytes)

	var contentString = "{'path': '" + storageDestination + "', 'hash': '" + hashSumBase64 + "'}"

	payload := map[string]interface{}{
		"contents": contentString,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return
	}

	req, err := http.NewRequest("POST", url, strings.NewReader(string(jsonData)))
	if err != nil {
		return
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	response, err := client.Do(req)
	if err != nil {
		return
	}

	defer response.Body.Close()

}

func downloadHash(comparison md5Comparer, tamperProofLocation string, storageSource string) HashResult {

	var ledgerUrl = tamperProofLocation

	certPEM := getIdentityCertificate(ledgerUrl, &http.Client{})

	certPool := x509.NewCertPool()

	if ok := certPool.AppendCertsFromPEM([]byte(certPEM)); !ok {
		return HashResult{}
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs: certPool,
		},
	}

	client := &http.Client{
		Transport: transport,
	}

	headers := map[string]string{
		"Authorization":          "Bearer " + getLedgerAccessToken(),
		"Content-Type":           "application/json",
		"x-ms-client-request-id": uuid.New().String(),
	}

	url := fmt.Sprintf("%s/app/transactions?api-version=0.1-preview&subLedgerId=%s", ledgerUrl, getStorageAccount(storageSource))

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return HashResult{}
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	response, err := client.Do(req)
	if err != nil {
		return HashResult{}
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return HashResult{}
	}

	var responsee Response
	err = json.Unmarshal([]byte(body), &responsee)
	if err != nil {
		return HashResult{}
	}

	var state = responsee.State
	var entries = responsee.Entries

	for state == "Loading" {
		time.Sleep(5 * time.Second)

		response, err := client.Do(req)
		if err != nil {
			return HashResult{}
		}
		defer response.Body.Close()

		body, err := io.ReadAll(response.Body)
		if err != nil {
			return HashResult{}
		}

		var responsee Response
		err = json.Unmarshal([]byte(body), &responsee)
		if err != nil {
			return HashResult{}
		}

		state = responsee.State
		entries = responsee.Entries
	}

	for _, entry := range entries {

		contentsJSON := strings.ReplaceAll(entry.Contents, "'", `"`)

		var contents Contents
		err := json.Unmarshal([]byte(contentsJSON), &contents)
		if err != nil {
			continue
		}

		desiredString := storageSource
		if contents.Path == desiredString {

			aclHash := contents.Hash
			hashSumString := hex.EncodeToString(comparison.expected)
			hashSumBytes, err := hex.DecodeString(hashSumString)
			if err != nil {
				return HashResult{}
			}

			hashSumBase64 := base64.StdEncoding.EncodeToString(hashSumBytes)

			if aclHash != hashSumBase64 {
				var log = "ACL Hash: " + aclHash + " " + "Does Not Match Re-Calculated Hash: " + hashSumBase64
				return HashResult{false, log}
			} else {
				var log = "Re-Calculated Hash: " + hashSumBase64 + " " + "Matches Hash Stored in ACL: " + aclHash
				return HashResult{true, log}
			}
		}
	}

	return HashResult{}
}
