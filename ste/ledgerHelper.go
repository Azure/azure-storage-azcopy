package ste

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io/ioutil"
	"net/http"
	"os/exec"
	"regexp"
	"strings"
	"time"
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

// Response represents the structure of the JSON response
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

func getStorageAccount(storageLocation string) string {
	// Define regular expression pattern to extract storage account and container
	re := regexp.MustCompile(`https://([^.]+)\.blob\.core\.windows\.net/([^/]+)`)

	// Find matches
	matches := re.FindStringSubmatch(storageLocation)

	// Check if matches were found
	var newString string
	if len(matches) >= 3 {
		// Extract storage account and container from the matches
		storageAccount := matches[1]
		container := matches[2]

		// Create new string with the format "storage-account-container"
		newString = fmt.Sprintf("%s-%s", storageAccount, container)
	}

	return newString
}

func uploadHash(md5Hasher hash.Hash, tamperProofLocation string, storageDestination string) {

	// Create a custom Transport with InsecureSkipVerify set to true
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	// Create a custom HTTP client using the custom Transport
	client := &http.Client{
		Transport: transport,
	}

	headers := map[string]string{
		"Authorization":          "Bearer " + getLedgerAccessToken(),
		"Content-Type":           "application/json",
		"x-ms-client-request-id": "123456789",
	}

	hashSum := md5Hasher.Sum(nil)

	var ledgerUrl = tamperProofLocation

	url := fmt.Sprintf("%s/app/transactions?api-version=0.1-preview&subLedgerId=%s", ledgerUrl, getStorageAccount(storageDestination))

	// Convert hashSumBytes to hexadecimal string
	hashSumString := hex.EncodeToString(hashSum)

	// Decode hexadecimal string to bytes
	hashSumBytes, err := hex.DecodeString(hashSumString)
	if err != nil {
		return
	}

	// Encode bytes to base64
	hashSumBase64 := base64.StdEncoding.EncodeToString(hashSumBytes)

	var contentString = "{'path': '" + storageDestination + "', 'hash': '" + hashSumBase64 + "'}"

	// Create a map for the payload
	payload := map[string]interface{}{
		"contents": contentString,
	}

	// Marshal the payload into JSON
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return
	}

	req, err := http.NewRequest("POST", url, strings.NewReader(string(jsonData)))
	if err != nil {
		// fmt.Printf("Failed to create request: %v\n", err)
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

	// Create a custom Transport with InsecureSkipVerify set to true
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	// Create a custom HTTP client using the custom Transport
	client := &http.Client{
		Transport: transport,
	}

	headers := map[string]string{
		"Authorization":          "Bearer " + getLedgerAccessToken(),
		"Content-Type":           "application/json",
		"x-ms-client-request-id": "123456789",
	}

	var ledgerUrl = tamperProofLocation

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

	// Read response body
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return HashResult{}
	}

	// Unmarshal the JSON response into a Response struct
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

		// Read response body
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return HashResult{}
		}

		// Unmarshal the JSON response into a Response struct
		var responsee Response
		err = json.Unmarshal([]byte(body), &responsee)
		if err != nil {
			// fmt.Println("Error unmarshaling JSON:", err)
			return HashResult{}
		}

		state = responsee.State
		entries = responsee.Entries
	}

	// Iterate through the entries
	for _, entry := range entries {

		contentsJSON := strings.ReplaceAll(entry.Contents, "'", `"`)

		// Unmarshal the contents into a Contents struct
		var contents Contents
		err := json.Unmarshal([]byte(contentsJSON), &contents)
		if err != nil {
			fmt.Println("Error unmarshaling contents:", err)
			continue
		}

		// Compare the path value with your desired string
		desiredString := storageSource
		var aclHash = ""

		if contents.Path == desiredString {
			aclHash = contents.Hash
			// Convert hashSumBytes to hexadecimal string
			hashSumString := hex.EncodeToString(comparison.expected)

			// Decode hexadecimal string to bytes
			hashSumBytes, err := hex.DecodeString(hashSumString)
			if err != nil {
				return HashResult{}
			}

			// Encode bytes to base64
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
