package ste

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"log"
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

const (
	apiVersion     = "0.1-preview"
	identityURLFmt = "https://identity.confidential-ledger.core.azure.com/ledgerIdentity/%s"
	ledgerURLFmt   = "%s/app/transactions?api-version=%s&subLedgerId=%s"
)

// Global regex for extracting storage account information
var storageRegex = regexp.MustCompile(`https://([^.]+)\.blob\.core\.windows\.net/([^/]+)(?:/([^/]+))?`)

// Create a reusable HTTP client
var httpClient = &http.Client{}

// Utility function to get the ledger access token
func getLedgerAccessToken() (string, error) {
	cmd := "az"
	args := []string{"account", "get-access-token", "--resource", "https://confidential-ledger.azure.com"}
	out, err := exec.Command(cmd, args...).Output()
	if err != nil {
		log.Printf("Failed to execute az account get-access-token: %v", err)
		return "", err
	}

	var data map[string]interface{}
	if err := json.Unmarshal(out, &data); err != nil {
		log.Printf("Failed to unmarshal access token: %v", err)
		return "", err
	}

	accessToken, ok := data["accessToken"].(string)
	if !ok {
		return "", fmt.Errorf("accessToken not found in the response")
	}

	return accessToken, nil
}

// Fetch the ledger identity certificate
func getIdentityCertificate(ledgerUrl string, client *http.Client) (string, error) {
	parts := strings.Split(ledgerUrl, ".")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid URL format")
	}
	ledgerName := strings.TrimPrefix(parts[0], "https://")

	identityURL := fmt.Sprintf(identityURLFmt, ledgerName)
	response, err := client.Get(identityURL)
	if err != nil {
		log.Printf("Failed to fetch identity certificate: %v", err)
		return "", err
	}
	defer response.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		log.Printf("Failed to decode identity certificate response: %v", err)
		return "", err
	}

	ledgerTlsCertificate, ok := result["ledgerTlsCertificate"].(string)
	if !ok {
		return "", fmt.Errorf("ledgerTlsCertificate not found in response")
	}

	return ledgerTlsCertificate, nil
}

// Extract the storage account and subdirectory from the storage location
func getStorageAccount(storageLocation string) (string, error) {
	matches := storageRegex.FindStringSubmatch(storageLocation)
	if len(matches) < 3 {
		return "", fmt.Errorf("invalid storage location format")
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

	return newString, nil
}

// Create an HTTP client with the provided identity certificate
func createHttpClient(certPEM string) (*http.Client, error) {
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM([]byte(certPEM)) {
		return nil, fmt.Errorf("failed to append certs from PEM")
	}

	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: certPool,
			},
		},
	}, nil
}

// Set common request headers
func setRequestHeaders(req *http.Request) error {
	accessToken, err := getLedgerAccessToken()
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-ms-client-request-id", uuid.New().String())
	return nil
}

// Upload a hash to the ledger
func uploadHash(md5Hasher hash.Hash, tamperProofLocation string, storageDestination string) error {
	certPEM, err := getIdentityCertificate(tamperProofLocation, httpClient)
	if err != nil {
		return err
	}

	client, err := createHttpClient(certPEM)
	if err != nil {
		return err
	}

	storageAccount, err := getStorageAccount(storageDestination)
	if err != nil {
		return err
	}

	url := fmt.Sprintf(ledgerURLFmt, tamperProofLocation, apiVersion, storageAccount)
	hashSum := md5Hasher.Sum(nil)
	hashSumBase64 := base64.StdEncoding.EncodeToString(hashSum)

	var contentString = "{'path': '" + storageDestination + "', 'hash': '" + hashSumBase64 + "'}"

	payload := map[string]interface{}{
		"contents": contentString,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, strings.NewReader(string(jsonData)))
	if err != nil {
		return err
	}

	if err := setRequestHeaders(req); err != nil {
		return err
	}

	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		return fmt.Errorf("upload failed: %s", string(body))
	}

	return nil
}

// Download and compare hash from the ledger
func downloadHash(comparison md5Comparer, tamperProofLocation string, storageSource string) (HashResult, error) {
	certPEM, err := getIdentityCertificate(tamperProofLocation, httpClient)
	if err != nil {
		return HashResult{}, err
	}

	client, err := createHttpClient(certPEM)
	if err != nil {
		return HashResult{}, err
	}

	storageAccount, err := getStorageAccount(storageSource)
	if err != nil {
		return HashResult{}, err
	}

	url := fmt.Sprintf(ledgerURLFmt, tamperProofLocation, apiVersion, storageAccount)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return HashResult{}, err
	}

	if err := setRequestHeaders(req); err != nil {
		return HashResult{}, err
	}

	response, err := client.Do(req)
	if err != nil {
		return HashResult{}, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return HashResult{}, err
	}

	var ledgerResponse Response
	if err := json.Unmarshal(body, &ledgerResponse); err != nil {
		return HashResult{}, err
	}

	// Handle long polling for state "Loading"
	for ledgerResponse.State == "Loading" {
		time.Sleep(5 * time.Second)

		response, err := client.Do(req)
		if err != nil {
			return HashResult{}, err
		}
		defer response.Body.Close()

		body, err := io.ReadAll(response.Body)
		if err != nil {
			return HashResult{}, err
		}

		if err := json.Unmarshal(body, &ledgerResponse); err != nil {
			return HashResult{}, err
		}
	}

	for i := len(ledgerResponse.Entries) - 1; i >= 0; i-- {
		entry := ledgerResponse.Entries[i]
		contentsJSON := strings.ReplaceAll(entry.Contents, "'", `"`)
		var contents Contents
		if err := json.Unmarshal([]byte(contentsJSON), &contents); err != nil {
			continue
		}

		if contents.Path == storageSource {
			hashSumBase64 := base64.StdEncoding.EncodeToString(comparison.expected)
			logMessage := fmt.Sprintf("\n\nComparing hash for '%s' in tamper-proof storage.\n", storageSource)
			if contents.Hash != hashSumBase64 {
				logMessage := logMessage + fmt.Sprintf("ACL Hash: %s does not match recalculated Hash: %s", contents.Hash, hashSumBase64)
				fmt.Println(logMessage)
				return HashResult{false, logMessage}, nil
			} else {
				logMessage := logMessage + fmt.Sprintf("Recalculated Hash: %s matches Hash stored in ACL: %s\n", hashSumBase64, contents.Hash)
				fmt.Println(logMessage)
				return HashResult{true, logMessage}, nil
			}
		}
	}

	return HashResult{}, nil
}
