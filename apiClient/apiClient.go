// Package apiclient to provide methods to send HTTP requests
// to babylon server.
package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

const (
	// DefaultBasePath is the default base path for the API client.
	DefaultBasePath = "/api"
)

// httpUnexpectedStatusCodeError is a custom error.
var errHTTPUnexpectedStatusCode = errors.New("unexpected http status code")
var errHTTPBasePathFormatting = errors.New("error formatting HTTP base path")
var errHTTPBodyUnmarshall = errors.New("errror unmarshalling HTTP response body")
var errHTTPBodyClose = errors.New("error closing io stream for HTTP response body")
var errHTTPBabylonAPI = errors.New("error returned from babylon api")

// APIClient manages all endpoints of the Babylon API.
type APIClient struct {
	// a pointer to the http client to use.
	HTTPClient *http.Client
	// a pointer to the url to be used as a base url for all requests.
	BasePath *url.URL
}

// HTTPUnexpectedStatusCodeError is a error wrapper.
func HTTPUnexpectedStatusCodeError(statusCode int) error {
	return fmt.Errorf("%w, %d", errHTTPUnexpectedStatusCode, statusCode)
}

func HTPBasePathFormattingError(basePath string) error {
	return fmt.Errorf("%w, %s", errHTTPBasePathFormatting, basePath)
}

func HTTPBodyUnmarshallError(baseErr error) error {
	return fmt.Errorf("%w, %w", errHTTPBodyUnmarshall, baseErr)
}

func HTTPBodyCloseError(baseErr error) error {
	return fmt.Errorf("%w, %w", errHTTPBodyClose, baseErr)
}

func HTTPBabylonAPI(errorMsg string) error {
	return fmt.Errorf("%w, %s", errHTTPBabylonAPI, errorMsg)
}

// NewAPIClient creates a new APIClient.
func NewAPIClient(httpClient *http.Client, basePath string) (*APIClient, error) {
	// Use a default http client if none is provided.
	if httpClient == nil {
		httpClient = &http.Client{}
	}

	// Parse the base path URL.
	basePathURL, err := url.Parse(basePath)
	if err != nil {
		return nil, HTPBasePathFormattingError(basePath)
	}

	// Return a new APIClient instance.
	return &APIClient{
		HTTPClient: httpClient,
		BasePath:   basePathURL,
	}, nil
}

// EchoResponse represents the response from the Echo endpoint.
type EchoResponse struct {
	// The value that was echoed back.
	EchoedValue string `json:"value,omitempty"`
}

// HistoryTransaction represents a transaction in the history.
type HistoryTransaction struct {
	// A unique identifier for a transaction.
	ID string `json:"id,omitempty"`
}

// TransactionPutResponse response body from PUT transaction.
type TransactionPutResponse struct {
	TransactionID string `json:"transactionId"`
}

// Transaction represents a single transaction.
type Transaction struct {
	// The type of transaction (ingress or egress).
	TransactionType string `json:"transactionType"`
	// The source of the transaction.
	TransactionSource string `json:"transactionSource"`
	// Date the transaction was posted to an external account.
	DatePosted string `json:"datePosted"`
	// Description of the transaction.
	Description string `json:"description"`
	// Amount posted in the transaction.
	Amount float64 `json:"amount"`
	// Slip number from an external institution.
	SlipNumber string `json:"slipNumber,omitempty"`
}

// TransactionType represents the type of transaction.
type TransactionType string

// UtcTimestamp represents a UNIX UTC timestamp in seconds.
type UtcTimestamp int64

// DebugMessageResponse represents a debug message attached to an HTTP response.
type DebugMessageResponse struct {
	// A message attached to an HTTP response for debugging purposes.
	Message string `json:"message,omitempty"`
}

// TransactionHistorySearchResponse is a list of transactions.
type TransactionHistorySearchResponse struct {
	Transactions []HistoryTransaction `json:"transactions,omitempty"`
}

// DoEcho sends a GET request to the /echo endpoint.
func (c *APIClient) DoEcho(ctx context.Context, inputVal string) (*http.Response, *EchoResponse, error) {
	// Construct the full URL by combining the base path with the endpoint path.
	localVarPath := c.BasePath.String() + "/echo"

	// Create the request.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, localVarPath, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating request: %w", err)
	}

	// Add query parameters.
	q := req.URL.Query()
	q.Add("inputVal", inputVal)
	req.URL.RawQuery = q.Encode()

	// Add Content-Type header.
	req.Header.Add("Content-Type", "application/json")

	// Send the request.
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return resp, nil, fmt.Errorf("error sending request: %w", err)
	}

	err = resp.Body.Close()
	if err != nil {
		return resp, nil, HTTPBodyUnmarshallError(err)
	}

	// Handle error status codes first.
	if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusInternalServerError {
		var debugMsg DebugMessageResponse
		// ... (error handling code) ...
		return resp, nil, HTTPBabylonAPI(debugMsg.Message)
	}
	// Handle unexpected status codes.
	if resp.StatusCode != http.StatusOK {
		return resp, nil, HTTPUnexpectedStatusCodeError(resp.StatusCode)
	}

	// Now handle the successful case (happy path).
	var result EchoResponse

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, fmt.Errorf("error reading response body: %w", err)
	}

	err = json.Unmarshal(body, &result)
	if err != nil {
		return resp, nil, HTTPBodyUnmarshallError(err)
	}

	return resp, &result, nil
}

// GetTransactionByID sends a GET request to the /history/transaction endpoint.
func (c *APIClient) GetTransactionByID(
	ctx context.Context,
	transactionID string,
	transactionType string) (*http.Response, *HistoryTransaction, error) {
	// Use ResolveReference to correctly combine the base URL with the endpoint path.
	localVarPath := c.BasePath.ResolveReference(&url.URL{Path: "/history/transaction"})

	// Add query parameters.
	q := url.Values{}
	q.Add("transactionId", transactionID)
	q.Add("transactionType", transactionType)
	localVarPath.RawQuery = q.Encode()

	// Create the request.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, localVarPath.String(), nil)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating request: %w", err)
	}

	// Add Content-Type header.
	req.Header.Add("Content-Type", "application/json")

	// Send the request.
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return resp, nil, fmt.Errorf("error sending request: %w", err)
	}

	err = resp.Body.Close()
	if err != nil {
		return resp, nil, fmt.Errorf("error closing IO stream for http body: %w", err)
	}

	// Handle response based on status code.
	if resp.StatusCode == http.StatusOK {
		return historyTransactionGetResponseUnmarshall(resp)
	} else if resp.StatusCode >= http.StatusBadRequest {
		var debugMsg DebugMessageResponse

		//nolint:govet // For now its fine to redeclare the err. We return it back either way.
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return resp, nil, fmt.Errorf("error reading response body for error: %w", err)
		}

		err = json.Unmarshal(body, &debugMsg)
		if err != nil {
			return resp, nil, fmt.Errorf("error unmarshaling error response body: %w", err)
		}

		return resp, nil, HTTPBabylonAPI(debugMsg.Message)
	}

	return resp, nil, HTTPUnexpectedStatusCodeError(resp.StatusCode)
}

// AddTransaction sends a PUT request to the /history/transaction endpoint.
func (c *APIClient) AddTransaction(
	ctx context.Context,
	transaction Transaction) (*http.Response, *TransactionPutResponse, error) {
	// Marshal the request body.
	bodyBytes, err := json.Marshal(transaction)
	if err != nil {
		return nil, nil, fmt.Errorf("error marshaling request body: %w", err)
	}

	// Construct the full URL by combining the base path with the endpoint path.
	localVarPath := c.BasePath.String() + "/history/transaction"

	// Create the request.
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, localVarPath, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, nil, fmt.Errorf("error creating request: %w", err)
	}

	// Add Content-Type header.
	req.Header.Add("Content-Type", "application/json")

	// Send the request.
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return resp, nil, fmt.Errorf("error sending request: %w", err)
	}

	err = resp.Body.Close()
	if err != nil {
		return resp, nil, HTTPBodyCloseError(err)
	}

	defer resp.Body.Close()

	// Handle response based on status code.
	if resp.StatusCode == http.StatusCreated {
		return transactionPutResponseUnmarshall(resp)
	} else if resp.StatusCode >= http.StatusBadRequest {
		var debugMsg DebugMessageResponse

		//nolint:govet // For now its fine to redeclare the err. We return it back either way.
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return resp, nil, fmt.Errorf("error reading response body for error: %w", err)
		}

		err = json.Unmarshal(body, &debugMsg)
		if err != nil {
			return resp, nil, HTTPBodyUnmarshallError(err)
		}

		return resp, nil, HTTPBabylonAPI(debugMsg.Message)
	}

	return resp, nil, HTTPUnexpectedStatusCodeError(resp.StatusCode)
}

// GetTransactionHistory sends a GET request to the /history/transactions/{transactionType} endpoint.
func (c *APIClient) GetTransactionHistory(
	ctx context.Context,
	transactionType string,
	start,
	end int64) (*http.Response, *TransactionHistorySearchResponse, error) {
	// Use ResolveReference to correctly combine the base URL with the endpoint path.
	localVarPath := c.BasePath.ResolveReference(
		&url.URL{Path: "/history/transactions/" + url.PathEscape(transactionType)})

	// Add query parameters.
	q := url.Values{}
	q.Add("start", strconv.FormatInt(start, 10))
	q.Add("end", strconv.FormatInt(end, 10))
	localVarPath.RawQuery = q.Encode()

	// Create the request.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, localVarPath.String(), nil)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating request: %w", err)
	}

	// Add Content-Type header.
	req.Header.Add("Content-Type", "application/json")

	// Send the request.
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return resp, nil, fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	// Handle response based on status code.
	if resp.StatusCode == http.StatusOK {
		return historySearchResponseUnmarshall(resp)
	} else if resp.StatusCode >= http.StatusBadRequest {
		var debugMsg DebugMessageResponse

		//nolint:govet // For now its fine to redeclare the err. We return it back either way.
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return resp, nil, fmt.Errorf("error reading response body for error: %w", err)
		}

		err = json.Unmarshal(body, &debugMsg)
		if err != nil {
			return resp, nil, fmt.Errorf("error unmarshaling error response body: %w", err)
		}

		return resp, nil, HTTPBabylonAPI(debugMsg.Message)
	}

	return resp, nil, HTTPUnexpectedStatusCodeError(resp.StatusCode)
}

// historySearchResponseUnmarshall validates the http response and unmarshalls the result.
// Return an error if one exists.
func historySearchResponseUnmarshall(
	resp *http.Response) (*http.Response, *TransactionHistorySearchResponse, error) {
	var result TransactionHistorySearchResponse

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, fmt.Errorf("error reading response body: %w", err)
	}

	err = json.Unmarshal(body, &result)
	if err != nil {
		return resp, nil, fmt.Errorf("error unmarshaling response body: %w", err)
	}

	return resp, &result, nil
}

// transactionPutResponseUnmarshall validates the http response and unmarshalls the result.
// Return an error if one exists.
func transactionPutResponseUnmarshall(
	resp *http.Response) (*http.Response, *TransactionPutResponse, error) {
	var result TransactionPutResponse

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, fmt.Errorf("error reading response body: %w", err)
	}

	err = json.Unmarshal(body, &result)
	if err != nil {
		return resp, nil, fmt.Errorf("error unmarshaling response body: %w", err)
	}

	return resp, &result, nil
}

// historyTransactionGetResponseUnmarshall validates the http response and unmarshalls the result.
// Return an error if one exists.
func historyTransactionGetResponseUnmarshall(
	resp *http.Response) (*http.Response, *HistoryTransaction, error) {
	var result HistoryTransaction

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, fmt.Errorf("error reading response body: %w", err)
	}

	err = json.Unmarshal(body, &result)
	if err != nil {
		return resp, nil, fmt.Errorf("error unmarshaling response body: %w", err)
	}

	return resp, &result, nil
}
