package retry_mechanism

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

// retrialOperator implements a simple retrial execution policy.
func retrialOperator(err error, fn func() error, maxRetries int) error {
	retryCount := 0
	for err != nil || retryCount < maxRetries {
		err = fn()

		retryCount++
	}

	return err
}

const maxRetrials = 5

func AnyFunc(arguments ...interface{}) (int, error) {
	return 0, nil
}

func testRetrialOperator() {
	var1 := 1
	var2 := 2
	fn := func() error {
		_, err := AnyFunc(var1, var2)

		return err
	}
	err := fmt.Errorf("any-error")
	for err != nil {
		err = retrialOperator(
			err,
			fn,
			maxRetrials,
		)
	}

	hc := NewHTTPClient(maxRetrials)

	fnArgs := &FnArgs{
		URL:         "minha-rola.com",
		ContentType: "pirocóptero/voador",
		Body:        io.NopCloser(bytes.NewReader([]byte("minha piroca alada"))),
	}
	resp, err := hc.Post(fnArgs)
	_ = resp
}

type (
	HTTPClient interface {
		Post(fnArgs *FnArgs) (*http.Response, error)
		Do(req *http.Request) (*http.Response, error)
	}

	httpClient struct {
		maxRetries int
		client     *http.Client
	}

	FnArgs struct {
		URL         string
		ContentType string
		Body        io.Reader
	}
)

func NewHTTPClient(maxRetries int) HTTPClient {
	return &httpClient{
		maxRetries: maxRetries,
		client:     http.DefaultClient,
	}
}

// retrialOperator implements a simple retrial execution policy.
func (c *httpClient) retrialHTTPOperator(
	err error,
	post func(url string, contentType string, body io.Reader) (*http.Response, error),
	fnArgs *FnArgs,
) (*http.Response, error) {
	retryCount := 0
	resp := new(http.Response)
	for err != nil ||
		retryCount < c.maxRetries ||
		resp != nil && resp.StatusCode != http.StatusOK {
		resp, err = post(fnArgs.URL, fnArgs.ContentType, fnArgs.Body)

		retryCount++
	}

	return resp, err
}

// retrialHTTPDoOperator implements a simple retrial execution policy.
func (c *httpClient) retrialHTTPDoOperator(
	err error,
	do func(*http.Request) (*http.Response, error),
	req *http.Request,
) (*http.Response, error) {
	retryCount := 0
	resp := new(http.Response)
	for err != nil ||
		retryCount < c.maxRetries ||
		resp != nil && resp.StatusCode != http.StatusOK {
		resp, err = do(req)

		retryCount++
	}

	return resp, err
}

func (c *httpClient) Post(fnArgs *FnArgs) (*http.Response, error) {
	if err := c.requestValidator(fnArgs); err != nil {
		return nil, fmt.Errorf("failed to validate post argumnents: %v", err)
	}

	resp, err := c.client.Post(fnArgs.URL, fnArgs.ContentType, fnArgs.Body)
	for err != nil {
		resp, err = c.retrialHTTPOperator(err, c.client.Post, fnArgs)
	}

	return resp, err
}

func (c *httpClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.client.Do(req)
	for err != nil {
		resp, err = c.retrialHTTPDoOperator(err, c.client.Do, req)
	}

	return resp, err
}

func (c *httpClient) requestValidator(fnArgs *FnArgs) error {
	if fnArgs.URL == "" {
		return fmt.Errorf("url cannot be empty")
	}

	return nil
}
