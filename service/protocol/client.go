package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient(baseURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    httpClient,
	}
}

func (c *Client) Register(ctx context.Context, req RegisterRunnerRequest) (RegisterRunnerResponse, error) {
	var resp RegisterRunnerResponse
	err := c.post(ctx, RegisterRunnerPath, req, &resp)
	return resp, err
}

func (c *Client) Heartbeat(ctx context.Context, req HeartbeatRequest) (HeartbeatResponse, error) {
	var resp HeartbeatResponse
	err := c.post(ctx, HeartbeatPath, req, &resp)
	return resp, err
}

func (c *Client) Poll(ctx context.Context, req PollTaskRequest) (PollTaskResponse, error) {
	var resp PollTaskResponse
	err := c.post(ctx, PollTaskPath, req, &resp)
	return resp, err
}

func (c *Client) ReportResult(ctx context.Context, req ReportResultRequest) (ReportResultResponse, error) {
	var resp ReportResultResponse
	err := c.post(ctx, ReportResultPath, req, &resp)
	return resp, err
}

func (c *Client) post(ctx context.Context, path string, body any, out any) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("runner protocol %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
