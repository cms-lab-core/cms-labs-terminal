package loopback

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/term"

	"github.com/cms-lab-core/cms-labs-terminal/internal/broker"
)

type Client struct {
	Address  string
	Target   string
	Identity string
	Input    io.Reader
	Output   io.Writer
	FD       int
	HTTP     *http.Client
}

func (c *Client) Run(ctx context.Context) error {
	if c.Input == nil {
		c.Input = os.Stdin
	}
	if c.Output == nil {
		c.Output = os.Stdout
	}
	if c.FD == 0 {
		c.FD = int(os.Stdin.Fd()) //nolint:gosec // stdin is an OS-assigned process descriptor and fits ioctl's int API.
	}
	if c.HTTP == nil {
		c.HTTP = &http.Client{Transport: &http.Transport{DisableCompression: true}}
	}
	if term.IsTerminal(c.FD) {
		state, err := term.MakeRaw(c.FD)
		if err != nil {
			return fmt.Errorf("putting ttyd terminal into raw mode: %w", err)
		}
		defer func() { _ = term.Restore(c.FD, state) }()
	}

	size := terminalSize(c.FD)
	endpoint := fmt.Sprintf(
		"http://%s/v1/targets/%s/attach?size=%dx%d",
		c.Address, url.PathEscape(c.Target), size.Cols, size.Rows,
	)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, c.Input)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	if c.Identity != "" {
		request.Header.Set(IdentityHeader, c.Identity)
	}

	response, err := c.HTTP.Do(request)
	if err != nil {
		return fmt.Errorf("attaching to broker: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("broker returned %s: %s", response.Status, bytes.TrimSpace(message))
	}

	go c.forwardResize(ctx)
	_, err = io.Copy(c.Output, response.Body)
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("reading terminal stream: %w", err)
	}
	return nil
}

func (c *Client) forwardResize(ctx context.Context) {
	changes := make(chan os.Signal, 1)
	signal.Notify(changes, syscall.SIGWINCH)
	defer signal.Stop(changes)
	for {
		select {
		case <-ctx.Done():
			return
		case <-changes:
			_ = c.resize(ctx, terminalSize(c.FD))
		}
	}
}

func (c *Client) resize(ctx context.Context, size broker.Size) error {
	payload, err := json.Marshal(size)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("http://%s/v1/targets/%s/size", c.Address, url.PathEscape(c.Target))
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.HTTP.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("resize returned %s", response.Status)
	}
	return nil
}

func terminalSize(fd int) broker.Size {
	cols, rows, err := term.GetSize(fd)
	if err != nil {
		return broker.DefaultSize
	}
	return (broker.Size{Cols: cols, Rows: rows}).OrDefault()
}
