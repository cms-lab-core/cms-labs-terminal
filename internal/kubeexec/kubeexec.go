// Package kubeexec owns Kubernetes exec/attach streams used by the broker.
package kubeexec

import (
	"context"
	"fmt"
	"io"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/streaming/pkg/httpstream"

	"github.com/maintainer64/cms-labs-terminal/internal/broker"
)

type terminalSizeQueue struct {
	updates chan remotecommand.TerminalSize
	mu      sync.Mutex
	closed  bool
}

func newTerminalSizeQueue(initial broker.Size) *terminalSizeQueue {
	queue := &terminalSizeQueue{updates: make(chan remotecommand.TerminalSize, 4)}
	queue.Resize(initial)
	return queue
}

func (q *terminalSizeQueue) Resize(size broker.Size) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	size = size.OrDefault()
	update := remotecommand.TerminalSize{
		Width:  uint16(size.Cols), //nolint:gosec // OrDefault bounds both dimensions to uint16.
		Height: uint16(size.Rows), //nolint:gosec // OrDefault bounds both dimensions to uint16.
	}
	select {
	case q.updates <- update:
	default:
		// Keep resize non-blocking. The next browser resize supersedes a stale queued value.
		select {
		case <-q.updates:
		default:
		}
		select {
		case q.updates <- update:
		default:
		}
	}
}

func (q *terminalSizeQueue) Next() *remotecommand.TerminalSize {
	size, ok := <-q.updates
	if !ok {
		return nil
	}
	return &size
}

func (q *terminalSizeQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.updates)
	}
}

// Device opens streams through the Kubernetes API. It has no filesystem or process dependency in a
// target container: exec starts exactly the configured argv, while attach joins the container's
// existing stdin/stdout stream.
type Device struct {
	client kubernetes.Interface
	config *rest.Config
}

func New(client kubernetes.Interface, restConfig *rest.Config) *Device {
	return &Device{client: client, config: restConfig}
}

func (d *Device) Start(ctx context.Context, spec broker.Spec, size broker.Size) (broker.Process, error) {
	if spec.Namespace == "" || spec.Pod == "" || spec.Container == "" {
		return nil, fmt.Errorf("namespace, pod and container are required")
	}

	request := d.client.CoreV1().RESTClient().Post().Resource("pods").
		Namespace(spec.Namespace).Name(spec.Pod)

	switch spec.Mode {
	case broker.ModeExec:
		if len(spec.Command) == 0 {
			return nil, fmt.Errorf("exec target has no command")
		}
		request = request.SubResource("exec").VersionedParams(&corev1.PodExecOptions{
			Container: spec.Container,
			Command:   spec.Command,
			Stdin:     true,
			Stdout:    true,
			Stderr:    false,
			TTY:       true,
		}, scheme.ParameterCodec)

	case broker.ModeAttach:
		request = request.SubResource("attach").VersionedParams(&corev1.PodAttachOptions{
			Container: spec.Container,
			Stdin:     true,
			Stdout:    true,
			Stderr:    false,
			TTY:       true,
		}, scheme.ParameterCodec)

	default:
		return nil, fmt.Errorf("unsupported connection mode %q", spec.Mode)
	}

	spdyExecutor, err := remotecommand.NewSPDYExecutor(d.config, "POST", request.URL())
	if err != nil {
		return nil, fmt.Errorf("preparing %s for %s/%s: %w", spec.Mode, spec.Namespace, spec.Pod, err)
	}
	websocketExecutor, err := remotecommand.NewWebSocketExecutor(d.config, "GET", request.URL().String())
	if err != nil {
		return nil, fmt.Errorf("preparing websocket %s for %s/%s: %w", spec.Mode, spec.Namespace, spec.Pod, err)
	}
	executor, err := remotecommand.NewFallbackExecutor(websocketExecutor, spdyExecutor, func(err error) bool {
		return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
	})
	if err != nil {
		return nil, fmt.Errorf("preparing transport fallback for %s/%s: %w", spec.Namespace, spec.Pod, err)
	}

	streamContext, cancel := context.WithCancel(ctx)
	inputReader, inputWriter := io.Pipe()
	outputReader, outputWriter := io.Pipe()
	queue := newTerminalSizeQueue(size)
	process := &process{
		input: inputWriter, output: outputReader, cancel: cancel, sizes: queue,
	}

	go func() {
		err := executor.StreamWithContext(streamContext, remotecommand.StreamOptions{
			Stdin: inputReader, Stdout: outputWriter, Tty: true, TerminalSizeQueue: queue,
		})
		queue.Close()
		_ = inputReader.CloseWithError(err)
		if err != nil && streamContext.Err() == nil {
			err = fmt.Errorf("%s stream for %s/%s ended: %w", spec.Mode, spec.Namespace, spec.Pod, err)
		} else if streamContext.Err() != nil {
			err = io.EOF
		}
		_ = outputWriter.CloseWithError(err)
		cancel()
	}()

	return process, nil
}

type process struct {
	input  *io.PipeWriter
	output *io.PipeReader
	cancel context.CancelFunc
	sizes  *terminalSizeQueue
	once   sync.Once
}

func (p *process) Read(buffer []byte) (int, error)  { return p.output.Read(buffer) }
func (p *process) Write(buffer []byte) (int, error) { return p.input.Write(buffer) }

func (p *process) Resize(size broker.Size) error {
	p.sizes.Resize(size)
	return nil
}

func (p *process) Close() error {
	p.once.Do(func() {
		p.cancel()
		_ = p.input.Close()
		_ = p.output.Close()
	})
	return nil
}
