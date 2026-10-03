// Package loopback bridges ttyd's short-lived child process to the long-lived broker.
package loopback

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/cms-lab-core/cms-labs-terminal/internal/broker"
)

const IdentityHeader = "X-CMS-Identity"

type Server struct {
	address  string
	registry *broker.Registry
	logger   *slog.Logger
	server   *http.Server
}

func NewServer(address string, registry *broker.Registry, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	mux := http.NewServeMux()
	server := &Server{address: address, registry: registry, logger: logger}
	mux.HandleFunc("GET /healthz", server.health)
	mux.HandleFunc("POST /v1/targets/{target}/attach", server.attach)
	mux.HandleFunc("PUT /v1/targets/{target}/size", server.resize)
	server.server = &http.Server{
		Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second,
	}
	return server
}

// Listen binds synchronously so ttyd clients cannot race broker startup.
func (s *Server) Listen() (net.Listener, error) {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return nil, fmt.Errorf("listening for ttyd clients on %s: %w", s.address, err)
	}
	return listener, nil
}

func (s *Server) Serve(listener net.Listener) error {
	err := s.server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Shutdown(ctx context.Context) error { return s.server.Shutdown(ctx) }

func (s *Server) health(response http.ResponseWriter, _ *http.Request) {
	response.WriteHeader(http.StatusNoContent)
}

func (s *Server) attach(response http.ResponseWriter, request *http.Request) {
	if err := http.NewResponseController(response).EnableFullDuplex(); err != nil {
		http.Error(response, "full-duplex streaming is unavailable", http.StatusInternalServerError)
		return
	}

	target := request.PathValue("target")
	size := sizeFromRequest(request)
	_, client, history, spec, err := s.registry.Attach(request.Context(), target, size)
	if err != nil {
		http.Error(response, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = client.Close() }()

	identityValue := request.Header.Get(IdentityHeader)
	identity := decodeIdentity(identityValue)
	s.logger.Info("terminal attached", "target", target, "pod", spec.Pod, "mode", spec.Mode,
		"subject", identity.Subject, "username", identity.Username)
	defer s.logger.Info("terminal detached", "target", target,
		"subject", identity.Subject, "username", identity.Username)

	response.Header().Set("Content-Type", "application/octet-stream")
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(http.StatusOK)
	connectedAs := ""
	if identity.Username != "" {
		connectedAs = " as " + identity.Username
	}
	if err = writeAndFlush(response, []byte(fmt.Sprintf(
		"connected: %s (%s/%s)%s\r\n\r\n",
		spec.Label, spec.Namespace, spec.Pod, connectedAs,
	))); err != nil {
		return
	}
	if len(history) > 0 {
		if err = writeAndFlush(response, history); err != nil {
			return
		}
	}

	go func() {
		_, _ = io.Copy(client, request.Body)
	}()

	buffer := make([]byte, 32<<10)
	for {
		n, readErr := client.Read(buffer)
		if n > 0 {
			if err = writeAndFlush(response, buffer[:n]); err != nil {
				return
			}
		}
		if readErr != nil {
			return
		}
	}
}

type workspaceIdentity struct {
	Subject  string `json:"sub"`
	Username string `json:"username"`
}

func decodeIdentity(value string) workspaceIdentity {
	encoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return workspaceIdentity{Username: value}
	}
	var result workspaceIdentity
	if err = json.Unmarshal(encoded, &result); err != nil {
		return workspaceIdentity{Username: value}
	}
	return result
}

func (s *Server) resize(response http.ResponseWriter, request *http.Request) {
	var size broker.Size
	if err := json.NewDecoder(io.LimitReader(request.Body, 1024)).Decode(&size); err != nil || !size.Valid() {
		http.Error(response, "valid cols and rows are required", http.StatusBadRequest)
		return
	}
	if err := s.registry.Resize(request.PathValue("target"), size); err != nil {
		http.Error(response, err.Error(), http.StatusConflict)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func sizeFromRequest(request *http.Request) broker.Size {
	var size broker.Size
	_, _ = fmt.Sscanf(request.URL.Query().Get("size"), "%dx%d", &size.Cols, &size.Rows)
	return size.OrDefault()
}

func writeAndFlush(response http.ResponseWriter, payload []byte) error {
	if _, err := response.Write(payload); err != nil { //nolint:gosec // raw PTY bytes, not an HTML response.
		return err
	}
	return http.NewResponseController(response).Flush()
}
