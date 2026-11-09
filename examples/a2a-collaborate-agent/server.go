// Copyright 2026 The A2A Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collagent

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/anthropics/anthropic-sdk-go"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/a2aproject/a2a-go/v2/a2a"
	a2agrpc "github.com/a2aproject/a2a-go/v2/a2agrpc/v1"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// Run starts a collaborative agent server. role selects the specialist persona
// ("orders", "inventory", "shipping") or "" for the generic agent.
func Run(name string, grpcPort, httpPort int, model string, debug bool, role string) {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		log.Fatal("ANTHROPIC_API_KEY environment variable is not set")
	}

	llmClient := anthropic.NewClient()

	fanIn := NewFanInHandler()
	exec := &CollabAgentExecutor{
		name:  name,
		llm:   &llmClient,
		model: model,
		fanIn: fanIn,
		debug: debug,
		role:  role,
	}

	card := buildAgentCard(name, httpPort, grpcPort, role)
	inner := a2asrv.NewHandler(exec, a2asrv.WithExtendedAgentCard(card))
	fanIn.SetInner(inner)

	grpcHandler := a2agrpc.NewHandler(fanIn)

	var g errgroup.Group
	g.Go(func() error {
		return serveGRPC(grpcPort, grpcHandler)
	})
	g.Go(func() error {
		return serveHTTP(httpPort, card)
	})

	log.Printf("[%s] starting — gRPC :%d  HTTP :%d  model %s  role %q", name, grpcPort, httpPort, model, role)

	if err := g.Wait(); err != nil {
		log.Fatalf("server shutdown: %v", err)
	}
}

func buildAgentCard(name string, httpPort, grpcPort int, role string) *a2a.AgentCard {
	grpcURL := fmt.Sprintf("localhost:%d", grpcPort)
	description := "Collaborative group-chat agent — participates in A2A collaborative task sessions"
	if role != "" {
		description = fmt.Sprintf("Specialist %s agent — participates in A2A collaborative task sessions", role)
	}
	return &a2a.AgentCard{
		Name:        name,
		Description: description,
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(grpcURL, a2a.TransportProtocolGRPC),
		},
		DefaultInputModes:  []string{"text"},
		DefaultOutputModes: []string{"text"},
		Capabilities: a2a.AgentCapabilities{
			Streaming: true,
			Extensions: []a2a.AgentExtension{
				{URI: "https://a2a-protocol.org/extensions/collaborative-task/v1"},
				{URI: "https://a2a-protocol.org/extensions/shared-task/v1"},
			},
		},
		Skills: []a2a.AgentSkill{
			{
				ID:          "collaborate",
				Name:        "Collaborate",
				Description: "Participates in a multi-agent collaborative task session",
				Tags:        []string{"collaborate", "group-chat"},
			},
		},
	}
}

func serveGRPC(port int, handler *a2agrpc.Handler) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("gRPC listen on :%d: %w", port, err)
	}
	log.Printf("gRPC server listening on :%d", port)
	s := grpc.NewServer()
	handler.RegisterWith(s)
	return s.Serve(lis)
}

func serveHTTP(port int, card *a2a.AgentCard) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("HTTP listen on :%d: %w", port, err)
	}
	log.Printf("HTTP card server listening on :%d", port)
	mux := http.NewServeMux()
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	return http.Serve(lis, mux)
}
