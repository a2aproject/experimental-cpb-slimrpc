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

// Command orders starts an e-commerce Orders specialist agent. It can look up
// order details, list orders by customer, and retrieve tracking IDs for shipped
// orders. See the parent package for the scenario data.
package main

import (
	"flag"

	collagent "github.com/a2aproject/experimental-cpb-slimrpc/examples/a2a-collaborate-agent"
)

func main() {
	name := flag.String("name", "orders-agent", "Agent name shown in the group channel")
	grpcPort := flag.Int("grpc-port", 50052, "Port for the gRPC A2A server")
	httpPort := flag.Int("http-port", 8081, "Port for the HTTP agent-card server")
	model := flag.String("model", "claude-haiku-4-5-20251001", "Anthropic model to use")
	debug := flag.Bool("debug", false, "Log LLM reasoning, tool calls, and inbox messages")
	flag.Parse()

	collagent.Run(*name, *grpcPort, *httpPort, *model, *debug, "orders")
}
