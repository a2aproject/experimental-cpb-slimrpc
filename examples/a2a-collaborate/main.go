package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/a2aproject/a2a-cli/devkit/cliplugin"
	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
	a2agrpc "github.com/a2aproject/a2a-go/v2/a2agrpc/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"encoding/json"
)

type Relay struct {
	agents []agentClient
	ctx    context.Context
	cancel context.CancelFunc
}

type agentClient struct {
	name      string
	client    *a2aclient.Client
	taskID    a2a.TaskID
	contextID string
	queue     chan *a2a.SendMessageRequest
	taskReady chan struct{}
}

func NewRelay(ctx context.Context, agents []agentClient) *Relay {
	ctx, cancel := context.WithCancel(ctx)
	return &Relay{
		agents: agents,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (r *Relay) Run(initialMsg string) {
	var wg sync.WaitGroup
	output := make(chan string, 100)

	for i := range r.agents {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r.readAndRelay(idx, output)
		}(i)
	}

	go func() {
		if initialMsg != "" {
			r.Broadcast(initialMsg)
		}
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			text := scanner.Text()
			r.Broadcast(text)
		}
	}()

	go func() {
		for msg := range output {
			fmt.Println(msg)
		}
	}()

	<-r.ctx.Done()
	close(output)
	wg.Wait()
}

func (r *Relay) Broadcast(text string) {
	for _, agent := range r.agents {
		req := &a2a.SendMessageRequest{
			Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(text)),
		}
		if agent.taskID != "" {
			req.Message.TaskID = agent.taskID
		}
		if agent.contextID != "" {
			req.Message.ContextID = agent.contextID
		}
		select {
		case agent.queue <- req:
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Relay) readAndRelay(idx int, output chan<- string) {
	agent := r.agents[idx]

	var firstReq *a2a.SendMessageRequest
	select {
	case firstReq = <-agent.queue:
	case <-r.ctx.Done():
		return
	}

	go func() {
		<-agent.taskReady
		for {
			select {
			case req := <-agent.queue:
				// task ID and context ID are already set correctly at enqueue time
				_, err := agent.client.SendMessage(r.ctx, req)
				if err != nil {
					output <- fmt.Sprintf("[System]: relay error to %s: %v", agent.name, err)
				}
			case <-r.ctx.Done():
				return
			}
		}
	}()

	streamIter := agent.client.SendStreamingMessage(r.ctx, firstReq)
	for event, err := range streamIter {
		if err != nil {
			output <- fmt.Sprintf("[System]: error from %s: %v", agent.name, err)
			return
		}

		if task, ok := event.(*a2a.Task); ok {
			r.agents[idx].taskID = task.ID
			r.agents[idx].contextID = task.ContextID
			close(agent.taskReady)
		}

		if b, err := json.Marshal(event); err == nil {
			output <- fmt.Sprintf("[%s]: %s", agent.name, b)
		}

		relayReq := translateEventToRequest(event, agent.name)
		if relayReq != nil {
			for j, other := range r.agents {
				if j != idx {
					// Clone per target so each gets its own task/context IDs.
					targetReq := cloneRequestForAgent(relayReq, &r.agents[j])
					output <- fmt.Sprintf("[relay] %s → %s (%s)", agent.name, other.name, eventKind(event))
					select {
					case other.queue <- targetReq:
					case <-r.ctx.Done():
						return
					}
				}
			}
		}
	}
}

// cloneRequestForAgent copies a relay request and stamps it with the target
// agent's task ID and context ID so each target gets an independent struct.
func cloneRequestForAgent(src *a2a.SendMessageRequest, target *agentClient) *a2a.SendMessageRequest {
	msgCopy := *src.Message
	msgCopy.ID = a2a.NewMessageID()
	msgCopy.TaskID = target.taskID
	msgCopy.ContextID = target.contextID
	return &a2a.SendMessageRequest{Message: &msgCopy}
}

func eventKind(e a2a.Event) string {
	switch e.(type) {
	case *a2a.Task:
		return "task"
	case *a2a.TaskStatusUpdateEvent:
		return "status-update"
	case *a2a.TaskArtifactUpdateEvent:
		return "artifact-update"
	default:
		return "event"
	}
}

func toMap(v any) map[string]any {
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil
	}
	return m
}

func translateEventToRequest(event a2a.Event, sender string) *a2a.SendMessageRequest {
	msg := &a2a.Message{
		ID:       a2a.NewMessageID(),
		Role:     a2a.MessageRoleUser,
		Metadata: map[string]any{
			"https://a2a-protocol.org/extensions/shared-task/v1": map[string]any{
				"message-sender": sender,
			},
		},
	}

	switch e := event.(type) {
	case *a2a.Task:
		m := toMap(e)
		if m == nil {
			return nil
		}
		msg.Parts = append(msg.Parts, &a2a.Part{
			Content:   a2a.Data{Value: m},
			MediaType: "application/vnd.a2a.task+json",
		})
	case *a2a.TaskStatusUpdateEvent:
		m := toMap(e)
		if m == nil {
			return nil
		}
		msg.Parts = append(msg.Parts, &a2a.Part{
			Content:   a2a.Data{Value: m},
			MediaType: "application/vnd.a2a.task-status-update+json",
		})
	case *a2a.TaskArtifactUpdateEvent:
		m := toMap(e)
		if m == nil {
			return nil
		}
		msg.Parts = append(msg.Parts, &a2a.Part{
			Content:   a2a.Data{Value: m},
			MediaType: "application/vnd.a2a.task-artifact-update+json",
		})
	default:
		return nil
	}

	return &a2a.SendMessageRequest{Message: msg}
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == cliplugin.SubcommandInfo {
		cliplugin.ServeInfo(cliplugin.Info{
			Name:        "collaborate",
			Version:     "0.1.0",
			Description: "Initiate a collaborative session with multiple A2A agents",
		})
		return
	}

	fs := flag.NewFlagSet("collaborate", flag.ExitOnError)
	var agentCards []string
	fs.Func("agent-card", "Agent card reference", func(s string) error {
		agentCards = append(agentCards, s)
		return nil
	})

	args := os.Args[1:]
	if len(args) > 0 && args[0] == "collaborate" {
		args = args[1:]
	}
	fs.Parse(args)
	initialMsg := fs.Arg(0)

	if len(agentCards) == 0 {
		fmt.Fprintln(os.Stderr, "error: at least one --agent-card is required")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resolver := agentcard.NewResolver(&http.Client{Timeout: 30 * time.Second})
	var agents []agentClient

	for _, ref := range agentCards {
		card, err := resolver.Resolve(ctx, ref)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to resolve card %s: %v\n", ref, err)
			os.Exit(1)
		}

		client, err := a2aclient.NewFromCard(ctx, card,
			a2agrpc.WithGRPCTransport(grpc.WithTransportCredentials(insecure.NewCredentials())),
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create client for %s: %v\n", card.Name, err)
			os.Exit(1)
		}

		agents = append(agents, agentClient{
			name:      card.Name,
			client:    client,
			queue:     make(chan *a2a.SendMessageRequest, 100),
			taskReady: make(chan struct{}),
		})
	}

	relay := NewRelay(ctx, agents)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go relay.Run(initialMsg)

	<-sigChan
	fmt.Println("\nClosing session...")
	relay.cancel()
}
