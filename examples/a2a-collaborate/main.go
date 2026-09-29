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
	"google.golang.org/protobuf/types/known/structpb"
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
				if agent.taskID != "" {
					req.Message.TaskID = agent.taskID
				}
				if agent.contextID != "" {
					req.Message.ContextID = agent.contextID
				}
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
			agent.taskID = task.ID
			agent.contextID = task.ContextID
			close(agent.taskReady)
		}

		output <- fmt.Sprintf("[%s]: %v", agent.name, event)

		relayReq := translateEventToRequest(event, agent.name)
		if relayReq != nil {
			if agent.taskID != "" {
				relayReq.Message.TaskID = agent.taskID
			}
			if agent.contextID != "" {
				relayReq.Message.ContextID = agent.contextID
			}

			for j, other := range r.agents {
				if j != idx {
					select {
					case other.queue <- relayReq:
					case <-r.ctx.Done():
						return
					}
				}
			}
		}
	}
}

func translateEventToRequest(event a2a.Event, sender string) *a2a.SendMessageRequest {
	msg := &a2a.Message{
		Role: a2a.MessageRoleUser,
	}

	switch e := event.(type) {
	case *a2a.Task:
		val, _ := structpb.NewValue(e)
		msg.Parts = append(msg.Parts, &a2a.Part{
			Content: &a2a.Data{Value: val},
			MediaType: "application/vnd.a2a.task+json",
		})
	case *a2a.TaskStatusUpdateEvent:
		val, _ := structpb.NewValue(e)
		msg.Parts = append(msg.Parts, &a2a.Part{
			Content: &a2a.Data{Value: val},
			MediaType: "application/vnd.a2a.task-status-update+json",
		})
	// case *a2a.TaskMessageUpdateEvent: // Not added to SDK yet
	//     val, _ := structpb.NewValue(e)
	//     msg.Parts = append(msg.Parts, &a2a.Part{
	//         Content: &a2a.Data{Value: val},
	//         MediaType: "application/vnd.a2a.task-message-update+json",
	//     })
	case *a2a.TaskArtifactUpdateEvent:
		// If the SDK has a specific field in SendMessageRequest, we use it.
		// Otherwise, we treat it as a special message.
		val, _ := structpb.NewValue(e)
		msg.Parts = append(msg.Parts, &a2a.Part{
			Content: &a2a.Data{Value: val},
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
		card, err := resolver.Resolve(ctx, ref, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to resolve card %s: %v\n", ref, err)
			os.Exit(1)
		}

		client, err := a2aclient.NewFromCard(ctx, card, nil)
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
