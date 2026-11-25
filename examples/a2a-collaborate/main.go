package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
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

const (
	sharedTaskExt    = "https://a2a-protocol.org/extensions/shared-task/v1"
	collabTaskExt    = "https://a2a-protocol.org/extensions/collaborative-task/v1"
	historyMediaType = "application/vnd.a2a.collaborative-history+json"
	taskMediaType    = "application/vnd.a2a.task+json"
	clientSender     = "client"
)

type Relay struct {
	// mu guards agents and history.
	mu     sync.Mutex
	agents []*agentClient
	// history has each message that the relay delivered, in delivery order.
	// The task and context IDs are empty, because they are different for
	// each receiving agent.
	history []*a2a.Message
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	output  chan string
}

type agentClient struct {
	name      string
	client    *a2aclient.Client
	taskID    a2a.TaskID
	contextID string
	queue     []*a2a.SendMessageRequest // guarded by Relay.mu
	wake      chan struct{}
	taskReady chan struct{}
	readyOnce sync.Once
	lateJoin  bool
	joined    bool
}

func NewRelay(ctx context.Context, agents []*agentClient) *Relay {
	ctx, cancel := context.WithCancel(ctx)
	return &Relay{
		agents: agents,
		ctx:    ctx,
		cancel: cancel,
		output: make(chan string, 100),
	}
}

func (r *Relay) Run(initialMsg string, resolve func(ref string) (*agentClient, error)) {
	for _, agent := range r.agents {
		r.start(agent)
	}

	go func() {
		if initialMsg != "" {
			r.Broadcast(initialMsg)
		}
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			text := scanner.Text()
			if rest, ok := strings.CutPrefix(text, "/join "); ok {
				r.handleJoin(rest, resolve)
				continue
			}
			r.Broadcast(text)
		}
	}()

	go func() {
		for msg := range r.output {
			fmt.Println(msg)
		}
	}()

	<-r.ctx.Done()
	// Join does not start an agent after the session is closed, and it
	// holds the lock when it starts one. Take the lock one time before Wait.
	r.mu.Lock()
	r.mu.Unlock()
	r.wg.Wait()
}

func (r *Relay) start(agent *agentClient) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.readAndRelay(agent)
	}()
}

func (r *Relay) Broadcast(text string) {
	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(text))
	msg.Metadata = senderMetadata(clientSender)
	r.deliver(&a2a.SendMessageRequest{Message: msg}, nil, "")
}

// deliver records a message in the session history and puts a copy in the
// queue of each agent other than from. It does the two steps with the lock
// held, so an agent that joins gets each message one time: in the history,
// or from its queue.
func (r *Relay) deliver(req *a2a.SendMessageRequest, from *agentClient, kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.history = append(r.history, req.Message)
	for _, other := range r.agents {
		if other == from {
			continue
		}
		msgCopy := *req.Message
		msgCopy.ID = a2a.NewMessageID()
		other.push(&a2a.SendMessageRequest{Message: &msgCopy})
		if from != nil {
			r.print(fmt.Sprintf("[relay] %s → %s (%s)", from.name, other.name, kind))
		}
	}
}

// push adds a request to the queue of the agent. The caller holds Relay.mu.
// The queue has no size limit, so push does not wait: an agent that does not
// read its queue must not stop the other agents.
func (a *agentClient) push(req *a2a.SendMessageRequest) {
	a.queue = append(a.queue, req)
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// next takes the oldest request from the queue of the agent. It waits if
// the queue is empty. It returns nil when the session is closed.
func (r *Relay) next(a *agentClient) *a2a.SendMessageRequest {
	for {
		r.mu.Lock()
		if len(a.queue) > 0 {
			req := a.queue[0]
			a.queue = a.queue[1:]
			r.mu.Unlock()
			return req
		}
		r.mu.Unlock()
		select {
		case <-a.wake:
		case <-r.ctx.Done():
			return nil
		}
	}
}

// print writes a line to the output and does not wait if the output is full.
func (r *Relay) print(line string) {
	select {
	case r.output <- line:
	default:
	}
}

// handleJoin runs the command "/join <agent card> [all|none|last=N]".
func (r *Relay) handleJoin(args string, resolve func(ref string) (*agentClient, error)) {
	fields := strings.Fields(args)
	if len(fields) == 0 || len(fields) > 2 {
		r.output <- "[System]: usage: /join <agent card> [all|none|last=N]"
		return
	}
	keep := -1 // all
	if len(fields) == 2 {
		n, isLast := strings.CutPrefix(fields[1], "last=")
		switch {
		case fields[1] == "all":
		case fields[1] == "none":
			keep = 0
		case isLast:
			v, err := strconv.Atoi(n)
			if err != nil || v < 0 {
				r.output <- "[System]: last=N needs a number that is 0 or more"
				return
			}
			keep = v
		default:
			r.output <- "[System]: usage: /join <agent card> [all|none|last=N]"
			return
		}
	}
	agent, err := resolve(fields[0])
	if err != nil {
		r.output <- fmt.Sprintf("[System]: join failed: %v", err)
		return
	}
	if !agent.lateJoin {
		r.output <- fmt.Sprintf("[System]: join refused: %s does not declare late-join in its agent card", agent.name)
		return
	}
	sent, omitted := r.Join(agent, keep)
	r.output <- fmt.Sprintf("[System]: join message sent to %s (%d earlier messages, %d omitted)", agent.name, sent, omitted)
}

// Join adds an agent to the active session. The first message that the agent
// gets has the earlier messages of the session in one data part. keep is the
// number of earlier messages to send, counted from the newest; a negative
// value sends all of them. The initial Task of each agent is always sent, so
// the new agent learns who is in the session. It is not counted, and it is
// sent without its history and artifacts.
func (r *Relay) Join(agent *agentClient, keep int) (sent, omitted int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ctx.Err() != nil {
		return 0, 0
	}

	total := 0
	for _, m := range r.history {
		if !isTaskAnnouncement(m) {
			total++
		}
	}
	if keep >= 0 && keep < total {
		omitted = total - keep
	}
	messages := make([]any, 0, len(r.history))
	skip := omitted
	for _, m := range r.history {
		isTask := isTaskAnnouncement(m)
		if !isTask && skip > 0 {
			skip--
			continue
		}
		mm := toMap(m)
		if mm == nil {
			continue
		}
		if isTask {
			stripTaskContent(mm)
		} else {
			sent++
		}
		messages = append(messages, mm)
	}

	join := &a2a.Message{
		ID:       a2a.NewMessageID(),
		Role:     a2a.MessageRoleUser,
		Metadata: senderMetadata(clientSender),
		Parts: a2a.ContentParts{{
			Content:   a2a.Data{Value: map[string]any{"messages": messages, "omitted": omitted}},
			MediaType: historyMediaType,
		}},
	}
	// The queue of a new agent is empty, so this message is the one that
	// creates the task of the agent. Live messages go into the queue after it.
	agent.joined = true
	agent.push(&a2a.SendMessageRequest{Message: join})
	r.agents = append(r.agents, agent)
	r.start(agent)
	return sent, omitted
}

func isTaskAnnouncement(m *a2a.Message) bool {
	return len(m.Parts) == 1 && m.Parts[0] != nil && m.Parts[0].MediaType == taskMediaType
}

// stripTaskContent removes the history, the artifacts and the timeline from
// the Task JSON of an initial Task message. They can contain messages that
// the history rule omits.
func stripTaskContent(msg map[string]any) {
	parts, _ := msg["parts"].([]any)
	for _, p := range parts {
		part, _ := p.(map[string]any)
		if task, ok := part["data"].(map[string]any); ok {
			stripTask(task)
		}
	}
}

func stripTask(task map[string]any) {
	delete(task, "history")
	delete(task, "artifacts")
	delete(task, "timeline")
}

// remove takes an agent out of the session when its stream ends.
func (r *Relay) remove(agent *agentClient) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, a := range r.agents {
		if a == agent {
			r.agents = append(r.agents[:i:i], r.agents[i+1:]...)
			return
		}
	}
}

func senderMetadata(sender string) map[string]any {
	return map[string]any{
		sharedTaskExt: map[string]any{"message-sender": sender},
	}
}

func (r *Relay) readAndRelay(agent *agentClient) {
	output := r.output
	defer r.remove(agent)

	firstReq := r.next(agent)
	if firstReq == nil {
		return
	}

	go func() {
		select {
		case <-agent.taskReady:
		case <-r.ctx.Done():
			return
		}
		for {
			req := r.next(agent)
			if req == nil {
				return
			}
			// Set the IDs at send time. A message can enter the queue
			// before the agent has created its task.
			req.Message.TaskID = agent.taskID
			req.Message.ContextID = agent.contextID
			_, err := agent.client.SendMessage(r.ctx, req)
			if err != nil {
				output <- fmt.Sprintf("[System]: relay error to %s: %v", agent.name, err)
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
			agent.readyOnce.Do(func() {
				agent.taskID = task.ID
				agent.contextID = task.ContextID
				close(agent.taskReady)
				if agent.joined {
					output <- fmt.Sprintf("[System]: %s joined", agent.name)
				}
			})
		}

		if b, err := json.Marshal(event); err == nil {
			output <- fmt.Sprintf("[%s]: %s", agent.name, b)
		}

		relayReq := translateEventToRequest(event, agent.name)
		if _, isTask := event.(*a2a.Task); isTask && agent.joined && relayReq != nil {
			// The history of this Task has the join message. The other
			// agents must not get the earlier messages from it.
			if task, ok := relayReq.Message.Parts[0].Content.(a2a.Data).Value.(map[string]any); ok {
				stripTask(task)
			}
		}
		if relayReq != nil {
			r.deliver(relayReq, agent, eventKind(event))
		}
	}
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
		Metadata: senderMetadata(sender),
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

// declaresLateJoin reports whether the agent card has "late-join": true in
// the params of the collaborative-task extension.
func declaresLateJoin(card *a2a.AgentCard) bool {
	for _, ext := range card.Capabilities.Extensions {
		if ext.URI == collabTaskExt {
			v, _ := ext.Params["late-join"].(bool)
			return v
		}
	}
	return false
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
	resolve := func(ref string) (*agentClient, error) {
		card, err := resolver.Resolve(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve card %s: %w", ref, err)
		}

		client, err := a2aclient.NewFromCard(ctx, card,
			a2agrpc.WithGRPCTransport(grpc.WithTransportCredentials(insecure.NewCredentials())),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create client for %s: %w", card.Name, err)
		}

		return &agentClient{
			name:      card.Name,
			client:    client,
			wake:      make(chan struct{}, 1),
			taskReady: make(chan struct{}),
			lateJoin:  declaresLateJoin(card),
		}, nil
	}

	var agents []*agentClient
	for _, ref := range agentCards {
		agent, err := resolve(ref)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		agents = append(agents, agent)
	}

	relay := NewRelay(ctx, agents)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go relay.Run(initialMsg, resolve)

	<-sigChan
	fmt.Println("\nClosing session...")
	relay.cancel()
}
