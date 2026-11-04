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

package main

import (
	"context"
	"iter"
	"sync"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// FanInHandler is a RequestHandler middleware that intercepts SendMessage for
// tasks with an active inbox. When a running Execute has registered an inbox for
// a task, subsequent SendMessage calls for that task are routed directly to the
// inbox channel (no new Execute invocation), and the current task state is
// returned immediately. All other methods delegate to the wrapped inner handler.
type FanInHandler struct {
	inner   a2asrv.RequestHandler
	mu      sync.RWMutex
	inboxes map[a2a.TaskID]chan *a2a.Message
}

// NewFanInHandler creates a FanInHandler with no inner handler set yet.
// Call SetInner before the server starts accepting requests.
func NewFanInHandler() *FanInHandler {
	return &FanInHandler{
		inboxes: make(map[a2a.TaskID]chan *a2a.Message),
	}
}

// SetInner sets the underlying RequestHandler that all non-intercepted calls
// delegate to.
func (h *FanInHandler) SetInner(rh a2asrv.RequestHandler) {
	h.inner = rh
}

// RegisterInbox registers an inbox channel for a task. Must be called before
// yielding the first event from Execute to avoid a race with relay messages.
func (h *FanInHandler) RegisterInbox(taskID a2a.TaskID, inbox chan *a2a.Message) {
	h.mu.Lock()
	h.inboxes[taskID] = inbox
	h.mu.Unlock()
}

// DeregisterInbox removes the inbox for a task when Execute finishes.
func (h *FanInHandler) DeregisterInbox(taskID a2a.TaskID) {
	h.mu.Lock()
	delete(h.inboxes, taskID)
	h.mu.Unlock()
}

// SendMessage intercepts SendMessage for tasks with a registered inbox. The
// message is routed to the inbox and the current task state is returned.
// For tasks without an inbox, the call delegates to the inner handler.
func (h *FanInHandler) SendMessage(ctx context.Context, req *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
	if req.Message != nil && req.Message.TaskID != "" {
		h.mu.RLock()
		inbox, ok := h.inboxes[req.Message.TaskID]
		h.mu.RUnlock()
		if ok {
			select {
			case inbox <- req.Message:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return h.inner.GetTask(ctx, &a2a.GetTaskRequest{ID: req.Message.TaskID})
		}
	}
	return h.inner.SendMessage(ctx, req)
}

func (h *FanInHandler) GetTask(ctx context.Context, req *a2a.GetTaskRequest) (*a2a.Task, error) {
	return h.inner.GetTask(ctx, req)
}

func (h *FanInHandler) ListTasks(ctx context.Context, req *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	return h.inner.ListTasks(ctx, req)
}

func (h *FanInHandler) CancelTask(ctx context.Context, req *a2a.CancelTaskRequest) (*a2a.Task, error) {
	return h.inner.CancelTask(ctx, req)
}

func (h *FanInHandler) SubscribeToTask(ctx context.Context, req *a2a.SubscribeToTaskRequest) iter.Seq2[a2a.Event, error] {
	return h.inner.SubscribeToTask(ctx, req)
}

func (h *FanInHandler) SendStreamingMessage(ctx context.Context, req *a2a.SendMessageRequest) iter.Seq2[a2a.Event, error] {
	return h.inner.SendStreamingMessage(ctx, req)
}

func (h *FanInHandler) GetTaskPushConfig(ctx context.Context, req *a2a.GetTaskPushConfigRequest) (*a2a.PushConfig, error) {
	return h.inner.GetTaskPushConfig(ctx, req)
}

func (h *FanInHandler) ListTaskPushConfigs(ctx context.Context, req *a2a.ListTaskPushConfigRequest) (*a2a.ListTaskPushConfigResponse, error) {
	return h.inner.ListTaskPushConfigs(ctx, req)
}

func (h *FanInHandler) CreateTaskPushConfig(ctx context.Context, req *a2a.PushConfig) (*a2a.PushConfig, error) {
	return h.inner.CreateTaskPushConfig(ctx, req)
}

func (h *FanInHandler) DeleteTaskPushConfig(ctx context.Context, req *a2a.DeleteTaskPushConfigRequest) error {
	return h.inner.DeleteTaskPushConfig(ctx, req)
}

func (h *FanInHandler) GetExtendedAgentCard(ctx context.Context, req *a2a.GetExtendedAgentCardRequest) (*a2a.AgentCard, error) {
	return h.inner.GetExtendedAgentCard(ctx, req)
}
