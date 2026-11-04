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
	"encoding/json"
	"fmt"
	"iter"
	"log"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// CollabAgentExecutor is an AgentExecutor that participates in a collaborative
// group channel. It runs a long-lived LLM loop per task, receiving messages via
// FanInHandler's inbox and optionally responding using the "respond" tool.
type CollabAgentExecutor struct {
	name  string
	llm   *anthropic.Client
	model string
	fanIn *FanInHandler
	debug bool
}

func (a *CollabAgentExecutor) debugf(format string, args ...any) {
	if a.debug {
		log.Printf(format, args...)
	}
}

var _ a2asrv.AgentExecutor = (*CollabAgentExecutor)(nil)

// Cancel handles task cancellation by emitting a cancelled status event.
func (a *CollabAgentExecutor) Cancel(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCanceled, nil), nil)
	}
}

// Execute starts the long-running agent session for a task.
// The inbox is registered BEFORE the first yield to avoid a race with relay
// messages arriving immediately after the task becomes known to the client.
func (a *CollabAgentExecutor) Execute(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		inbox := make(chan *a2a.Message, 64)
		a.fanIn.RegisterInbox(execCtx.TaskID, inbox)
		defer a.fanIn.DeregisterInbox(execCtx.TaskID)

		if !yield(a2a.NewSubmittedTask(execCtx, execCtx.Message), nil) {
			return
		}
		if !yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateWorking, nil), nil) {
			return
		}

		a.runLLMLoop(ctx, execCtx, execCtx.Message, inbox, yield)
	}
}

func (a *CollabAgentExecutor) runLLMLoop(
	ctx context.Context,
	execCtx *a2asrv.ExecutorContext,
	initial *a2a.Message,
	inbox <-chan *a2a.Message,
	yield func(a2a.Event, error) bool,
) {
	systemPrompt := fmt.Sprintf(
		"You are %s, a participant in a collaborative group channel with other AI agents. "+
			"You receive messages from other participants — both humans and other agents. "+
			"Only respond using the `respond` tool if you genuinely have something to add to the conversation. "+
			"You don't need to respond to every message you see. Keep responses concise and relevant.",
		a.name,
	)

	respondTool := anthropic.ToolUnionParamOfTool(
		anthropic.ToolInputSchemaParam{
			Properties: map[string]any{
				"message": map[string]any{
					"type":        "string",
					"description": "The message to send to the collaborative channel.",
				},
			},
			Required: []string{"message"},
		},
		"respond",
	)
	respondTool.OfTool.Description = anthropic.String(
		"Send a message to the collaborative channel. Use this when you have something relevant to contribute.",
	)

	tag := fmt.Sprintf("[%s task=%s]", a.name, execCtx.TaskID)

	messages := []anthropic.MessageParam{
		anthropic.NewUserMessage(
			anthropic.NewTextBlock(formatMessage(initial)),
		),
	}

	for {
		resp, err := a.llm.Messages.New(ctx, anthropic.MessageNewParams{
			Model:     anthropic.Model(a.model),
			MaxTokens: 1024,
			System:    []anthropic.TextBlockParam{{Text: systemPrompt}},
			Messages:  messages,
			Tools:     []anthropic.ToolUnionParam{respondTool},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("%s LLM error: %v", tag, err)
			return
		}

		a.debugf("%s LLM stop_reason=%s", tag, resp.StopReason)
		for _, block := range resp.Content {
			switch b := block.AsAny().(type) {
			case anthropic.TextBlock:
				if b.Text != "" {
					a.debugf("%s LLM text: %s", tag, b.Text)
				}
			case anthropic.ToolUseBlock:
				a.debugf("%s LLM tool_use: %s input=%s", tag, b.Name, b.JSON.Input.Raw())
			}
		}

		messages = append(messages, resp.ToParam())

		var toolResults []anthropic.ContentBlockParamUnion
		for _, block := range resp.Content {
			switch b := block.AsAny().(type) {
			case anthropic.ToolUseBlock:
				if b.Name == "respond" {
					var input struct {
						Message string `json:"message"`
					}
					if jsonErr := json.Unmarshal([]byte(b.JSON.Input.Raw()), &input); jsonErr == nil && input.Message != "" {
						msg := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(input.Message))
						if !yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateWorking, msg), nil) {
							return
						}
					}
				}
				toolResults = append(toolResults, anthropic.ContentBlockParamUnion{
					OfToolResult: &anthropic.ToolResultBlockParam{
						ToolUseID: b.ID,
						Content: []anthropic.ToolResultBlockParamContentUnion{
							{OfText: &anthropic.TextBlockParam{Text: "ok"}},
						},
					},
				})
			}
		}

		if len(toolResults) > 0 {
			messages = append(messages, anthropic.NewUserMessage(toolResults...))

			// Non-blocking check for a new incoming message.
			select {
			case incoming, ok := <-inbox:
				if !ok {
					return
				}
				formatted := formatMessage(incoming)
				a.debugf("%s inbox: %s", tag, formatted)
				messages = append(messages, anthropic.NewUserMessage(
					anthropic.NewTextBlock(formatted),
				))
			case <-ctx.Done():
				return
			default:
			}
		} else {
			// LLM chose not to use any tool; waiting for next incoming message.
			a.debugf("%s LLM did not use any tool; waiting for next incoming message", tag)
			select {
			case incoming, ok := <-inbox:
				if !ok {
					return
				}
				formatted := formatMessage(incoming)
				a.debugf("%s inbox: %s", tag, formatted)
				messages = append(messages, anthropic.NewUserMessage(
					anthropic.NewTextBlock(formatted),
				))
			case <-ctx.Done():
				return
			}
		}
	}
}

// formatMessage converts an incoming A2A message to a human-readable string for
// the LLM context, handling collaborative event media types.
func formatMessage(msg *a2a.Message) string {
	if msg == nil {
		return ""
	}
	sender := "agent"
	if msg.Metadata != nil {
		if ext, ok := msg.Metadata["https://a2a-protocol.org/extensions/shared-task/v1"].(map[string]any); ok {
			if s, ok := ext["message-sender"].(string); ok && s != "" {
				sender = s
			}
		}
	}
	var lines []string
	for _, part := range msg.Parts {
		if part == nil {
			continue
		}
		switch part.MediaType {
		case "application/vnd.a2a.task+json":
			lines = append(lines, fmt.Sprintf("[%s joined]", sender))
		case "application/vnd.a2a.task-status-update+json":
			text := extractStatusUpdateText(part)
			if text != "" {
				lines = append(lines, fmt.Sprintf("[%s] %s", sender, text))
			} else {
				lines = append(lines, fmt.Sprintf("[%s] (status update)", sender))
			}
		default:
			switch v := part.Content.(type) {
			case a2a.Text:
				lines = append(lines, "[user] "+string(v))
			case a2a.Data:
				lines = append(lines, "[user] (structured data)")
			default:
				lines = append(lines, "[user] (message)")
			}
		}
	}
	return strings.Join(lines, "\n")
}

// extractStatusUpdateText pulls the message text from a TaskStatusUpdateEvent
// that was encoded as map[string]any by the pbconv layer.
func extractStatusUpdateText(part *a2a.Part) string {
	dataVal, isData := part.Content.(a2a.Data)
	if !isData {
		return ""
	}
	m, ok := dataVal.Value.(map[string]any)
	if !ok {
		return ""
	}
	return mapNestedText(m, "status", "message", "parts")
}

// mapNestedText navigates a map[string]any to find text parts under
// m[statusKey][messageKey][partsKey][*]["text"].
func mapNestedText(m map[string]any, statusKey, messageKey, partsKey string) string {
	statusRaw, ok := m[statusKey]
	if !ok {
		return ""
	}
	statusMap, ok := statusRaw.(map[string]any)
	if !ok {
		return ""
	}
	messageRaw, ok := statusMap[messageKey]
	if !ok {
		return ""
	}
	messageMap, ok := messageRaw.(map[string]any)
	if !ok {
		return ""
	}
	partsRaw, ok := messageMap[partsKey]
	if !ok {
		return ""
	}
	parts, ok := partsRaw.([]any)
	if !ok {
		return ""
	}
	var texts []string
	for _, p := range parts {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}
		if t, ok := pm["text"].(string); ok && t != "" {
			texts = append(texts, t)
		}
	}
	return strings.Join(texts, " ")
}
