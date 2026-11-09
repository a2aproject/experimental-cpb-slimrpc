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
	"encoding/json"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"
)

// ── Data types ────────────────────────────────────────────────────────────────

// Order represents a customer order record.
type Order struct {
	ID            string `json:"id"`
	CustomerID    string `json:"customer_id"`
	ProductID     string `json:"product_id"`
	ProductName   string `json:"product_name"`
	Status        string `json:"status"`
	TrackingID    string `json:"tracking_id,omitempty"`
	PlacedDaysAgo int    `json:"placed_days_ago"`
	Notes         string `json:"notes,omitempty"`
}

// Product represents an inventory/catalog record.
type Product struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Stock       int    `json:"stock"`
	Reserved    int    `json:"reserved"`
	Available   int    `json:"available"`
	WarehouseID string `json:"warehouse_id"`
	BackOrdered bool   `json:"back_ordered"`
	Notes       string `json:"notes,omitempty"`
}

// Shipment represents a carrier tracking record.
type Shipment struct {
	TrackingID      string `json:"tracking_id"`
	Carrier         string `json:"carrier"`
	Status          string `json:"status"`
	LastScanLocation string `json:"last_scan_location"`
	DaysSinceUpdate int    `json:"days_since_update"`
	EstDeliveryDays int    `json:"est_delivery_days,omitempty"`
	DeliveredAt     string `json:"delivered_at,omitempty"`
	SignedBy        string `json:"signed_by,omitempty"`
	Notes           string `json:"notes,omitempty"`
}

// ── Fake data ─────────────────────────────────────────────────────────────────

var ordersDB = map[string]Order{
	"ORD-1001": {
		ID: "ORD-1001", CustomerID: "C-100", ProductID: "P-500", ProductName: "Wireless Headphones",
		Status: "shipped", TrackingID: "TRK-9001", PlacedDaysAgo: 14,
		Notes: "standard shipping selected",
	},
	"ORD-1002": {
		ID: "ORD-1002", CustomerID: "C-100", ProductID: "P-501", ProductName: "USB-C Cable",
		Status: "delivered", TrackingID: "TRK-9002", PlacedDaysAgo: 21,
	},
	"ORD-1003": {
		ID: "ORD-1003", CustomerID: "C-101", ProductID: "P-502", ProductName: "Laptop Stand",
		Status: "on_hold", PlacedDaysAgo: 5,
		Notes: "held due to inventory shortage — awaiting restock",
	},
	"ORD-1004": {
		ID: "ORD-1004", CustomerID: "C-102", ProductID: "P-500", ProductName: "Wireless Headphones",
		Status: "cancelled", PlacedDaysAgo: 10,
		Notes: "cancelled by customer; refund issued",
	},
}

var productsDB = map[string]Product{
	"P-500": {
		ID: "P-500", Name: "Wireless Headphones", Stock: 3, Reserved: 5, Available: -2,
		WarehouseID: "W-1", BackOrdered: false,
		Notes: "WARNING: oversold — 2 units reserved without physical stock",
	},
	"P-501": {
		ID: "P-501", Name: "USB-C Cable", Stock: 150, Reserved: 12, Available: 138,
		WarehouseID: "W-1",
	},
	"P-502": {
		ID: "P-502", Name: "Laptop Stand", Stock: 0, Reserved: 1, Available: -1,
		WarehouseID: "W-2", BackOrdered: true,
		Notes: "backordered from supplier; ETA unknown",
	},
}

var shipmentsDB = map[string]Shipment{
	"TRK-9001": {
		TrackingID: "TRK-9001", Carrier: "FedEx",
		Status:          "delayed_weather",
		LastScanLocation: "Memphis Distribution Center",
		DaysSinceUpdate: 3, EstDeliveryDays: 3,
		Notes: "delayed by severe weather event in Memphis; package in holding",
	},
	"TRK-9002": {
		TrackingID: "TRK-9002", Carrier: "FedEx",
		Status:          "delivered",
		LastScanLocation: "Customer doorstep",
		DaysSinceUpdate: 5,
		DeliveredAt:     "5 days ago", SignedBy: "J. Smith",
	},
}

// ── Role system prompts ───────────────────────────────────────────────────────

// RoleSystemPrompt returns the system prompt for the given role. When role is
// empty it returns the generic collaborative-agent prompt.
func RoleSystemPrompt(agentName, role string) string {
	switch role {
	case "orders":
		return fmt.Sprintf(`You are %s, an Orders Agent in a collaborative investigation channel.

You have access to the order management system. Use your tools to look up specific order
records before drawing conclusions. Available orders:

  ORD-1001: Customer C-100 — Wireless Headphones (P-500), status=shipped, tracking=TRK-9001, placed 14 days ago
  ORD-1002: Customer C-100 — USB-C Cable (P-501), status=delivered, placed 21 days ago
  ORD-1003: Customer C-101 — Laptop Stand (P-502), status=on_hold, placed 5 days ago
  ORD-1004: Customer C-102 — Wireless Headphones (P-500), status=cancelled, placed 10 days ago

When you have relevant findings, use the "respond" tool to share them with the channel.
Only respond when you have something concrete to contribute. Keep responses concise.`, agentName)

	case "inventory":
		return fmt.Sprintf(`You are %s, an Inventory Agent in a collaborative investigation channel.

You have access to the warehouse inventory system. Use your tools to check stock levels,
reservation counts, and identify oversold or backordered products. Known products:

  P-500: Wireless Headphones  — stock=3, reserved=5 (OVERSOLD by 2 units), warehouse W-1
  P-501: USB-C Cable          — stock=150, reserved=12 (normal), warehouse W-1
  P-502: Laptop Stand         — stock=0, reserved=1 (backordered from supplier), warehouse W-2

When you find inventory anomalies (overselling, stockouts), use the "respond" tool to
surface them to the channel. Only respond when you have something concrete to contribute.`, agentName)

	case "shipping":
		return fmt.Sprintf(`You are %s, a Shipping Agent in a collaborative investigation channel.

You have access to carrier tracking data. Use your tools to look up shipment status and
timelines by tracking ID. Known tracking records:

  TRK-9001: FedEx — delayed_weather, last scanned at Memphis DC 3 days ago, est. delivery in 3 days
  TRK-9002: FedEx — delivered 5 days ago, signed by J. Smith

When you find relevant tracking information, use the "respond" tool to report it to the
channel. Only respond when you have something concrete to contribute. Keep responses concise.`, agentName)

	default:
		return fmt.Sprintf(
			"You are %s, a participant in a collaborative group channel with other AI agents. "+
				"You receive messages from other participants — both humans and other agents. "+
				"Only respond using the `respond` tool if you genuinely have something to add to the conversation. "+
				"You don't need to respond to every message you see. Keep responses concise and relevant.",
			agentName,
		)
	}
}

// ── Role tools ────────────────────────────────────────────────────────────────

// RoleTools returns the domain-specific tools for a role. Returns nil for the
// generic agent (role == ""), which only has the respond tool.
func RoleTools(role string) []anthropic.ToolUnionParam {
	switch role {
	case "orders":
		return ordersTools()
	case "inventory":
		return inventoryTools()
	case "shipping":
		return shippingTools()
	default:
		return nil
	}
}

func makeTool(name, description string, properties map[string]any, required []string) anthropic.ToolUnionParam {
	t := anthropic.ToolUnionParamOfTool(
		anthropic.ToolInputSchemaParam{
			Properties: properties,
			Required:   required,
		},
		name,
	)
	t.OfTool.Description = anthropic.String(description)
	return t
}

func ordersTools() []anthropic.ToolUnionParam {
	orderIDProp := map[string]any{
		"type":        "string",
		"description": "Order ID, e.g. ORD-1001",
	}
	customerIDProp := map[string]any{
		"type":        "string",
		"description": "Customer ID, e.g. C-100",
	}
	return []anthropic.ToolUnionParam{
		makeTool(
			"get_order",
			"Retrieve full details for a specific order by its ID.",
			map[string]any{"order_id": orderIDProp},
			[]string{"order_id"},
		),
		makeTool(
			"list_customer_orders",
			"List all orders placed by a specific customer.",
			map[string]any{"customer_id": customerIDProp},
			[]string{"customer_id"},
		),
		makeTool(
			"get_tracking_for_order",
			"Get the carrier tracking ID associated with a shipped order.",
			map[string]any{"order_id": orderIDProp},
			[]string{"order_id"},
		),
	}
}

func inventoryTools() []anthropic.ToolUnionParam {
	productIDProp := map[string]any{
		"type":        "string",
		"description": "Product ID, e.g. P-500",
	}
	return []anthropic.ToolUnionParam{
		makeTool(
			"get_product_stock",
			"Get current stock level, reservation count, and availability for a product.",
			map[string]any{"product_id": productIDProp},
			[]string{"product_id"},
		),
		makeTool(
			"list_low_stock",
			"List all products where available stock (stock minus reserved) is zero or negative.",
			map[string]any{},
			[]string{},
		),
	}
}

func shippingTools() []anthropic.ToolUnionParam {
	trackingIDProp := map[string]any{
		"type":        "string",
		"description": "Carrier tracking ID, e.g. TRK-9001",
	}
	return []anthropic.ToolUnionParam{
		makeTool(
			"get_shipment_status",
			"Get the current status and location of a shipment by its tracking ID.",
			map[string]any{"tracking_id": trackingIDProp},
			[]string{"tracking_id"},
		),
		makeTool(
			"get_shipment_timeline",
			"Get a human-readable timeline summary for a shipment.",
			map[string]any{"tracking_id": trackingIDProp},
			[]string{"tracking_id"},
		),
	}
}

// ── Tool dispatcher ───────────────────────────────────────────────────────────

type toolError struct {
	Error string `json:"error"`
}

func marshalResult(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return `{"error":"marshal failed"}`
	}
	return string(b)
}

// HandleToolCall dispatches a non-respond tool call to the appropriate fake
// data store and returns a JSON-encoded result string.
func HandleToolCall(role, toolName, rawInput string) string {
	switch role {
	case "orders":
		return handleOrdersTool(toolName, rawInput)
	case "inventory":
		return handleInventoryTool(toolName, rawInput)
	case "shipping":
		return handleShippingTool(toolName, rawInput)
	default:
		return marshalResult(toolError{Error: "unknown role: " + role})
	}
}

func handleOrdersTool(toolName, rawInput string) string {
	switch toolName {
	case "get_order":
		var inp struct {
			OrderID string `json:"order_id"`
		}
		if err := json.Unmarshal([]byte(rawInput), &inp); err != nil {
			return marshalResult(toolError{Error: "invalid input: " + err.Error()})
		}
		o, ok := ordersDB[inp.OrderID]
		if !ok {
			return marshalResult(toolError{Error: "order not found: " + inp.OrderID})
		}
		return marshalResult(o)

	case "list_customer_orders":
		var inp struct {
			CustomerID string `json:"customer_id"`
		}
		if err := json.Unmarshal([]byte(rawInput), &inp); err != nil {
			return marshalResult(toolError{Error: "invalid input: " + err.Error()})
		}
		var results []Order
		for _, o := range ordersDB {
			if o.CustomerID == inp.CustomerID {
				results = append(results, o)
			}
		}
		if len(results) == 0 {
			return marshalResult(toolError{Error: "no orders found for customer: " + inp.CustomerID})
		}
		return marshalResult(results)

	case "get_tracking_for_order":
		var inp struct {
			OrderID string `json:"order_id"`
		}
		if err := json.Unmarshal([]byte(rawInput), &inp); err != nil {
			return marshalResult(toolError{Error: "invalid input: " + err.Error()})
		}
		o, ok := ordersDB[inp.OrderID]
		if !ok {
			return marshalResult(toolError{Error: "order not found: " + inp.OrderID})
		}
		if o.TrackingID == "" {
			return marshalResult(map[string]any{
				"order_id": o.ID,
				"status":   o.Status,
				"tracking": nil,
				"note":     "no tracking ID — order has not shipped",
			})
		}
		return marshalResult(map[string]any{
			"order_id":    o.ID,
			"status":      o.Status,
			"tracking_id": o.TrackingID,
		})

	default:
		return marshalResult(toolError{Error: "unknown orders tool: " + toolName})
	}
}

func handleInventoryTool(toolName, rawInput string) string {
	switch toolName {
	case "get_product_stock":
		var inp struct {
			ProductID string `json:"product_id"`
		}
		if err := json.Unmarshal([]byte(rawInput), &inp); err != nil {
			return marshalResult(toolError{Error: "invalid input: " + err.Error()})
		}
		p, ok := productsDB[inp.ProductID]
		if !ok {
			return marshalResult(toolError{Error: "product not found: " + inp.ProductID})
		}
		return marshalResult(p)

	case "list_low_stock":
		var results []Product
		for _, p := range productsDB {
			if p.Available <= 0 {
				results = append(results, p)
			}
		}
		return marshalResult(results)

	default:
		return marshalResult(toolError{Error: "unknown inventory tool: " + toolName})
	}
}

func handleShippingTool(toolName, rawInput string) string {
	switch toolName {
	case "get_shipment_status":
		var inp struct {
			TrackingID string `json:"tracking_id"`
		}
		if err := json.Unmarshal([]byte(rawInput), &inp); err != nil {
			return marshalResult(toolError{Error: "invalid input: " + err.Error()})
		}
		s, ok := shipmentsDB[inp.TrackingID]
		if !ok {
			return marshalResult(toolError{Error: "tracking ID not found: " + inp.TrackingID})
		}
		return marshalResult(s)

	case "get_shipment_timeline":
		var inp struct {
			TrackingID string `json:"tracking_id"`
		}
		if err := json.Unmarshal([]byte(rawInput), &inp); err != nil {
			return marshalResult(toolError{Error: "invalid input: " + err.Error()})
		}
		s, ok := shipmentsDB[inp.TrackingID]
		if !ok {
			return marshalResult(toolError{Error: "tracking ID not found: " + inp.TrackingID})
		}
		var timeline string
		if s.Status == "delivered" {
			timeline = fmt.Sprintf("Package delivered %s. Signed by %s.", s.DeliveredAt, s.SignedBy)
		} else {
			timeline = fmt.Sprintf(
				"Last scanned: %s (%d days ago). Status: %s. Estimated delivery in %d day(s). Note: %s",
				s.LastScanLocation, s.DaysSinceUpdate, s.Status, s.EstDeliveryDays, s.Notes,
			)
		}
		return marshalResult(map[string]any{
			"tracking_id": s.TrackingID,
			"carrier":     s.Carrier,
			"timeline":    timeline,
		})

	default:
		return marshalResult(toolError{Error: "unknown shipping tool: " + toolName})
	}
}
