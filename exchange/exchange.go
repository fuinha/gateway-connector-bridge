// Copyright © 2017 The Things Network
// Use of this source code is governed by the MIT license that can be found in the LICENSE file.

package exchange

import (
	"fmt"
	"sync"
	"time"

	"github.com/TheThingsNetwork/gateway-connector-bridge/auth"
	"github.com/TheThingsNetwork/gateway-connector-bridge/backend"
	"github.com/TheThingsNetwork/gateway-connector-bridge/status/statusserver"
	"github.com/TheThingsNetwork/gateway-connector-bridge/types"
	"github.com/TheThingsNetwork/ttn/api/trace"
	"github.com/apex/log"
	"github.com/deckarep/golang-set"
)

// Exchange routes messages between northbound backends (servers that are up the chain)
// and southbound backends gateways or servers that are down the chain.
//
// When a connection message is received on the southbound backend:
// - Uplink messages are routed from the southbound backends to the northbound backends
// - Downlink messages are routed from the northbound backends to the southbound backends
// - Status messages are routed from the southbound backends to the northbound backends
// until a disconnection message is received on the southbound backend
type Exchange struct {
	ctx  log.Interface
	mu   sync.Mutex
	done chan struct{}

	id   string
	auth auth.Interface

	northboundBackends []backend.Northbound
	southboundBackends []backend.Southbound
	backendInit        sync.WaitGroup

	northboundDone map[string][]chan struct{}
	southboundDone map[string][]chan struct{}
	doneLock       sync.Mutex

	connect    chan *types.ConnectMessage
	disconnect chan *types.DisconnectMessage
	uplink     chan *types.UplinkMessage
	status     chan *types.StatusMessage
	downlink   chan *types.DownlinkMessage

	gateways gatewayState
}

// New initializes a new Exchange
func New(ctx log.Interface) *Exchange {
	return &Exchange{
		ctx:            ctx,
		done:           make(chan struct{}),
		northboundDone: make(map[string][]chan struct{}),
		southboundDone: make(map[string][]chan struct{}),
		connect:        make(chan *types.ConnectMessage),
		disconnect:     make(chan *types.DisconnectMessage),
		uplink:         make(chan *types.UplinkMessage),
		status:         make(chan *types.StatusMessage),
		downlink:       make(chan *types.DownlinkMessage),
		gateways:       mapset.NewSet(),
	}
}

// SetID sets the id of this bridge
func (b *Exchange) SetID(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.id = id
	trace.SetComponent("bridge", id)
}

// SetAuth sets the authentication component
func (b *Exchange) SetAuth(auth auth.Interface) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.auth = auth
}

// AddNorthbound adds a new northbound backend (server that is up the chain)
func (b *Exchange) AddNorthbound(backend ...backend.Northbound) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.northboundBackends = append(b.northboundBackends, backend...)
}

func (b *Exchange) subscribeNorthbound(backend backend.Northbound) {
	if err := backend.Connect(); err != nil {
		b.ctx.WithError(err).Errorf("Could not set up backend %v", backend)
	}
	b.backendInit.Done()
	for {
		select {
		case <-b.done:
			return
		}
	}
}

// AddSouthbound adds a new southbound backend (gateway or server that is down the chain)
func (b *Exchange) AddSouthbound(backend ...backend.Southbound) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.southboundBackends = append(b.southboundBackends, backend...)
}

func (b *Exchange) subscribeSouthbound(backend backend.Southbound) {
	if err := backend.Connect(); err != nil {
		b.ctx.WithError(err).Errorf("Could not set up backend %v", backend)
	}
	connect, err := backend.SubscribeConnect()
	if err != nil {
		b.ctx.WithError(err).Errorf("Could not subscribe to connect from backend %v", backend)
	}
	disconnect, err := backend.SubscribeDisconnect()
	if err != nil {
		b.ctx.WithError(err).Errorf("Could not subscribe to disconnect from backend %v", backend)
	}
	b.backendInit.Done()
loop:
	for {
		select {
		case <-b.done:
			break loop
		case connectMessage := <-connect:
			b.connect <- connectMessage
		case disconnectMessage := <-disconnect:
			b.disconnect <- disconnectMessage
		}
	}
	if err := backend.UnsubscribeConnect(); err != nil {
		b.ctx.WithError(err).Errorf("Could not unsubscribe from connect on backend %v", backend)
	}
	if err := backend.UnsubscribeDisconnect(); err != nil {
		b.ctx.WithError(err).Errorf("Could not unsubscribe from disconnect on backend %v", backend)
	}
}

// ConnectGateway force-connects gateways with the given IDs
func (b *Exchange) ConnectGateway(gatewayID ...string) {
	for _, gatewayID := range gatewayID {
		b.connect <- &types.ConnectMessage{GatewayID: gatewayID}
	}
}

func (b *Exchange) handleChannels() {
	for {
		select {
		case <-b.done:
			return
		case connectMessage, ok := <-b.connect:
			if !ok {
				continue
			}
			ctx := b.ctx.WithField("GatewayID", connectMessage.GatewayID)
			if b.auth != nil {
				if connectMessage.Key != "" {
					ctx.Debug("Got access key")
					if err := b.auth.SetKey(connectMessage.GatewayID, connectMessage.Key); err != nil {
						ctx.WithError(err).Warn("Could not set gateway key")
					}
				}
			}
			if !b.gateways.Add(connectMessage.GatewayID) {
				ctx.Debug("Got connect message from already-connected gateway")
				continue
			}
			for _, backend := range b.northboundBackends {
				go b.activateNorthbound(backend, connectMessage.GatewayID)
			}
			for _, backend := range b.southboundBackends {
				go b.activateSouthbound(backend, connectMessage.GatewayID)
			}
			statusserver.ConnectGateway()
			ctx.Info("Handled connect")
		case disconnectMessage, ok := <-b.disconnect:
			if !ok {
				continue
			}
			ctx := b.ctx.WithField("GatewayID", disconnectMessage.GatewayID)
			if !b.gateways.Contains(disconnectMessage.GatewayID) {
				ctx.Debug("Got disconnect message from not-connected gateway")
				continue
			}
			if err := b.auth.ValidateKey(disconnectMessage.GatewayID, disconnectMessage.Key); err != nil {
				ctx.WithError(err).Warn("Got disconnect message with invalid Key")
				continue
			}
			b.deactivateNorthbound(disconnectMessage.GatewayID)
			b.deactivateSouthbound(disconnectMessage.GatewayID)
			b.gateways.Remove(disconnectMessage.GatewayID)
			statusserver.DisconnectGateway()
			ctx.Info("Handled disconnect")
		case uplinkMessage, ok := <-b.uplink:
			if !ok {
				continue
			}
			if meta := uplinkMessage.Message.GetGatewayMetadata(); meta != nil {
				meta.GatewayId = uplinkMessage.GatewayID
			}
			uplinkMessage.Message.Trace = uplinkMessage.Message.Trace.WithEvent(trace.ForwardEvent)
			for _, backend := range b.northboundBackends {
				if err := backend.PublishUplink(uplinkMessage); err != nil {
					b.ctx.WithFields(log.Fields{
						"Backend":   fmt.Sprintf("%T", backend),
						"GatewayID": uplinkMessage.GatewayID,
					}).WithError(err).Warn("Could not publish uplink")
				}
			}
			statusserver.Uplink()
			b.ctx.WithField("GatewayID", uplinkMessage.GatewayID).Info("Routed uplink")
		case downlinkMessage, ok := <-b.downlink:
			if !ok {
				continue
			}
			downlinkMessage.Message.Trace = downlinkMessage.Message.Trace.WithEvent(trace.ForwardEvent)
			for _, backend := range b.southboundBackends {
				if err := backend.PublishDownlink(downlinkMessage); err != nil {
					b.ctx.WithFields(log.Fields{
						"Backend":   fmt.Sprintf("%T", backend),
						"GatewayID": downlinkMessage.GatewayID,
					}).WithError(err).Warn("Could not publish downlink")
				}
			}
			statusserver.Downlink()
			b.ctx.WithField("GatewayID", downlinkMessage.GatewayID).Info("Routed downlink")
		case statusMessage, ok := <-b.status:
			if !ok {
				continue
			}
			statusMessage.Message.Bridge = b.id
			for _, backend := range b.northboundBackends {
				if err := backend.PublishStatus(statusMessage); err != nil {
					b.ctx.WithFields(log.Fields{
						"Backend":   fmt.Sprintf("%T", backend),
						"GatewayID": statusMessage.GatewayID,
					}).WithError(err).Warn("Could not publish status")
				}
			}
			statusserver.GatewayStatus()
			b.ctx.WithField("GatewayID", statusMessage.GatewayID).Info("Routed status")
		}
	}
}

func (b *Exchange) activateNorthbound(backend backend.Northbound, gatewayID string) {
	ctx := b.ctx.WithField("GatewayID", gatewayID).WithField("Backend", fmt.Sprintf("%T", backend))
	downlink, err := backend.SubscribeDownlink(gatewayID)
	if err != nil {
		ctx.WithError(err).Error("Could not subscribe to downlink")
	}
	done := make(chan struct{})
	b.doneLock.Lock()
	b.northboundDone[gatewayID] = append(b.northboundDone[gatewayID], done)
	b.doneLock.Unlock()
	ctx.Debug("Activated northbound")
loop:
	for {
		select {
		case <-done:
			break loop
		case downlinkMessage, ok := <-downlink:
			if !ok {
				continue
			}
			b.downlink <- downlinkMessage
		}
	}
	if err := backend.UnsubscribeDownlink(gatewayID); err != nil {
		ctx.WithError(err).Error("Could not unsubscribe from downlink")
	}
	ctx.Debug("Deactivated northbound")
}

func (b *Exchange) activateSouthbound(backend backend.Southbound, gatewayID string) {
	ctx := b.ctx.WithField("GatewayID", gatewayID).WithField("Backend", fmt.Sprintf("%T", backend))
	uplink, err := backend.SubscribeUplink(gatewayID)
	if err != nil {
		ctx.WithError(err).Error("Could not subscribe to uplink")
	}
	status, err := backend.SubscribeStatus(gatewayID)
	if err != nil {
		ctx.WithError(err).Error("Could not subscribe to status")
	}
	done := make(chan struct{})
	b.doneLock.Lock()
	b.southboundDone[gatewayID] = append(b.southboundDone[gatewayID], done)
	b.doneLock.Unlock()
	ctx.Debug("Activated southbound")
loop:
	for {
		select {
		case <-done:
			break loop
		case uplinkMessage, ok := <-uplink:
			if !ok {
				continue
			}
			b.uplink <- uplinkMessage
		case statusMessage, ok := <-status:
			if !ok {
				continue
			}
			b.status <- statusMessage
		}
	}
	if err := backend.UnsubscribeUplink(gatewayID); err != nil {
		ctx.WithError(err).Error("Could not unsubscribe from uplink")
	}
	if err := backend.UnsubscribeStatus(gatewayID); err != nil {
		ctx.WithError(err).Error("Could not unsubscribe from status")
	}
	ctx.Debug("Deactivated southbound")
}

func (b *Exchange) deactivateNorthbound(gatewayID string) {
	b.doneLock.Lock()
	defer b.doneLock.Unlock()
	if backends, ok := b.northboundDone[gatewayID]; ok {
		for _, done := range backends {
			close(done)
		}
		delete(b.northboundDone, gatewayID)
	}
}

func (b *Exchange) deactivateSouthbound(gatewayID string) {
	b.doneLock.Lock()
	defer b.doneLock.Unlock()
	if backends, ok := b.southboundDone[gatewayID]; ok {
		for _, done := range backends {
			close(done)
		}
		delete(b.southboundDone, gatewayID)
	}
}

// Start the Exchange
func (b *Exchange) Start(goroutines int, timeout time.Duration) (finishedWithinTimeout bool) {
	b.mu.Lock()
	for _, backend := range b.northboundBackends {
		b.backendInit.Add(1)
		go b.subscribeNorthbound(backend)
	}
	for _, backend := range b.southboundBackends {
		b.backendInit.Add(1)
		go b.subscribeSouthbound(backend)
	}
	c := make(chan struct{})
	go func() {
		defer close(c)
		b.backendInit.Wait()
	}()
	select {
	case <-c:
		finishedWithinTimeout = true
	case <-time.After(timeout):
		finishedWithinTimeout = false
	}
	for i := 0; i < goroutines; i++ {
		go b.handleChannels()
	}
	return
}

// Stop the Exchange
func (b *Exchange) Stop() {
	close(b.done) // This stops all new connections/disconnections
	b.doneLock.Lock()
	defer b.doneLock.Unlock()
	for _, backends := range b.northboundDone {
		for _, backend := range backends {
			close(backend)
		}
	}
	for _, backends := range b.southboundDone {
		for _, backend := range backends {
			close(backend)
		}
	}
	b.northboundDone = make(map[string][]chan struct{})
	b.southboundDone = make(map[string][]chan struct{})
	b.mu.Unlock()
}
