// Copyright © 2016 The Things Network
// Use of this source code is governed by the MIT license that can be found in the LICENSE file.

package pktfwd

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"

	logrus "github.com/Sirupsen/logrus"
	"github.com/TheThingsNetwork/gateway-connector-bridge/types"
	"github.com/apex/log"
	pktfwdgtw "github.com/brocaar/lora-gateway-bridge/gateway"
	"github.com/brocaar/lorawan"
)

func init() {
	logrus.SetOutput(ioutil.Discard)
}

// New returns a new PktFwd
func New(config Config, ctx log.Interface) (*PktFwd, error) {
	pktfwd := new(PktFwd)
	pktfwd.ctx = ctx.WithField("Connector", "PktFwd")
	pktfwd.config = config
	pktfwd.gateways = make(map[lorawan.EUI64]*gateway)
	pktfwd.downlink = make(chan *types.DownlinkMessage, BufferSize)
	return pktfwd, nil
}

// BufferSize indicates the maximum number of messages that should be buffered
var BufferSize = 10

// Config contains configuration for PktFwd
type Config struct {
	UDPBind string
}

type gateway struct {
	sync.Mutex
	uplink chan *types.UplinkMessage
	status chan *types.StatusMessage
}

// PktFwd side of the bridge
type PktFwd struct {
	config  Config
	ctx     log.Interface
	mu      sync.Mutex
	backend *pktfwdgtw.Backend

	connect    chan *types.ConnectMessage
	disconnect chan *types.DisconnectMessage
	downlink   chan *types.DownlinkMessage

	gatewaysMu sync.RWMutex
	gateways   map[lorawan.EUI64]*gateway
}

func (c *PktFwd) getMAC(id string) (mac lorawan.EUI64, err error) {
	err = mac.UnmarshalText([]byte(strings.TrimPrefix(id, "eui-")))
	return
}

func (c *PktFwd) getID(mac lorawan.EUI64) string {
	return fmt.Sprintf("eui-%s", mac)
}

func (c *PktFwd) getGateway(mac lorawan.EUI64) *gateway {
	c.gatewaysMu.Lock()
	defer c.gatewaysMu.Unlock()
	_, ok := c.gateways[mac]
	if !ok {
		c.gateways[mac] = new(gateway)
	}
	return c.gateways[mac]
}

// Connect PktFwd
func (c *PktFwd) Connect() (err error) {
	c.backend, err = pktfwdgtw.NewBackend(c.config.UDPBind, c.onNew, c.onDelete)

	go func() {
		for rxPacket := range c.backend.RXPacketChan() {
			ctx := c.ctx.WithField("GatewayID", c.getID(rxPacket.RXInfo.MAC))

			// Get the gateway
			gtw := c.getGateway(rxPacket.RXInfo.MAC)

			// Get the uplink channel or continue
			gtw.Lock()
			uplink := gtw.uplink
			gtw.Unlock()
			if uplink == nil {
				ctx.Warn("Gateway Uplink inactive")
				continue
			}

			// Send the uplink if possible
			select {
			case uplink <- &types.UplinkMessage{
				GatewayID: fmt.Sprintf("eui-%s", rxPacket.RXInfo.MAC),
				Message:   c.convertRXPacket(rxPacket),
			}:
				ctx.Debug("Received uplink message")
			default:
				ctx.WithField("error", "buffer full").Error("Could not send Uplink message")
			}
		}
	}()

	go func() {
		for stat := range c.backend.StatsChan() {
			ctx := c.ctx.WithField("GatewayID", c.getID(stat.MAC))

			// Get the gateway
			gtw := c.getGateway(stat.MAC)

			// Get the status channel or continue
			gtw.Lock()
			status := gtw.status
			gtw.Unlock()
			if status == nil {
				ctx.Warn("Gateway Status inactive")
				continue
			}

			// Send the status if possible
			select {
			case status <- &types.StatusMessage{
				GatewayID: fmt.Sprintf("eui-%s", stat.MAC),
				Message:   c.convertStatsPacket(stat),
			}:
				ctx.Debug("Received status message")
			default:
				ctx.WithField("error", "buffer full").Error("Could not send Status message")
			}
		}
	}()

	go func() {
		for downlink := range c.downlink {
			ctx := c.ctx.WithField("GatewayID", downlink.GatewayID)
			txpk, err := c.convertTXPacket(downlink)
			if err != nil {
				ctx.WithError(err).Error("Could not convert downlink to TXPacket")
			}
			if err := c.backend.Send(*txpk); err != nil {
				ctx.WithError(err).Error("Could not send downlink message")
			}
			ctx.Debug("Published downlink message")
		}
	}()

	return
}

func (c *PktFwd) onNew(mac lorawan.EUI64) error {
	select {
	case c.connect <- &types.ConnectMessage{GatewayID: c.getID(mac)}:
		return nil
	default:
	}
	return errors.New("Could not send connect message")
}

func (c *PktFwd) onDelete(mac lorawan.EUI64) error {
	select {
	case c.disconnect <- &types.DisconnectMessage{GatewayID: c.getID(mac)}:
		return nil
	default:
	}
	return errors.New("Could not send disconnect message")
}

// Disconnect PktFwd
func (c *PktFwd) Disconnect() error {
	return c.backend.Close()
}

// SubscribeConnect subscribes to connect messages
func (c *PktFwd) SubscribeConnect() (<-chan *types.ConnectMessage, error) {
	c.connect = make(chan *types.ConnectMessage, BufferSize)
	return c.connect, nil
}

// UnsubscribeConnect unsubscribes from connect messages
func (c *PktFwd) UnsubscribeConnect() error {
	close(c.connect)
	return nil
}

// SubscribeDisconnect subscribes to disconnect messages
func (c *PktFwd) SubscribeDisconnect() (<-chan *types.DisconnectMessage, error) {
	c.disconnect = make(chan *types.DisconnectMessage, BufferSize)
	return c.disconnect, nil
}

// UnsubscribeDisconnect unsubscribes from disconnect messages
func (c *PktFwd) UnsubscribeDisconnect() error {
	close(c.disconnect)
	return nil
}

// SubscribeUplink handles uplink messages for the given gateway ID
func (c *PktFwd) SubscribeUplink(gatewayID string) (<-chan *types.UplinkMessage, error) {
	mac, err := c.getMAC(gatewayID)
	if err != nil {
		return nil, err
	}
	gtw := c.getGateway(mac)
	uplink := make(chan *types.UplinkMessage, BufferSize)
	gtw.Lock()
	gtw.uplink = uplink
	gtw.Unlock()
	return uplink, nil
}

// UnsubscribeUplink unsubscribes from uplink messages for the given gateway ID
func (c *PktFwd) UnsubscribeUplink(gatewayID string) error {
	mac, err := c.getMAC(gatewayID)
	if err != nil {
		return err
	}
	gtw := c.getGateway(mac)
	gtw.Lock()
	if gtw.uplink != nil {
		close(gtw.uplink)
		gtw.uplink = nil
	}
	gtw.Unlock()
	return nil
}

// SubscribeStatus handles status messages for the given gateway ID
func (c *PktFwd) SubscribeStatus(gatewayID string) (<-chan *types.StatusMessage, error) {
	mac, err := c.getMAC(gatewayID)
	if err != nil {
		return nil, err
	}
	gtw := c.getGateway(mac)
	status := make(chan *types.StatusMessage, BufferSize)
	gtw.Lock()
	gtw.status = status
	gtw.Unlock()
	return status, nil
}

// UnsubscribeStatus unsubscribes from status messages for the given gateway ID
func (c *PktFwd) UnsubscribeStatus(gatewayID string) error {
	mac, err := c.getMAC(gatewayID)
	if err != nil {
		return err
	}
	gtw := c.getGateway(mac)
	gtw.Lock()
	if gtw.status != nil {
		close(gtw.status)
		gtw.status = nil
	}
	gtw.Unlock()
	return nil
}

// PublishDownlink publishes a downlink message
func (c *PktFwd) PublishDownlink(message *types.DownlinkMessage) error {
	ctx := c.ctx.WithField("GatewayID", message.GatewayID)
	select {
	case c.downlink <- message:
	default:
		ctx.WithField("error", "buffer full").Error("Could not publish downlink message")
	}
	return nil
}
