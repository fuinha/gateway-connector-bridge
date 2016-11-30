// Copyright © 2016 The Things Network
// Use of this source code is governed by the MIT license that can be found in the LICENSE file.

package pktfwd

import (
	"fmt"

	"github.com/TheThingsNetwork/gateway-connector-bridge/types"
	pb_gateway "github.com/TheThingsNetwork/ttn/api/gateway"
	"github.com/TheThingsNetwork/ttn/api/protocol"
	pb_lorawan "github.com/TheThingsNetwork/ttn/api/protocol/lorawan"
	"github.com/TheThingsNetwork/ttn/api/router"
	ttnTypes "github.com/TheThingsNetwork/ttn/core/types"
	"github.com/brocaar/loraserver/api/gw"
	"github.com/brocaar/lorawan/band"
)

func (c *PktFwd) convertRXPacket(rxPacket gw.RXPacketBytes) *router.UplinkMessage {
	// Convert some Modulation-dependent fields
	var modulation pb_lorawan.Modulation
	var datarate string
	var bitrate uint32
	switch rxPacket.RXInfo.DataRate.Modulation {
	case band.LoRaModulation:
		modulation = pb_lorawan.Modulation_LORA
		datarate = fmt.Sprintf("SF%dBW%d", rxPacket.RXInfo.DataRate.SpreadFactor, rxPacket.RXInfo.DataRate.Bandwidth)
	case band.FSKModulation:
		modulation = pb_lorawan.Modulation_FSK
		bitrate = uint32(rxPacket.RXInfo.DataRate.BitRate)
	}

	return &router.UplinkMessage{
		Payload: rxPacket.PHYPayload,
		ProtocolMetadata: &protocol.RxMetadata{Protocol: &protocol.RxMetadata_Lorawan{Lorawan: &pb_lorawan.Metadata{
			Modulation: modulation,
			DataRate:   datarate,
			BitRate:    bitrate,
			CodingRate: rxPacket.RXInfo.CodeRate,
		}}},
		GatewayMetadata: &pb_gateway.RxMetadata{
			GatewayId: fmt.Sprintf("eui-%s", rxPacket.RXInfo.MAC),
			Timestamp: rxPacket.RXInfo.Timestamp,
			Time:      rxPacket.RXInfo.Time.UnixNano(),
			RfChain:   uint32(rxPacket.RXInfo.RFChain),
			Channel:   uint32(rxPacket.RXInfo.Channel),
			Frequency: uint64(rxPacket.RXInfo.Frequency),
			Rssi:      float32(rxPacket.RXInfo.RSSI),
			Snr:       float32(rxPacket.RXInfo.LoRaSNR),
		},
	}
}

func (c *PktFwd) convertTXPacket(in *types.DownlinkMessage) (*gw.TXPacketBytes, error) {
	var dataRate band.DataRate

	mac, err := c.getMAC(in.GatewayID)
	if err != nil {
		return nil, err
	}

	lora := in.Message.ProtocolConfiguration.GetLorawan()

	if lora.Modulation == pb_lorawan.Modulation_LORA {
		dr, _ := ttnTypes.ParseDataRate(lora.DataRate)
		dataRate.Modulation = band.LoRaModulation
		dataRate.SpreadFactor = int(dr.SpreadingFactor)
		dataRate.Bandwidth = int(dr.Bandwidth)
	}

	if lora.Modulation == pb_lorawan.Modulation_FSK {
		dataRate.Modulation = band.FSKModulation
		dataRate.BitRate = int(lora.BitRate)
	}

	txPacket := new(gw.TXPacketBytes)
	txPacket.TXInfo = gw.TXInfo{
		MAC:       mac,
		Timestamp: in.Message.GatewayConfiguration.Timestamp,
		Frequency: int(in.Message.GatewayConfiguration.Frequency),
		Power:     int(in.Message.GatewayConfiguration.Power),
		DataRate:  dataRate,
		CodeRate:  lora.CodingRate,
	}
	txPacket.PHYPayload = in.Message.Payload

	return txPacket, nil
}

func (c *PktFwd) convertStatsPacket(stats gw.GatewayStatsPacket) *pb_gateway.Status {
	status := new(pb_gateway.Status)

	status.Time = stats.Time.UnixNano()
	status.RxIn = uint32(stats.RXPacketsReceived)
	status.RxOk = uint32(stats.RXPacketsReceivedOK)

	if platform, ok := stats.CustomData["pfrm"]; ok {
		if platform, ok := platform.(string); ok {
			status.Platform = string(platform)
		}
	}
	if contactEmail, ok := stats.CustomData["mail"]; ok {
		if contactEmail, ok := contactEmail.(string); ok {
			status.ContactEmail = string(contactEmail)
		}
	}
	if description, ok := stats.CustomData["desc"]; ok {
		if description, ok := description.(string); ok {
			status.Description = string(description)
		}
	}
	if ip, ok := stats.CustomData["ip"]; ok {
		if ip, ok := ip.([]string); ok {
			status.Ip = ip
		}
	}
	if stats.Latitude != 0 || stats.Longitude != 0 || stats.Altitude != 0 {
		status.Gps = &pb_gateway.GPSMetadata{
			Latitude:  float32(stats.Latitude),
			Longitude: float32(stats.Longitude),
			Altitude:  int32(stats.Altitude),
		}
	}

	return status
}
