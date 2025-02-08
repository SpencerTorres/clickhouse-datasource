package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/sqlds/v4"
)

type chStreamSource struct {
	*sqlds.SQLDatasource
	connector *sqlds.Connector
	driver    sqlds.Driver
	Progress  chan ProgressPacket
}

var (
	_ backend.CheckHealthHandler    = (*chStreamSource)(nil)
	_ instancemgmt.InstanceDisposer = (*chStreamSource)(nil)
	_ backend.StreamHandler         = (*chStreamSource)(nil)
)

func (ds *chStreamSource) CheckHealth(_ context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Data source is working",
	}, nil
}

func (ds *chStreamSource) Dispose() {}

func (ds *chStreamSource) SubscribeStream(context.Context, *backend.SubscribeStreamRequest) (*backend.SubscribeStreamResponse, error) {
	return &backend.SubscribeStreamResponse{
		Status: backend.SubscribeStreamStatusOK,
	}, nil
}

func (ds *chStreamSource) PublishStream(context.Context, *backend.PublishStreamRequest) (*backend.PublishStreamResponse, error) {
	return &backend.PublishStreamResponse{
		Status: backend.PublishStreamStatusPermissionDenied,
	}, nil
}

type ErrorPacket struct {
	Error string `json:"error"`
}

type ProgressPacket struct {
	QueryID   string `json:"query_id"`
	Rows      uint64 `json:"rows"`
	Bytes     uint64 `json:"bytes"`
	ElapsedMs uint64 `json:"elapsed_ms"`
}

type CompletedPacket struct {
	Completed bool `json:"completed"`
}

func (ds *chStreamSource) RunStream(ctx context.Context, req *backend.RunStreamRequest, sender *backend.StreamSender) error {
	fmt.Println("STREAM STARTED")
	for {
		select {
		case prog, ok := <-ds.Progress:
			if !ok {
				return nil
			}

			fmt.Printf("PROGRESS PACKET %+v\n", prog)

			data, err := json.Marshal(prog)
			if err != nil {
				return err
			}

			err = sender.SendJSON(data)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
