package plugin

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
	"github.com/grafana/sqlds/v3"
	"io"
	"reflect"
	"time"
	"unsafe"
)

type testDs struct {
	*sqlds.SQLDatasource
	connector *sqlds.Connector
	driver    sqlds.Driver
}

var (
	_ backend.CheckHealthHandler    = (*testDs)(nil)
	_ instancemgmt.InstanceDisposer = (*testDs)(nil)
	_ backend.StreamHandler         = (*testDs)(nil)
)

func (ds *testDs) NotQueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	refID := req.Queries[0].RefID

	qdr := backend.NewQueryDataResponse()

	fieldValues := []float64{1, 2, 3, 4}
	field := data.NewField("a", nil, fieldValues)
	fields := []*data.Field{field}

	frames := make(data.Frames, 1)
	frames[0] = &data.Frame{
		Name:   "test",
		Fields: fields,
		RefID:  refID,
		Meta:   nil,
	}

	qdr.Responses[refID] = backend.DataResponse{
		Frames:      frames,
		Error:       nil,
		Status:      backend.StatusOK,
		ErrorSource: "",
	}

	return qdr, nil
}

func (ds *testDs) CheckHealth(_ context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Data source is working",
	}, nil
}

func (ds *testDs) Dispose() {}

func (ds *testDs) SubscribeStream(context.Context, *backend.SubscribeStreamRequest) (*backend.SubscribeStreamResponse, error) {
	return &backend.SubscribeStreamResponse{
		Status: backend.SubscribeStreamStatusOK,
	}, nil
}

func (ds *testDs) PublishStream(context.Context, *backend.PublishStreamRequest) (*backend.PublishStreamResponse, error) {
	return &backend.PublishStreamResponse{
		Status: backend.PublishStreamStatusPermissionDenied,
	}, nil
}

type StreamableQuery struct {
	RefID         string                    `json:"refId"`
	QueryType     string                    `json:"queryType"`
	Format        sqlutil.FormatQueryOption `json:"format"`
	FromTime      int64                     `json:"fromTime"`
	ToTime        int64                     `json:"toTime"`
	IntervalMs    int64                     `json:"intervalMs"`
	MaxDataPoints int64                     `json:"maxDataPoints"`
	RawSQL        string                    `json:"rawSql"`
	FillMissing   *data.FillMissing         `json:"fillMode"`
}

func (q *StreamableQuery) DataQuery(otherJSON json.RawMessage) backend.DataQuery {
	return backend.DataQuery{
		RefID:         q.RefID,
		QueryType:     q.QueryType,
		MaxDataPoints: q.MaxDataPoints,
		Interval:      time.Duration(q.IntervalMs) * time.Millisecond,
		TimeRange: backend.TimeRange{
			From: time.UnixMilli(q.FromTime),
			To:   time.UnixMilli(q.ToTime),
		},
		JSON: otherJSON,
	}
}

type ErrorPacket struct {
	Error string `json:"error"`
}

type ProgressPacket struct {
	Progress float64 `json:"progress"`
}

type CompletedPacket struct {
	Completed bool `json:"completed"`
}

func (ds *testDs) RunStream(ctx context.Context, req *backend.RunStreamRequest, sender *backend.StreamSender) error {
	sq := StreamableQuery{}
	err := json.Unmarshal(req.Data, &sq)
	if err != nil {
		return err
	}

	dataQuery := sq.DataQuery(req.Data)
	if queryMutator, ok := ds.driver.(sqlds.QueryMutator); ok {
		ctx, dataQuery = queryMutator.MutateQuery(ctx, dataQuery)
	}

	q, err := sqlutil.GetQuery(dataQuery)
	if err != nil {
		return err
	}

	q.RawSQL, err = sqlutil.Interpolate(q, ds.driver.Macros())
	if err != nil {
		// TODO: SEND ERROR FRAME
		sqlutil.ErrorFrameFromQuery(q)
		return fmt.Errorf("%s: %w", "could not apply macros", err)
	}

	fillMode := ds.DriverSettings().FillMode
	if q.FillMissing != nil {
		fillMode = q.FillMissing
	}

	_, dbConn, err := ds.connector.GetConnectionFromQuery(ctx, q)
	if err != nil {
		// TODO: SEND ERROR FRAME
		sqlutil.ErrorFrameFromQuery(q)
		return err
	}
	dbField := reflect.ValueOf(&dbConn).Elem().Field(0)
	dbConnDB := *reflect.NewAt(dbField.Type(), unsafe.Pointer(dbField.UnsafeAddr())).Interface().(**sql.DB)

	if ds.DriverSettings().Timeout != 0 {
		tctx, cancel := context.WithTimeout(ctx, ds.DriverSettings().Timeout)
		defer cancel()

		ctx = tctx
	}

	var args []interface{}
	if argSetter, ok := ds.driver.(sqlds.QueryArgSetter); ok {
		args = argSetter.SetQueryArgs(ctx, nil)
	}

	rows, err := dbConnDB.QueryContext(ctx, q.RawSQL, args...)
	if err != nil {
		errType := sqlds.ErrorQuery
		if errors.Is(err, context.Canceled) {
			errType = context.Canceled
		}
		errWithSource := sqlds.DownstreamError(fmt.Errorf("%w: %s", errType, err.Error()))
		// TODO: SEND ERROR FRAME
		sqlutil.ErrorFrameFromQuery(q)
		return errWithSource
	} else if errors.Is(err, sqlds.ErrorNoResults) {
		// TODO: return res
		return nil
	}

	if err := rows.Err(); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Should we even response with an error here?
			// The panel will simply show "no data"
			errWithSource := sqlds.DownstreamError(fmt.Errorf("%s: %w", "no results from query", err))
			// TODO: SEND ERROR FRAME
			sqlutil.ErrorFrameFromQuery(q)
			return errWithSource
		}
		errWithSource := sqlds.DownstreamError(fmt.Errorf("%s: %w", "error response from database", err))

		// TODO: SEND ERROR FRAME
		sqlutil.ErrorFrameFromQuery(q)
		return errWithSource
	}

	defer func() {
		if err := rows.Close(); err != nil {
			backend.Logger.Error(err.Error())
		}
	}()

	batchLimit := RowBatchLimit{
		Size:    1000,
		Timeout: 50 * time.Millisecond,
	}

	framesChan, errChan := getFrames(ctx, rows, batchLimit, ds.driver.Converters(), fillMode, q)

	progressPacket := &ProgressPacket{0.65}
	blah, _ := json.Marshal(&progressPacket)
	sender.SendJSON(blah)

	defer func() {
		completedPacket := &CompletedPacket{true}
		blah, _ := json.Marshal(&completedPacket)
		sender.SendJSON(blah)
	}()

	for {
		select {
		case frame, ok := <-framesChan:
			if !ok {
				return nil // Channel closed, we're done
			}
			err := sender.SendFrame(&frame, data.IncludeAll)
			if err != nil {
				return err
			}
		case err := <-errChan:
			if err == io.EOF || err == nil {
				return nil
			}
			errFrame := sqlutil.ErrorFrameFromQuery(q)[0]
			sendErr := sender.SendFrame(errFrame, data.IncludeAll)
			if sendErr != nil {
				return sendErr
			}
			return sqlds.PluginError(fmt.Errorf("could not process SQL results: %w", err))
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func NewDatasource(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	clickhousePlugin := Clickhouse{}
	ds := sqlds.NewDatasource(&clickhousePlugin)
	pluginSettings := clickhousePlugin.Settings(ctx, settings)
	if pluginSettings.ForwardHeaders {
		ds.EnableMultipleConnections = true
	}
	ds.NewDatasource(ctx, settings)

	connector, _ := sqlds.NewConnector(ctx, &clickhousePlugin, settings, ds.EnableMultipleConnections)
	return &testDs{
		SQLDatasource: ds,
		connector:     connector,
		driver:        &clickhousePlugin,
	}, nil
}
