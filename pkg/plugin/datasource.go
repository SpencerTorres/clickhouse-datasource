package plugin

import (
	"context"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/sqlds/v4"
)

func NewDatasource(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	progressChan := make(chan ProgressPacket, 512)
	clickhousePlugin := Clickhouse{
		Progress: progressChan,
	}
	ds := sqlds.NewDatasource(&clickhousePlugin)
	pluginSettings := clickhousePlugin.Settings(ctx, settings)
	if pluginSettings.ForwardHeaders {
		ds.EnableMultipleConnections = true
	}

	_, err := ds.NewDatasource(ctx, settings)
	if err != nil {
		return nil, err
	}

	connector, err := sqlds.NewConnector(ctx, &clickhousePlugin, settings, ds.EnableMultipleConnections)
	if err != nil {
		return nil, err
	}

	return &chStreamSource{
		SQLDatasource: ds,
		connector:     connector,
		driver:        &clickhousePlugin,
		Progress:      progressChan,
	}, nil
}
