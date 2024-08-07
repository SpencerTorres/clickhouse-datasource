package plugin

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/grafana/dataplane/sdata/timeseries"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
	"github.com/grafana/sqlds/v3"
	"io"
	"time"
)

type RowBatchLimit struct {
	Size    int
	Timeout time.Duration
}

func getFrames(ctx context.Context, rows *sql.Rows, batchLimit RowBatchLimit, converters []sqlutil.Converter, fillMode *data.FillMissing, query *sqlutil.Query) (<-chan data.Frame, <-chan error) {
	framesChan := make(chan data.Frame, 10)
	errChan := make(chan error, 1)

	go func() {
		defer close(framesChan)
		defer close(errChan)

		for {
			frame, err := FrameFromRows(ctx, rows, batchLimit, converters...)
			if err != nil {
				if err == io.EOF {
					return // No more rows
				}
				errChan <- err
				return
			}

			if frame == nil || frame.Fields == nil || frame.Fields[0].Len() == 0 {
				continue // Skip empty frames
			}

			frame.Name = query.RefID
			frame.RefID = query.RefID
			if frame.Meta == nil {
				frame.Meta = &data.FrameMeta{}
			}
			frame.Meta.ExecutedQueryString = query.RawSQL
			frame.Meta.PreferredVisualization = data.VisTypeGraph

			count, err := frame.RowLen()
			if err != nil {
				errChan <- err
				return
			}

			zeroRows := count == 0

			switch query.Format {
			case sqlutil.FormatOptionMulti:
				if zeroRows {
					errChan <- sqlds.ErrorNoResults
					return
				}

				if frame.TimeSeriesSchema().Type == data.TimeSeriesTypeLong {
					err = fixFrameForLongToMulti(frame)
					if err != nil {
						errChan <- err
						return
					}

					frames, err := timeseries.LongToMulti(&timeseries.LongFrame{frame})
					if err != nil {
						errChan <- err
						return
					}
					for _, f := range frames.Frames() {
						framesChan <- *f
					}
					continue
				}
			case sqlutil.FormatOptionTable:
				frame.Meta.PreferredVisualization = data.VisTypeTable
			case sqlutil.FormatOptionLogs:
				frame.Meta.PreferredVisualization = data.VisTypeLogs
			case sqlutil.FormatOptionTrace:
				frame.Meta.PreferredVisualization = data.VisTypeTrace
			default: // Format as timeSeries
				if zeroRows {
					errChan <- sqlds.ErrorNoResults
					return
				}

				if frame.TimeSeriesSchema().Type == data.TimeSeriesTypeLong {
					frame, err = data.LongToWide(frame, fillMode)
					if err != nil {
						errChan <- err
						return
					}
				}
			}

			framesChan <- *frame
		}
	}()

	return framesChan, errChan
}

// fixFrameForLongToMulti edits the passed in frame so that it's first time field isn't nullable and has the correct meta
func fixFrameForLongToMulti(frame *data.Frame) error {
	if frame == nil {
		return fmt.Errorf("can not convert to wide series, input is nil")
	}

	timeFields := frame.TypeIndices(data.FieldTypeTime, data.FieldTypeNullableTime)
	if len(timeFields) == 0 {
		return fmt.Errorf("can not convert to wide series, input is missing a time field")
	}

	// the timeseries package expects the first time field in the frame to be non-nullable and ignores the rest
	timeField := frame.Fields[timeFields[0]]
	if timeField.Type() == data.FieldTypeNullableTime {
		newValues := []time.Time{}
		for i := 0; i < timeField.Len(); i++ {
			val, ok := timeField.ConcreteAt(i)
			if !ok {
				return fmt.Errorf("can not convert to wide series, input has null time values")
			}
			newValues = append(newValues, val.(time.Time))
		}
		newField := data.NewField(timeField.Name, timeField.Labels, newValues)
		newField.Config = timeField.Config
		frame.Fields[timeFields[0]] = newField

		// LongToMulti requires the meta to be set for the frame
		if frame.Meta == nil {
			frame.Meta = &data.FrameMeta{}
		}
		frame.Meta.Type = data.FrameTypeTimeSeriesLong
		frame.Meta.TypeVersion = data.FrameTypeVersion{0, 1}
	}
	return nil
}

func FrameFromRows(ctx context.Context, rows *sql.Rows, batchLimit RowBatchLimit, converters ...sqlutil.Converter) (*data.Frame, error) {
	var scanRow *sqlutil.RowConverter
	var frame *data.Frame

	rowCount := 0
	timeoutChan := time.After(batchLimit.Timeout)

	for {
		select {
		case <-ctx.Done():
			return frame, ctx.Err()
		case <-timeoutChan:
			return frame, nil
		default:
			if !rows.Next() {
				if err := rows.Err(); err != nil {
					return frame, err
				}
				if !rows.NextResultSet() {
					if rowCount > 0 {
						return frame, nil
					}
					return frame, io.EOF
				}
				rowCount = 0
				continue
			}

			if scanRow == nil {
				var err error
				frame, scanRow, err = newFrameAndRowConverter(rows, converters...)
				if err != nil {
					return nil, err
				}
			}

			r := scanRow.NewScannableRow()
			if err := rows.Scan(r...); err != nil {
				return frame, err
			}

			if err := sqlutil.Append(frame, r, scanRow.Converters...); err != nil {
				return frame, err
			}

			rowCount++
			if rowCount >= batchLimit.Size {
				return frame, nil
			}
		}
	}
}

// initialize a new frame and row converter. Should call rows.Next first.
func newFrameAndRowConverter(rows *sql.Rows, converters ...sqlutil.Converter) (*data.Frame, *sqlutil.RowConverter, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	names, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	scanRow, err := sqlutil.MakeScanRow(types, names, converters...)
	if err != nil {
		return nil, nil, err
	}

	frame := sqlutil.NewFrame(names, scanRow.Converters...)
	return frame, scanRow, nil
}
