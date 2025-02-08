import { useEffect } from 'react';
import { getGrafanaLiveSrv } from "@grafana/runtime";
import { DataQueryResponse, LiveChannelEventType, LiveChannelScope } from '@grafana/data';
import { switchMap, of, takeWhile, finalize, catchError } from "rxjs";

interface CustomLiveChannelEvent {
	type: 'progress' | 'data' | 'error' | 'complete';
	payload?: any;
}

export function useClickHouseStreamer(datasourceID: string) {
	// eslint-disable-next-line
	useEffect(() => {
		let hasEnded = false;

		const streamEvents = getGrafanaLiveSrv().getStream<CustomLiveChannelEvent>({
			scope: LiveChannelScope.DataSource,
			path: 'clickhouse',
			namespace: datasourceID,
			data: {}
		});

		streamEvents.pipe(
			takeWhile(() => !hasEnded),
			switchMap((event) => {
			  if (event.type !== LiveChannelEventType.Message) {
				return of(null);
			  }
			  if ((event.message as any).progress) {
				event.message.type = 'progress';
			  } else if ((event.message as any).completed) {
				event.message.type = 'complete';
			  } else {
				event.message.type = 'data';
			  }
			  switch (event.message.type) {
				case 'progress':
				  console.log('Progress:', event.message.payload);
				  return of(null);
				case 'complete':
				  hasEnded = true;
				  return of(null);
				  // return of({ data: [], key: channelId, state: LoadingState.Streaming } as DataQueryResponse);
				default:
				  return of(null);
			  }
			}),
			catchError((error) => {
			  console.error('Query error:', error);
			  return of({ data: [], error: { message: error.message }, key: 'clickhouse' } as DataQueryResponse);
			}),
			finalize(() => {
			  console.log(`Stream completed or errored`);
			})
		);
	}, []);
}
