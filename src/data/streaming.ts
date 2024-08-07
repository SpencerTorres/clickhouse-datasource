import { DataFrameJSON, DataQueryRequest, DataQueryResponse, LiveChannelEventType, LiveChannelScope, LoadingState, dataFrameFromJSON } from "@grafana/data";
import { getGrafanaLiveSrv } from "@grafana/runtime";
import { CHQuery } from "types/sql";
import { Observable, merge, switchMap, of, 
  // throwError, 
  takeWhile, finalize, catchError, map, filter } from "rxjs";
import { transformQueryResponseWithTraceAndLogLinks } from "./utils";
import { Datasource } from "./CHDatasource";

interface CustomLiveChannelEvent {
  type: 'progress' | 'data' | 'error' | 'complete';
  payload?: any;
}

export function queryStream(datasource: Datasource, request: DataQueryRequest<CHQuery>): Observable<DataQueryResponse> {
  console.log(request);
  const targets = request.targets
      .filter((t) => t.hide !== true)
      .map((t) => ({
        ...t,
        meta: {
          ...t?.meta,
          timezone: datasource.getTimezone(request),
        },
      }));

    const observables = targets.map((query, index) => {
      const channelId = `query-${request.requestId}-${index}-${Math.random()}`;
      let hasEnded = false;

      return getGrafanaLiveSrv().getStream<CustomLiveChannelEvent>({
        scope: LiveChannelScope.DataSource,
        namespace: datasource.uid,
        path: `my-ws/${channelId}`,
        data: {
          fromTime: request.startTime,
          toTime: request.endTime,
          intervalMs: request.intervalMs,
          maxDataPoints: request.maxDataPoints,
          ...query
        }
      }).pipe(
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
            case 'data':
              return of({
                data: [dataFrameFromJSON(event.message as DataFrameJSON)],
                key: channelId,
                state: LoadingState.Done
              } as DataQueryResponse);
            // case 'error':
            //   hasEnded = true;
            //   return throwError(() => new Error(event.message.payload));
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
          return of({ data: [], error: { message: error.message }, key: channelId } as DataQueryResponse);
        }),
        finalize(() => {
          console.log(`Query ${channelId} completed or errored`);
        })
      );
    });

    return merge(...observables).pipe(
      filter((response): response is DataQueryResponse => response !== null),
      map((res: DataQueryResponse) => transformQueryResponseWithTraceAndLogLinks(datasource, request, res)),
      catchError((error) => {
        console.error('Global error:', error);
        return of({ data: [], error: { message: 'An error occurred while fetching data' } } as DataQueryResponse);
      })
    );
}
