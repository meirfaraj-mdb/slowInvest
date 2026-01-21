import logging
import time
from sl_async.slapi import  SlSource, SourceFilter, DefaultSourceFilter
from sl_json.json import get_time_from_line
from datetime import datetime, timezone
import isodate

sl_atlas_log = logging.getLogger("sl_atlas")


class BufferedSlAtlasSource(SlSource):

    def __init__(self,
                 atlas,
                 groupId,
                 processId,
                 line_buffer_size=500,
                 dtime=None,
                 line_filter: SourceFilter =None):
        super().__init__(path=processId,max_queue_size=line_buffer_size)
        self.atlas=atlas
        self.line_filter=DefaultSourceFilter(self.queue) if line_filter is None else line_filter
        self.groupId=groupId
        self.processId=processId
        self.dtime=dtime
        self.valueHigh = 15_000

    def set_dtime(self,dtime):
        self.dtime=dtime


    def iso8601_to_duration_ms(self,value: str) -> int:
        """
        Converts ISO8601 date or duration string to duration in milliseconds (int).
        - Durations are returned directly in ms.
        - Date/times are treated as duration from now (now - date).
          If date is in the past => positive duration; future => negative.
        """
        # Current UTC time for duration reference
        now_utc = datetime.now(timezone.utc)

        try:
            # ISO8601 duration format starts with 'P'
            if value.startswith("P"):
                duration = isodate.parse_duration(value)

                # If months/years involved, .parse_duration() returns isodate.Duration
                if isinstance(duration, isodate.Duration):
                    target_date = now_utc + duration  # Add to now to resolve months/years
                    duration_sec = (target_date - now_utc).total_seconds()
                else:
                    duration_sec = duration.total_seconds()

                return int(duration_sec * 1000)

            else:
                # Treat as ISO8601 datetime
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                duration_sec = (now_utc - dt).total_seconds()
                return int(duration_sec * 1000)

        except Exception as e:
            raise ValueError(f"Unable to parse ISO8601 value '{value}': {e}")

    async def task_fn(self):
        last_count = -1
        it=0
        total_loaded=0
        while last_count <0 or last_count>=self.valueHigh :
            path=f"/groups/{self.groupId}/processes/{self.processId}/performanceAdvisor/slowQueryLogs"
            arg={}
            it+=1
            take_report_date_from = self.atlas.config.get_config("atlas.take_from", None)
            if  take_report_date_from == "last":
                if not (self.dtime is None):
                    since=str(int(time.mktime(self.dtime.timetuple())*1000))
                    print(f"executing slowQuery {self.processId} : since : {since}")
                    arg={"since": since}
            else :
                if not (self.dtime is None):
                    since=str(int(time.mktime(self.dtime.timetuple())*1000))
                    print(f"executing slowQuery {self.processId} : {take_report_date_from} . ignored : since : {since}")
                if not (take_report_date_from == "PT24H" or take_report_date_from is None):
                    duration = self.iso8601_to_duration_ms(take_report_date_from)
                    print(f"executing slowQuery {self.processId} : duration:{duration}")
                    arg={"duration": duration}

            resp=self.atlas.atlas_request('SlowQueries', path, '2023-01-01', arg)
            last_count=len(resp['slowQueries'])
            total_loaded+=last_count
            for entry in resp['slowQueries']:
                await self.line_filter.process(entry.get('line',""))
            if last_count>=self.valueHigh:
                last_entry=resp['slowQueries'][-1].get('line',"")
                self.dtime = get_time_from_line(last_entry)


        sl_atlas_log.info(f"read {self.path} complete after {it} iteration, loaded {total_loaded}")
        await self.queue.put(None)

    async def close(self):
        pass
