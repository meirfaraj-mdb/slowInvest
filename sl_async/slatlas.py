import logging
import time
from sl_async.slapi import  SlSource, SourceFilter, DefaultSourceFilter
from sl_json.json import get_time_from_line

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

    async def task_fn(self):
        last_count = -1
        it=0
        total_loaded=0
        while last_count <0 or last_count>=self.valueHigh :
            path=f"/groups/{self.groupId}/processes/{self.processId}/performanceAdvisor/slowQueryLogs"
            arg={}
            it+=1
            if not (self.dtime is None):
                if self.atlas.config.get_config("atlas.take_report_date_from", None) == "last":
                    since=str(int(time.mktime(self.dtime.timetuple())*1000))
                    print(f"executing slowQuery {self.processId} : since : {since}")
                    arg={"since": since}
                else :
                    since=str(int(time.mktime(self.dtime.timetuple())*1000))
                    print(f"executing slowQuery {self.processId} : 24H ignored : since : {since}")
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
