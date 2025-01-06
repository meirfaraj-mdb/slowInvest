import logging
import time
from sl_async.slapi import  SlSource, SourceFilter, DefaultSourceFilter

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

    def set_dtime(self,dtime):
        self.dtime=dtime

    async def task_fn(self):
        last_count = -1

        while last_count <0 or last_count>=15000 :
            path=f"/groups/{self.groupId}/processes/{self.processId}/performanceAdvisor/slowQueryLogs"
            arg={}
            if not (self.dtime is None):
                since=str(int(time.mktime(self.dtime.timetuple())*1000))
                print(f"executing slowQuery {self.processId} : since : {since}")
                arg={"since": since}
            resp=self.atlas.atlas_request('SlowQueries', path, '2023-01-01', arg)
            last_count=len(resp['slowQueries'])
            if last_count>=15000:
                last_entry=resp['slowQueries'][-1]
            for entry in resp['slowQueries']:
                await self.line_filter.process(entry.get('line',""))
        logging.info(f"read {self.path} complete")
        await self.queue.put(None)

    async def close(self):
        pass
