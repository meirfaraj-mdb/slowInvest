import asyncio
import logging
import os
import time
from concurrent import futures
from datetime import datetime

import msgspec

from sl_async.gzip import BufferedGzipReader, BufferedGzipWriter
from sl_json.json import JsonAndText
from sl_async.slag import append_to_parquet
from sl_utils.utils import convertToHumanReadable,remove_extension,createDirs

import msgspec

decoder = msgspec.json.Decoder()




# Function for extracting from File :
def extract_slow_queries_from_file(
        log_file_path,
        output_file_path,
        chunk_size=200000,
        save_by_chunk="none",
        display_at=200000):
    parquet_file_path_base=f"{remove_extension(output_file_path)}/"
    createDirs(parquet_file_path_base)
    src= BufferedGzipReader(log_file_path)
    dest= BufferedGzipWriter(output_file_path)
    orch=AsyncExtractAndAggregate(src,dest,parquet_file_path_base,chunk_size,save_by_chunk  )
    orch.run()
    return orch.get_results()

class AsyncExtractAndAggregate:
    def __init__(self,
                 source,
                 dest=None,
                 parquet_file_path_base=None,
                 chunk_size=200000,
                 save_by_chunk="none",
                 display_at=200000,
                 line_buffer_size=500
                 ):
        self.source=source
        self.dest=dest
        self.queue_decoded = asyncio.Queue(3*line_buffer_size)
        self.queue_source=self.source.get_queue()
        self.source_task=None
        self.consumer_task = None
        self.aggreg_task = None
        self.chunk_size=chunk_size
        self.save_by_chunk=save_by_chunk
        self.display_at=display_at
        self.parquet_file_path_base=parquet_file_path_base
        self.result=self.init_result(parquet_file_path_base)
        self.pool = futures.ThreadPoolExecutor(max_workers=1)
        self.lastPrint=0
        self.lastHours=None

    def init_result(self,file_path_base):
        result= {"countOfSlow": 0, "systemSkipped": 0, "groupByCommandShape": {}, "groupByCommandShapeChangeStream": {},
                 "resume": {}}
        if os.path.isfile(f"{file_path_base}resume.json"):
            with open(f"{file_path_base}resume.json") as out_file:
                read=out_file.read()
                if len(read)>10:
                    result["resume"]= decoder.decode(read)
            dtime=result["resume"].get("dtime",None)
            if dtime is not None:
                result["resume"]["dtime"]=datetime.fromisoformat(dtime)
        return result


    def run(self):
        asyncio.run(self.internal())

    async def decode(self):
        while True:
            item = await self.queue_source.get()
            if item is None:
                await self.queue_decoded.put(None)
                self.queue_source.task_done()
                break
            # logging.info(f"added {item}")
            await self.queue_decoded.put(JsonAndText(item))
            self.queue_source.task_done()
        logging.info(f"Decode ended for {self.source.get_name()}")

    async def bufferAggregate(self):
        it=0
        start_time = time.time()
        data = []
        dtime = self.result.get("resume",{}).get(None)
        future=None
        while True:
            try:
                if self.queue_decoded.qsize()>50 :
                    dline = self.queue_decoded.get_nowait()
                else:
                    dline = await self.queue_decoded.get()
                if dline is None:
                    self.queue_decoded.task_done()
                    break
                dtime,dhour,log_entry,orig_line = dline.decode()
                dline.clear()
                if log_entry is not None:
                    if self.lastHours is None :
                        self.lastHours = dhour
                    if self.lastHours != dhour or len(data) >= self.chunk_size:
                        dump_aggregation=(self.lastHours != dhour)
                        self.lastHours = dhour
                        it+=1
                        future=self.pool.submit(append_to_parquet,data, self.parquet_file_path_base,dtime, it,self.save_by_chunk,dump_aggregation,self.result)
                        data = []  # Clear list to free memory
                        if self.result["countOfSlow"]-self.lastPrint>0 and self.result["countOfSlow"]-self.lastPrint>self.display_at:
                            self.lastPrint=self.result["countOfSlow"]
                            end_time = time.time()
                            elapsed_time_ms = (end_time - start_time) * 1000
                            logging.info(f"loaded {self.result["countOfSlow"]} slow queries in {convertToHumanReadable("Millis",elapsed_time_ms)} it={it}"+
                                         f" Q={self.source.get_queue().qsize()}|{self.queue_decoded.qsize()}|{self.dest.queue_size()} {int(round(self.result["countOfSlow"]/(elapsed_time_ms/1000)))}SQPS")
                    data.append(log_entry)
                    if log_entry:
                        await self.dest.write(orig_line)
                    else:
                        self.result["systemSkipped"]+=1
                self.queue_decoded.task_done()
            except msgspec.MsgspecError:
                # Skip lines that are not valid JSON
                self.queue_decoded.task_done()
                continue
        await self.dest.notify_write_end()
        # Handle any remaining data
        logging.info("Finishing global aggregation")
        if data:
            it+=1
            future=self.pool.submit(append_to_parquet,data, self.parquet_file_path_base,dtime,it,self.save_by_chunk,True,self.result,True)
        start_waiting=time.time()
        future.result()
        end_time = time.time()
        elapsed_time_ms = (end_time - start_waiting) * 1000
        logging.info(f"waiting for last pool took {convertToHumanReadable("Millis",elapsed_time_ms)}")
        self.pool.shutdown(wait=True)
        end_time = time.time()
        elapsed_time_ms = (end_time - start_time) * 1000
        logging.info(f"Extracted {self.result["countOfSlow"]} slow queries have been saved to {self.dest.get_path()} and {self.parquet_file_path_base} in {convertToHumanReadable("Millis",elapsed_time_ms)}")


    async def internal(self):
        self.consumer_task = asyncio.create_task(self.decode())
        self.aggreg_task = asyncio.create_task(self.bufferAggregate())
        self.source.create_task()
        self.source_task=self.source.get_task()
        if self.dest is None:
            await asyncio.gather(self.source_task,self.consumer_task,self.aggreg_task)
        else:
            self.dest.create_task()
            await asyncio.gather(self.source_task,self.consumer_task,self.aggreg_task,self.dest.get_task())
        #await self.queue.join()
        #await self.queue.put(None)  # Signal the consumer to stop

    def get_results(self):
        return self.result