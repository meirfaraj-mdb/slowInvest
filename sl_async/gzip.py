import io
import logging
import zlib

import aiofile
import cramjam
from aiofile import async_open

from sl_async.slapi import  SlSource, SourceFilter,SlDest, DefaultSourceFilter

reader_log = logging.getLogger()

class BufferedGzipReader(SlSource):

    def __init__(self,
                 file_path,
                 line_buffer_size=500,
                 line_filter: SourceFilter =None):
        super().__init__(path=file_path,max_queue_size=line_buffer_size)
        self.line_filter=DefaultSourceFilter(self.queue) if line_filter is None else line_filter

    async def async_read_gzip_lines(self):
        decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)  # Set up the decompressor with gzip header support
        async with async_open(self.path, mode='rb') as f:
            buffer = b''  # Buffer to hold incomplete lines across chunks
            while True:
                chunk = await f.read(64*1024)  # Read raw compressed data in chunks
                if not chunk:
                    break
                try:
                    decompressed_data = decompressor.decompress(chunk)
                except zlib.error as err:
                    logging.warning("Decompression error occurred", exc_info=err)
                    break
                buffer += decompressed_data  # Add decompressed data to buffer
                lines = buffer.split(b'\n')  # Split the buffer into lines
                buffer = lines[-1]  # Retain any incomplete line in the buffer
                for line in lines[:-1]:
                    await self.line_filter.process(line.decode('utf-8'))
            # Flush any remaining data in the decompressor
            try:
                remaining_data = decompressor.flush()
                if remaining_data:
                    buffer += remaining_data
            except zlib.error as err:
                logging.warning("Final data flush error occurred", exc_info=err)
            # Process any remaining data in the buffer
            if buffer:
                await self.line_filter.process(buffer.decode('utf-8'))

    async def task_fn(self):
        if self.path.endswith('.gz'):
            await self.async_read_gzip_lines()
        else:
            async with async_open(self.path, 'rt', buffering=8 * io.DEFAULT_BUFFER_SIZE) as log_file:
                async for line in log_file:
                    await self.line_filter.process( line)
        logging.info(f"read {self.path} complete")
        await self.queue.put(None)


    async def close(self):
        pass



class BufferedGzipWriter(SlDest):
    def __init__(self,
                 file_path,
                 max_queue_size=50):
        super().__init__(
            path=file_path,
            max_queue_size=max_queue_size)
        self.compressor = cramjam.gzip.Compressor()
        self.buffer = io.BytesIO()
        self.file = None

    async def __aenter__(self):
        return self

    async def task_fn(self):
        self.file = await aiofile.async_open(self.path, 'wb')
        await self._writer_task()
        await self.close()

    async def write(self, data):
        await self.queue.put(data)

    def queue_size(self):
        return self.queue.qsize()

    async def _writer_task(self):
        while True:
            data = await self.queue.get()
            if data is None:  # Sentinel value for termination
                await self.file.write(bytes(self.compressor.compress(self.buffer.getvalue())))
                #loggging
                break
            self.buffer.write(data.encode('utf-8'))
            if self.buffer.tell()>4*1024*1024 :
               await self.file.write(bytes(self.compressor.compress(self.buffer.getvalue())))
               self.buffer.truncate(4*1024*1024)
               self.buffer.seek(0)
            self.queue.task_done()

    async def close(self):
        if self.task:
            await self.queue.put(None)  # Signal the writer_task to terminate
            await self.file.write(bytes(self.compressor.finish()))
            self.buffer.close()
            await self.file.close()
        logging.info(f"Closed Gzip writer {self.path}")
        self.buffer=None
        self.queue=None
        self.compressor = None
        self.file = None
        await super().close()