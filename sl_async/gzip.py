import io
import logging
import os
import struct
import time
import zlib

import aiofile
from aiofile import async_open

from sl_async.slapi import SlSource, SourceFilter, SlDest, DefaultSourceFilter
from sl_json.json import get_time_from_line

reader_log = logging.getLogger("BufferedGzipReader")
writer_log = logging.getLogger("BufferedGzipWriter")
writer_log.setLevel(logging.DEBUG)
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
                    reader_log.warning("Decompression error occurred", exc_info=err)
                    break
                buffer += decompressed_data  # Add decompressed data to buffer
                lines = buffer.split(b'\n')  # Split the buffer into lines
                buffer = lines[-1]  # Retain any incomplete line in the buffer
                for line in lines[:-1]:
                    await self.line_filter.process(line.decode('utf-8'))
                dtime = get_time_from_line(lines[-2])

        # Flush any remaining data in the decompressor
            try:
                remaining_data = decompressor.flush()
                if remaining_data:
                    buffer += remaining_data
            except zlib.error as err:
                reader_log.warning("Final data flush error occurred", exc_info=err)
            # Process any remaining data in the buffer
            if buffer:
                await self.line_filter.process(buffer.decode('utf-8'))

    async def task_fn(self):
        if self.path.endswith('.gz'):
            await self.async_read_gzip_lines()
        else:
            async with async_open(self.path, 'rt') as log_file:
                async for line in log_file:
                    await self.line_filter.process( line)
        reader_log.info(f"read {self.path} complete")
        await self.queue.put(None)

    async def close(self):
        pass




FTEXT, FHCRC, FEXTRA, FNAME, FCOMMENT = 1, 2, 4, 8, 16

_COMPRESS_LEVEL_FAST = 1
_COMPRESS_LEVEL_TRADEOFF = 6
_COMPRESS_LEVEL_BEST = 9

class BufferedGzipWriter(SlDest):
    GZIP_HEADER = b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff'  # Gzip magic number and header
    GZIP_FOOTER_SIZE = 8
    def __init__(self, file_path, max_queue_size=50):
        super().__init__(path=self.modify_path(file_path), max_queue_size=max_queue_size)

        self.compresslevel=_COMPRESS_LEVEL_TRADEOFF
        self.compressor = zlib.compressobj(self.compresslevel,
                                         zlib.DEFLATED,
                                         -zlib.MAX_WBITS,
                                         zlib.DEF_MEM_LEVEL,
                                         0)
        self.buffer = io.BytesIO()
        self.file = None
        self.crc32 = zlib.crc32(b"")
        self.offset = 0  # Current file offset for seek(), tell(), etc
        self.input_size = 0
        self._write_mtime = None


    def modify_path(self, path):
        # Replace ':' with '_'
        path = path.replace(':', '_')
        # Check if path ends with '.gz', if not, append it
        if not path.endswith('.gz'):
            path += '.gz'
        return path

    async def __aenter__(self):
        return self

    async def _write_gzip_header(self, compresslevel):
        await self.file.write(b'\037\213')             # magic header
        await self.file.write(b'\010')                 # compression method
        try:
            # RFC 1952 requires the FNAME field to be Latin-1. Do not
            # include filenames that cannot be represented that way.
            fname = os.path.basename(self.get_path())
            if not isinstance(fname, bytes):
                fname = fname.encode('latin-1')
            if fname.endswith(b'.gz'):
                fname = fname[:-3]
        except UnicodeEncodeError:
            fname = b''
        flags = 0
        if fname:
            flags = FNAME
        await self.file.write(chr(flags).encode('latin-1'))
        mtime = self._write_mtime
        if mtime is None:
            mtime = time.time()
        await self.file.write(struct.pack("<L", int(mtime)))
        if compresslevel == _COMPRESS_LEVEL_BEST:
            xfl = b'\002'
        elif compresslevel == _COMPRESS_LEVEL_FAST:
            xfl = b'\004'
        else:
            xfl = b'\000'
        await self.file.write(xfl)
        await self.file.write(b'\377')
        if fname:
            await self.file.write(fname + b'\000')





    async def task_fn(self):
        self.file = await aiofile.async_open(self.path, 'wb')

        # Write the Gzip header at the beginning of the file
        await self._write_gzip_header(6)
        #await self.file.write(self.GZIP_HEADER)
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
                break
            # Write data to the internal buffer
            self.buffer.write(data.encode('utf-8'))
            # Flush the buffer if size exceeds 4MB
            if self.buffer.tell() > 4 * 1024 * 1024:
                await self._flush_buffer()
            self.queue.task_done()
        # Flush remaining data in the buffer, if any
        await self._flush_buffer()
        compressed_data = self.compressor.flush(zlib.Z_FINISH)
        await self.file.write(compressed_data)
        # Write the Gzip footer (CRC32 and input size)
        await self._build_gzip_footer()
        path_str=self.get_path()
        writer_log.info(f"All writes to {path_str} finished")
    async def _flush_buffer(self):
        if self.buffer.tell() > 0:
            # Compress the buffer contents
            dataBytes = self.buffer.getvalue()
            self.crc32 = zlib.crc32(dataBytes, self.crc32)
            self.input_size += len(dataBytes)
            compressed_data = self.compressor.compress(dataBytes)
            await self.file.write(compressed_data)
            # Reset the buffer
            self.buffer.seek(0)
            self.buffer.truncate(0)
    async def _build_gzip_footer(self):
        # Gzip footer contains CRC32 (4 bytes) and input size (4 bytes, modulo 2^32)
        await self.file.write(struct.pack("<L", self.crc32))
        await self.file.write(struct.pack("<L", self.input_size & 0xFFFFFFFF))

    async def close(self):
        if self.queue is not None:
            await self.queue.put(None)  # Signal t      he writer_task to terminate
            # Close the compression stream
            if self.file is not None:
                await self.file.close()
            writer_log.info(f"Closed Gzip writer {self.path}")
            self.buffer.close()
        self.queue = None
        self.compressor = None
        self.file = None
        await super().close()