import asyncio
import logging

class SLClosable:
    async def close(self):
        pass

class SLTask(SLClosable):
    def __init__(self,max_queue_size):
        super().__init__()
        self.max_queue_size=max_queue_size
        self.queue = asyncio.Queue(max_queue_size)
        self.max_queue_size=max_queue_size
        self.task=None

    def create_task(self):
        self.task = asyncio.create_task(self.task_fn())

    async def task_fn(self):
        pass

    def get_name(self):
        pass

    def get_task(self):
        return self.task

    def get_queue_size(self):
        return self.queue.qsize()

    def get_max_queue_size(self):
        return self.max_queue_size

    def get_queue(self):
        return self.queue



class SlSource(SLTask):
    def __init__(self,path,max_queue_size):
        super().__init__(max_queue_size)
        self.path=path #url or file path

    def get_path(self):
        return self.path

    def get_name(self):
        return f"src:{self.path}"

    async def __aenter__(self):
        return self

    async def __aiter__(self):
        return self

    async def __anext__(self):
        logging.info(f"next called")
        cur = self.queue.get()
        if cur == "":
            raise StopIteration  # End of file
        return cur

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

class SlDest(SLTask):
    def __init__(self,path,max_queue_size):
        super().__init__(max_queue_size)
        self.path=path #url or file path

    def get_path(self):
        return self.path

    def get_name(self):
        return f"dst:{self.path}"

    async def notify_write_end(self):
        await self.queue.put(None)

    async def __aenter__(self):
        return self

    async def __aiter__(self):
        return self

    async def __anext__(self):
        logging.info(f"next called")
        cur = self.queue.get()
        if cur == "":
            raise StopIteration  # End of file
        return cur

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        self.path=None
        await super().close()

class SourceFilter(SLClosable):
    def __init__(self,queue):
       super().__init__()
       self.queue=queue

    def close(self):
       self.queue=None



class DefaultSourceFilter(SourceFilter):
    def __init__(self,
                 queue,
                 filter_list=None,
                 min_size=20):
        super().__init__(queue)
        self.filter_list= ['Slow query'] if filter_list is None else filter_list
        self.min_size=min_size

    async def process(self, line):
        if not line:
            return
        if len(line) < self.min_size:
            return
        find = False
        if any(s in line for s in self.filter_list):
            find = True
        if not find:
            return
        await self.queue.put(line)
