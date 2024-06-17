import asyncio
import threading

class Executor:
    def __init__(self):
        new_loop = asyncio.new_event_loop()
        thread = threading.Thread(target=new_loop.run_forever)
        thread.start()
        self.loop = new_loop
        self.thread = thread

    def run(self,corofn,*args):
        coro = corofn(*args)
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        self.future = future

    def done(self):
        return self.future.done()

    def result(self):
        return self.future.result()
