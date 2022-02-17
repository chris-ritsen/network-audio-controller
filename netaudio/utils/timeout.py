from multiprocessing import Process, Pipe


class Timeout:
    def __init__(self, func, timeout):
        self.func = func
        self.timeout = timeout

    def __call__(self, *args, **kargs):
        def pmain(pipe, func, args, kargs):
            result = None

            try:
                result = func(*args, **kargs)
            except Exception:
                pass

            pipe.send(result)

        parent_pipe, child_pipe = Pipe()

        p = Process(target=pmain, args=(child_pipe, self.func, args, kargs))
        p.start()
        p.join(self.timeout)

        result = None

        if p.is_alive():
            p.terminate()
            result = None
            raise TimeoutError

        result = parent_pipe.recv()

        return result
