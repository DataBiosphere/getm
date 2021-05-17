import time
from functools import wraps


class Profile:
    """
    Profile methods or code blocks with decorators or contexts, respectively.
    @profile("profiling my method")
    def my_method(*args, **kwargs):
       ...
    with profile("profiling my block"):
       ...
    """   
    def __init__(self, name="default"):
        self.name = name
    
    def __enter__(self):
        self.start = time.time()
    
    def __exit__(self, *args):
        print(f"{self.name} took {time.time() - self.start} seconds")
    
    def __call__(self, meth):
        @wraps(meth)
        def wrapper(*args, **kwargs):
            start = time.time()
            res = meth(*args, **kwargs)
            print(f"{self.name} took {time.time() - start} seconds")
            return res
        return wrapper
