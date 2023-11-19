import asyncio
import time
from typing import Any, Callable, Coroutine, TypeVar

T = TypeVar("T")


class Throttler:
    """
    Throttler class for restricting the number of function calls per second.
    It does so by ensuring that there are at least (1000 / milliseconds) seconds between calls.
    Args:
        milliseconds (int): The number of milliseconds to wait between function calls.
    """

    def __init__(self, milliseconds: int):
        self.milliseconds = milliseconds
        self.last_call = 0
        self.semaphore = asyncio.Semaphore(1)

    async def throttle(
        self,
        func: Callable[..., Coroutine[Any, Any, T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """
        Throttles the function call.
        Precondition:
            func (Callable[..., Coroutine[Any, Any, T]]): The function to throttle. It must be a coroutine function.
            *args (Any): The positional arguments to pass to the function.
            **kwargs (Any): The keyword arguments to pass to the function.
        Postcondition:
            Returns the result of the throttled function call.
        Returns:
            T: The return type of the function.
        """
        async with self.semaphore:
            elapsed = time.time() - self.last_call
            if elapsed < self.milliseconds / 1000:
                await asyncio.sleep((self.milliseconds / 1000) - elapsed)
            self.last_call = time.time()
        return await func(*args, **kwargs)
