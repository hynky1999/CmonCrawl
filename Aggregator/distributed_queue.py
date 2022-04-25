from abc import ABC, abstractmethod
from collections import deque
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class Queue(ABC, Generic[T]):
    """
    Abstract class for a queue.
    """

    @abstractmethod
    def enqueue(self, item: T) -> None:
        """
        Adds an item to the queue.
        """
        pass

    @abstractmethod
    def dequeue(self) -> T:
        """
        Removes an item from the queue.
        """
        pass

    @abstractmethod
    def is_empty(self) -> bool:
        """
        Checks if the queue is empty.
        """
        pass

    @abstractmethod
    def size(self) -> int:
        """
        Returns the size of the queue.
        """
        pass


class DummyQueue(Queue[T]):
    """
    Abstract class for a queue.
    """

    def __init__(self):
        self.queue: deque[T] = deque()

    def enqueue(self, item: T):
        self.queue.appendleft(item)

    def dequeue(self):
        return self.queue.pop()

    def is_empty(self):
        return len(self.queue) == 0

    def size(self):
        return len(self.queue)

