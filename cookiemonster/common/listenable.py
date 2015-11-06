from typing import Generic, List
from typing import TypeVar, Callable


_ListenableReturnType = TypeVar('_ListenableReturnType')


class Listenable(Generic[_ListenableReturnType]):
    """
    Class on which listeners can be added.
    """
    def __init__(self):
        self._listeners = []

    def get_listeners(self) -> List[Callable[..., _ListenableReturnType]]:
        """
        Get all of the registered listeners.
        :return: list of the registered listeners
        """
        return self._listeners

    def add_listener(self, listener: Callable[..., _ListenableReturnType]):
        """
        Adds a listener.
        :param listener: the event listener
        """
        self._listeners.append(listener)

    def remove_listener(self, listener: Callable[..., _ListenableReturnType]):
        """
        Removes a listener
        :param listener: the event listener to remove
        """
        self._listeners.remove(listener)

    def notify_listeners(self, data: _ListenableReturnType):
        """
        Notify event listeners, passing them the given data
        :param data: the data to pass to the event listeners
        """
        for listener in self._listeners:
            listener(data)