import threading
import queue
import json

_queues: list[queue.Queue] = []
_lock = threading.Lock()


def subscribe() -> queue.Queue:
    q: queue.Queue = queue.Queue(maxsize=30)
    with _lock:
        _queues.append(q)
    return q


def unsubscribe(q: queue.Queue) -> None:
    with _lock:
        if q in _queues:
            _queues.remove(q)


def broadcast(data: dict) -> None:
    with _lock:
        dead = []
        for q in _queues:
            try:
                q.put_nowait(data)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _queues.remove(q)
