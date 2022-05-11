from pipelime.piper.progress.listener.base import Listener
from pipelime.piper.progress.listener.factory import (
    ListenerCallbackFactory,
    ProgressReceiverFactory,
)

if __name__ == "__main__":
    receiver = ProgressReceiverFactory.get_receiver("TOKEN")
    callback = ListenerCallbackFactory.get_callback()
    watcher = Listener(receiver, callback)
    watcher.start()
