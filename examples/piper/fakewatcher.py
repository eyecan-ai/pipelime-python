from pipelime.piper.progress.listener import (
    Listener,
    ZMQProgressReceiver,
    RichTableListenerCallback,
)

if __name__ == "__main__":
    receiver = ZMQProgressReceiver("TOKEN")
    callback = RichTableListenerCallback()
    watcher = Listener(receiver, callback)
    watcher.start()
