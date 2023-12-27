import os
import sys
import time

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess
from utils import fileUploadUtils


# Define a custom event handler
class MyHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            # Execute your Python script when a file is created
            triggered_dir = os.path.dirname(event.src_path)
            subprocess.run(["python3", folder_script_dict[triggered_dir], event.src_path])

# List of folders to monitor
folder_script_dict = fileUploadUtils().fetchFolderScriptMappings()

# Create an observer for each folder and start watching
observers = []
for folder_path in folder_script_dict.keys():
    observer = Observer()
    observer.schedule(MyHandler(), path=folder_path)
    observer.start()
    observers.append(observer)

try:
    while True:
        time.sleep(1)
        pass
except KeyboardInterrupt:
    for observer in observers:
        observer.stop()

for observer in observers:
    observer.join()
