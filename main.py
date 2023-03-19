from storage.file import File

import psutil
import os
import time

def get_cpu_usage():
    return psutil.cpu_percent(interval=1, percpu=True)
data_dir = "./data"
storage = File(datadir=data_dir)

data = list()
while True:
    cpus = psutil.cpu_percent(interval=1, percpu=True)
    for core in range(len(cpus)):
        data.append({
            "timestamp": time.time(),
            "metrics": "cpu",
            "value": cpus[core],
            "tags": {"core": f"core{core}"}
        })
    memory = psutil.virtual_memory()
    now = time.time()
    mem_total = memory.total
    mem_used = memory.used
    mem_free = memory.free
    mem_available = memory.available
    data.append({
            "timestamp": now,
            "metrics": "memory",
            "value": mem_total,
            "tags": {"type": "total"}
        })
    data.append({
            "timestamp": now,
            "metrics": "memory",
            "value": mem_used,
            "tags": {"type": "used"}
        })
    data.append({
            "timestamp": now,
            "metrics": "memory",
            "value": mem_free,
            "tags": {"type": "free"}
        })
    data.append({
            "timestamp": now,
            "metrics": "memory",
            "value": mem_available,
            "tags": {"type": "vailable"}
        })
    storage.write(data=data)