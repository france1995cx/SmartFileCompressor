import os
import zlib, bz2, lzma
import time
import math
from multiprocessing import Process, Queue, Manager
from collections import Counter

CHUNK_SIZE = 1024 * 1024  # 1MB
SUPPORTED_METHODS = ['zlib', 'bz2', 'lzma']


class FileSplitter:
    def __init__(self, file_path, chunk_size=CHUNK_SIZE):
        self.file_path = file_path
        self.chunk_size = chunk_size

    def split(self):
        with open(self.file_path, 'rb') as f:
            index = 0
            while True:
                data = f.read(self.chunk_size)
                if not data:
                    break
                yield index, data
                index += 1


class Compressor:
    @staticmethod
    def compress_data(data, method):
        if method == 'zlib':
            return zlib.compress(data)
        elif method == 'bz2':
            return bz2.compress(data)
        elif method == 'lzma':
            return lzma.compress(data)
        else:
            raise ValueError("Unsupported compression method")

    @staticmethod
    def benchmark_compression(data):
        sample = data[:5120] if len(data) > 5120 else data
        sizes = {
            'zlib': len(zlib.compress(sample)),
            'bz2': len(bz2.compress(sample)),
            'lzma': len(lzma.compress(sample))
        }
        return min(sizes, key=sizes.get)


class CompressionStats:
    def __init__(self):
        self.stats = {method: {'time': 0.0, 'count': 0} for method in SUPPORTED_METHODS}

    def update(self, method, elapsed_time):
        self.stats[method]['time'] += elapsed_time
        self.stats[method]['count'] += 1

    def get_best_method(self):
        avg_times = {
            method: (info['time'] / info['count'] if info['count'] else float('inf'))
            for method, info in self.stats.items()
        }
        return min(avg_times, key=avg_times.get)

    def export(self):
        return self.stats


class SmartWorker:
    def __init__(self, task_queue, result_dict, stat_dict):
        self.task_queue = task_queue
        self.result_dict = result_dict
        self.stat_dict = stat_dict
        self.stats = CompressionStats()

    def run(self):
        while True:
            try:
                idx, data = self.task_queue.get(timeout=3)
                method = Compressor.benchmark_compression(data)
                start_time = time.time()
                compressed = Compressor.compress_data(data, method)
                elapsed = time.time() - start_time
                self.stats.update(method, elapsed)
                self.result_dict[idx] = (method, compressed)
            except:
                break
        self.stat_dict.update(self.stats.export())


class ChunkMerger:
    def __init__(self, output_path):
        self.output_path = output_path

    def merge(self, result_dict, total_chunks):
        with open(self.output_path, 'wb') as f:
            for i in range(total_chunks):
                method, chunk = result_dict[i]
                f.write(chunk)


class SmartCompressorSystem:
    def __init__(self, input_file, output_file, num_workers=4):
        self.input_file = input_file
        self.output_file = output_file
        self.num_workers = num_workers
        self.manager = Manager()
        self.task_queue = Queue(maxsize=10)
        self.result_dict = self.manager.dict()
        self.stat_dict = self.manager.dict()

    def start(self):
        splitter = FileSplitter(self.input_file)
        processes = []
        for _ in range(self.num_workers):
            worker = SmartWorker(self.task_queue, self.result_dict, self.stat_dict)
            p = Process(target=worker.run)
            p.start()
            processes.append(p)

        total_chunks = 0
        for idx, chunk in splitter.split():
            self.task_queue.put((idx, chunk))
            total_chunks += 1

        for p in processes:
            p.join()

        merger = ChunkMerger(self.output_file)
        merger.merge(self.result_dict, total_chunks)

        print("Compression completed.")
        print("Statistics:")
        for method, stat in self.stat_dict.items():
            avg_time = stat['time'] / stat['count'] if stat['count'] else 0
            print(f"{method}: {stat['count']} chunks, avg time: {avg_time:.4f} s")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: python smart_file_compressor.py <input_file> <output_file> [num_threads]")
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        num_workers = int(sys.argv[3]) if len(sys.argv) == 4 else 4
        system = SmartCompressorSystem(input_file, output_file, num_workers)
        system.start()
