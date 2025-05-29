import os
import zlib, bz2, lzma
import time
import math
import struct
from multiprocessing import Process, Queue, Manager
from collections import Counter

SUPPORTED_METHODS = ['zlib', 'bz2', 'lzma']


def calculate_entropy(data):
    if not data:
        return 0
    freq = Counter(data)
    total = len(data)
    return -sum((count / total) * math.log2(count / total) for count in freq.values())


class FileSplitter:
    def __init__(self, file_path, num_parts):
        self.file_path = file_path
        self.num_parts = num_parts
        self.file_size = os.path.getsize(file_path)
        self.part_size = self.file_size // num_parts + (self.file_size % num_parts > 0)

    def split(self):
        with open(self.file_path, 'rb') as f:
            for _ in range(self.num_parts):
                data = f.read(self.part_size)
                if not data:
                    break
                yield data


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
    def decompress_data(data, method):
        if method == 'zlib':
            return zlib.decompress(data)
        elif method == 'bz2':
            return bz2.decompress(data)
        elif method == 'lzma':
            return lzma.decompress(data)
        else:
            raise ValueError("Unsupported decompression method")

    @staticmethod
    def benchmark_compression(data):
        entropy = calculate_entropy(data[:8192] if len(data) > 8192 else data)
        if entropy < 4.0:
            return 'zlib'
        elif entropy < 6.5:
            return 'bz2'
        else:
            return 'lzma'


class CompressionStats:
    def __init__(self):
        self.stats = {method: {'time': 0.0, 'count': 0} for method in SUPPORTED_METHODS}

    def update(self, method, elapsed_time):
        self.stats[method]['time'] += elapsed_time
        self.stats[method]['count'] += 1

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
                # Use 3 small slices at beginning of data
                samples = [data[i*256:(i+1)*256] for i in range(3) if len(data) > (i+1)*256]
                if len(samples) == 3:
                    method = Compressor.benchmark_compression(samples[2])
                else:
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
            index_table = []
            current_offset = 0
            for i in range(total_chunks):
                method, data = result_dict[i]
                method_bytes = method.ljust(4).encode('utf-8')
                size_bytes = len(data).to_bytes(4, 'big')
                f.write(size_bytes + method_bytes + data)
                index_table.append((current_offset, len(data), method))
                current_offset += 8 + len(data)
            index_start = f.tell()
            for offset, size, method in index_table:
                f.write(struct.pack('>Q', offset))
                f.write(struct.pack('>I', size))
                f.write(method.ljust(4).encode('utf-8'))
            f.write(b'IDX1')
            f.write(struct.pack('>I', len(index_table)))
            f.write(struct.pack('>Q', index_start))


class Decompressor:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def start(self):
        with open(self.input_file, 'rb') as fin:
            fin.seek(-16, os.SEEK_END)
            if fin.read(4) != b'IDX1':
                raise ValueError("Invalid compressed file: missing index")
            count = struct.unpack('>I', fin.read(4))[0]
            index_start = struct.unpack('>Q', fin.read(8))[0]

            fin.seek(index_start)
            index = []
            for _ in range(count):
                offset = struct.unpack('>Q', fin.read(8))[0]
                size = struct.unpack('>I', fin.read(4))[0]
                method = fin.read(4).decode('utf-8').strip()
                index.append((offset, size, method))

            with open(self.output_file, 'wb') as fout:
                for offset, size, method in index:
                    fin.seek(offset + 8)
                    data = fin.read(size)
                    decompressed = Compressor.decompress_data(data, method)
                    fout.write(decompressed)


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
        start_all = time.time()
        splitter = FileSplitter(self.input_file, self.num_workers)
        processes = []
        for _ in range(self.num_workers):
            worker = SmartWorker(self.task_queue, self.result_dict, self.stat_dict)
            p = Process(target=worker.run)
            p.start()
            processes.append(p)

        index = 0
        for data in splitter.split():
            self.task_queue.put((index, data))
            index += 1

        for p in processes:
            p.join()

        merger = ChunkMerger(self.output_file)
        merger.merge(self.result_dict, index)

        total_elapsed = time.time() - start_all
        print("Compression completed.")
        print("Statistics:")
        for method, stat in self.stat_dict.items():
            avg_time = stat['time'] / stat['count'] if stat['count'] else 0
            print(f"{method}: {stat['count']} chunks, avg time: {avg_time:.4f} s")
        print(f"Total compression time: {total_elapsed:.4f} s")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4 or len(sys.argv) > 5:
        print("Usage:")
        print("  Compress:   python smart_file_compressor.py compress <input_file> <output_file> [num_threads]")
        print("  Decompress: python smart_file_compressor.py decompress <input_file> <output_file>")
    else:
        mode = sys.argv[1]
        input_file = sys.argv[2]
        output_file = sys.argv[3]

        if mode == 'compress':
            num_workers = int(sys.argv[4]) if len(sys.argv) == 5 else 4
            system = SmartCompressorSystem(input_file, output_file, num_workers)
            system.start()
        elif mode == 'decompress':
            decompressor = Decompressor(input_file, output_file)
            decompressor.start()
        else:
            print("Unknown mode. Use 'compress' or 'decompress'.")
