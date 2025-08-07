import time
import threading
import concurrent.futures
from hf3fs_fuse.io import make_iovec, make_ioring, register_fd, deregister_fd, extract_mount_point
import multiprocessing.shared_memory
import os
import random

class UsrbioClient():
    def __init__(self, path: str):
        self.file = os.open(path, os.O_RDWR | os.O_CREAT)
        self.hf3fs_mount_point = extract_mount_point(path)
        register_fd(self.file)
        self.bs = 1 << 24
        self.entries = 1
        self.shm_r = multiprocessing.shared_memory.SharedMemory(size=self.bs * self.entries, create=True)
        self.shm_w = multiprocessing.shared_memory.SharedMemory(size=self.bs * self.entries, create=True)

        self.ior_r = make_ioring(self.hf3fs_mount_point, 1, for_read=True)
        self.ior_w = make_ioring(self.hf3fs_mount_point, 1, for_read=False)
        self.iov_r = make_iovec(self.shm_r, self.hf3fs_mount_point)
        self.iov_w = make_iovec(self.shm_w, self.hf3fs_mount_point)

    def read(self, offset: int, size: int) -> bytes:
        if offset < 0 or offset + size > self.size:
            raise ValueError(f"Read out of bounds: offset={offset}, size={size}, file_size={self.size}")

        remain = size
        bufs = []
        roff = offset
        while remain > 0:
            now = min(remain, self.bs * self.entries)
            self.ior_r.prepare(self.iov_r[:now], True, self.file, roff)
            done = self.ior_r.submit().wait(min_results=1)[0]
            if done.result < 0:
                raise OSError(-done.result)

            roff += done.result
            bufs.append(bytes(self.shm_r.buf[:done.result]))
            remain -= done.result

        if len(bufs) == 1:
            return bufs[0]
        else:
            return b''.join(bufs)

    def write(self, offset: int, data: bytes):
        if offset < 0 or offset + len(data) > self.size:
            raise ValueError(f"Write out of bounds: offset={offset}, size={len(data)}, file_size={self.size}")

        remain = len(data)
        roff = offset
        times = 0
        while remain > 0:
            now = min(remain, self.bs * self.entries)
            start = times * self.bs * self.entries
            self.shm_w.buf[:now] = data[start:start + now]
            self.ior_w.prepare(self.iov_w[:now], False, self.file, roff)
            done = self.ior_w.submit().wait(min_results=1)[0]
            if done.result < 0:
                raise OSError(-done.result)

            times += 1
            roff += done.result
            remain -= done.result

        return roff - offset

    def get_size(self) -> int:
        return self.size

    def set_size(self, size: int):
        # check size get
        os.ftruncate(self.file, size)
        self.size = size

    def close(self):
        deregister_fd(self.file)
        os.close(self.file)
        del self.ior_r
        del self.ior_w
        del self.iov_r
        del self.iov_w
        self.shm_r.close()
        self.shm_w.close()
        self.shm_r.unlink()
        self.shm_w.unlink()

    def flush(self):
        os.fsync(self.file)


def thread_task(thread_id, file_size, data_size, rw):
    """每个线程的任务"""
    # 创建独立的文件路径
    # file_path = f"/data/testfile/testfile_{thread_id}"
    file_path = f"/data/testfile/testfile"
    client = UsrbioClient(file_path)
    try:
        # 初始化文件大小
        client.set_size(file_size)
        test_data = b'a' * data_size  # 测试数据

        # 记录线程内的性能指标
        local_bytes_written = 0
        local_bytes_read = 0
        write_times = []
        read_times = []

        if rw == "write":
            # 写入阶段
            total_written = 0
            while total_written < file_size * 2:  # 循环写入两倍文件大小的数据
                offset = random.randint(0, file_size - data_size)
                start_time = time.time()
                written = client.write(offset, test_data)  # 随机写入
                end_time = time.time()
                write_times.append(end_time - start_time)
                local_bytes_written += written
                total_written += written
        else:
            # 读取阶段
            total_read = 0
            while total_read < file_size * 2:  # 循环读取两倍文件大小的数据
                offset = random.randint(0, file_size - data_size)
                start_time = time.time()
                data = client.read(offset, data_size)  # 循环读取
                end_time = time.time()
                read_times.append(end_time - start_time)
                local_bytes_read += len(data)
                total_read += len(data)

        # 返回线程的性能结果
        return {
            "bytes_written": local_bytes_written,
            "bytes_read": local_bytes_read,
            "write_times": sum(write_times),
            "read_times": sum(read_times),
            "write_iops": len(write_times),
            "read_iops": len(read_times),
        }
    finally:
        # 清理资源
        client.close()

def test_read_write_performance(num_threads=4, file_size=10 * 1024 * 1024, data_size=4 * 1024, rw="write"):

    start_time = time.time()
    # 避免GIL，这里使用多进程测试
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(thread_task, i, file_size, data_size, rw) for i in range(num_threads)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    end_time = time.time()

    # 汇总性能指标
    total_bytes_written=0
    total_bytes_read = 0
    write_iops = 0
    read_iops = 0
    total_write_times = []
    total_read_times = []
    if rw == "write":
        total_bytes_written = sum(result["bytes_written"] for result in results)
        write_iops = sum(result["write_iops"] for result in results)
        total_write_times = sum(result["write_times"] for result in results)
    else:
        total_bytes_read = sum(result["bytes_read"] for result in results)
        read_iops = sum(result["read_iops"] for result in results)
        total_read_times = sum(result["read_times"] for result in results)

    # 计算性能指标
    total_duration = end_time - start_time
    write_bandwidth = total_bytes_written / total_duration / (
                1024 ** 2) if total_duration > 0 else 0  # MB/s
    read_bandwidth = total_bytes_read / total_duration / (1024 ** 2) if total_duration > 0 else 0  # MB/s
    write_latency = total_write_times / write_iops if write_iops > 0 else 0
    read_latency = total_read_times / read_iops if read_iops > 0 else 0

    print("=== 性能测试结果 ===")
    print(f"总线程数: {num_threads}")
    print(f"io时间: {total_duration} s")
    print(f"总写入字节数: {total_bytes_written} 字节")
    print(f"总读取字节数: {total_bytes_read} 字节")
    print(f"写入带宽: {write_bandwidth:.2f} MB/s")
    print(f"读取带宽: {read_bandwidth:.2f} MB/s")
    print(f"写入 IOPS: {write_iops/total_duration:.2f} 次/秒")
    print(f"读取 IOPS: {read_iops/total_duration:.2f} 次/秒")
    print(f"写入延迟: {1000 * write_latency:.3f} ms")
    print(f"读取延迟: {1000 * read_latency:.3f} ms")

if __name__ == "__main__":
    # 执行性能测试
    test_read_write_performance(num_threads=8, file_size=10 * 1024 * 1024 * 1024, data_size=4 * 1024 * 1024, rw="read")