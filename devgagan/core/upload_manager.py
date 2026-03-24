import asyncio

class UploadManager:
    def __init__(self, max_concurrent_uploads=3, max_bytes_queue=1024 * 1024 * 1024):
        self.semaphore = asyncio.Semaphore(max_concurrent_uploads)
        self.current_queued_bytes = 0
        self.max_bytes = max_bytes_queue
        self.condition = asyncio.Condition()

    async def wait_for_slot(self, file_size: int):
        async with self.condition:
            # If the queue is completely empty, we allow at least one file 
            # regardless of size (to prevent deadlocks on files > 1GB)
            while self.current_queued_bytes > 0 and (self.current_queued_bytes + file_size) > self.max_bytes:
                await self.condition.wait()
            self.current_queued_bytes += file_size

    async def release_slot(self, file_size: int):
        async with self.condition:
            self.current_queued_bytes -= file_size
            self.condition.notify_all()

# Global instance for the entire bot process
upload_manager = UploadManager(max_concurrent_uploads=3, max_bytes_queue=1 * 1024 * 1024 * 1024)
