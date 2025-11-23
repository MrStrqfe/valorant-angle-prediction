import subprocess
import boto3
import sys

BUCKET_NAME = "valorant-angle-videos"
S3_KEY = "raw/JRXFNhdmb8s.mp4"
VIDEO_URL = "https://www.youtube.com/watch?v=JRXFNhdmb8s"
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


class ReadablePipe:
    """
    Wrap a file-like pipe to make it compatible with boto3.upload_fileobj().
    """
    def __init__(self, pipe, chunk_size=CHUNK_SIZE):
        self.pipe = pipe
        self.chunk_size = chunk_size
        self.total_bytes = 0

    def read(self, size=-1):
        # boto3 may request a specific size; we respect that
        if size < 0:
            size = self.chunk_size
        data = self.pipe.read(size)
        if data:
            self.total_bytes += len(data)
            sys.stdout.write(f"\rUploaded ~{self.total_bytes / (1024*1024):.2f} MB")
            sys.stdout.flush()
        return data


def download_compress_upload(url, bucket, s3_key, crf=28, max_height=720):
    """
    Stream YouTube video → compress → upload to S3 using a file-like wrapper.
    """
    s3 = boto3.client("s3")

    ydl_cmd = [
        "yt-dlp",
        "-f", f"best[height<={max_height}]+bestaudio/best",
        "-o", "-",  # stdout
        url
    ]

    ffmpeg_cmd = [
        "ffmpeg",
        "-i", "pipe:0",         # input from yt-dlp
        "-vcodec", "libx264",
        "-crf", str(crf),
        "-preset", "fast",
        "-acodec", "aac",
        "-b:a", "128k",
        "-f", "mp4",            # output format
        "pipe:1"                # stdout
    ]

    # Start subprocesses
    with subprocess.Popen(ydl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as ydl_proc, \
         subprocess.Popen(ffmpeg_cmd, stdin=ydl_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as ffmpeg_proc:

        ydl_proc.stdout.close()  # allow SIGPIPE

        # Wrap ffmpeg stdout in file-like object for boto3
        readable = ReadablePipe(ffmpeg_proc.stdout)
        s3.upload_fileobj(readable, bucket, s3_key)
        print(f"\nUpload complete: s3://{bucket}/{s3_key}")

        ffmpeg_proc.stdout.close()
        ffmpeg_proc.wait()
        ydl_proc.wait()


if __name__ == "__main__":
    download_compress_upload(VIDEO_URL, BUCKET_NAME, S3_KEY)
