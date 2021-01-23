package it.tdlight.utils;

import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.AsyncFile;
import io.vertx.reactivex.core.file.FileProps;
import io.vertx.reactivex.core.file.FileSystem;
import reactor.core.publisher.Mono;

public class BinlogAsyncFile {

	private final FileSystem filesystem;
	private final String path;
	private final AsyncFile file;

	public BinlogAsyncFile(FileSystem fileSystem, String path, AsyncFile file) {
		this.filesystem = fileSystem;
		this.path = path;
		this.file = file;
	}

	public Mono<Buffer> readFully() {
		return filesystem
				.rxProps(path)
				.map(props -> (int) props.size())
				.as(MonoUtils::toMono)
				.flatMap(size -> {
					var buf = Buffer.buffer(size);
					return file.rxRead(buf, 0, 0, size).as(MonoUtils::toMono).thenReturn(buf);
				});
	}

	public Mono<byte[]> readFullyBytes() {
		return this.readFully().map(Buffer::getBytes);
	}

	public AsyncFile getFile() {
		return file;
	}

	public Mono<Void> overwrite(Buffer newData) {
		return file.rxWrite(newData, 0)
				.andThen(file.rxFlush())
				.andThen(filesystem.rxTruncate(path, newData.length()))
				.as(MonoUtils::toMono);
	}

	public Mono<Void> overwrite(byte[] newData) {
		return this.overwrite(Buffer.buffer(newData));
	}

	public FileSystem getFilesystem() {
		return filesystem;
	}

	public String getPath() {
		return path;
	}

	public Mono<Long> getLastModifiedTime() {
		return filesystem
				.rxProps(path)
				.map(FileProps::lastModifiedTime)
				.as(MonoUtils::toMono);
	}
}
