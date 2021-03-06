package it.tdlight.utils;

import io.vertx.core.file.OpenOptions;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.AsyncFile;
import io.vertx.reactivex.core.file.FileProps;
import io.vertx.reactivex.core.file.FileSystem;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Mono;

public class BinlogAsyncFile {

	private static final Logger logger = LoggerFactory.getLogger(BinlogAsyncFile.class);

	private final FileSystem filesystem;
	private final String path;
	private final OpenOptions openOptions;

	public BinlogAsyncFile(FileSystem fileSystem, String path) {
		this.filesystem = fileSystem;
		this.path = path;
		this.openOptions = new OpenOptions().setWrite(true).setRead(true).setCreate(false).setDsync(true);
	}
	
	private Mono<AsyncFile> openRW() {
		return filesystem.rxOpen(path, openOptions).as(MonoUtils::toMono);
	}

	public Mono<Buffer> readFully() {
		return openRW()
				.flatMap(asyncFile -> filesystem
						.rxProps(path)
						.map(props -> (int) props.size())
						.as(MonoUtils::toMono)
						.flatMap(size -> {
							var buf = Buffer.buffer(size);
							logger.debug("Reading binlog from disk. Size: " + BinlogUtils.humanReadableByteCountBin(size));
							return asyncFile.rxRead(buf, 0, 0, size).as(MonoUtils::toMono).thenReturn(buf);
						})
				);
	}

	public Mono<byte[]> readFullyBytes() {
		return this.readFully().map(Buffer::getBytes);
	}

	public Mono<Void> overwrite(Buffer newData) {
		return openRW().flatMap(asyncFile -> this
				.getSize()
				.doOnNext(size -> logger.debug("Preparing to overwrite binlog. Initial size: " + BinlogUtils.humanReadableByteCountBin(size)))
				.then(asyncFile.rxWrite(newData, 0)
						.andThen(asyncFile.rxFlush())
						.andThen(filesystem.rxTruncate(path, newData.length()))
						.as(MonoUtils::toMono)
				)
				.then(getSize())
				.doOnNext(size -> logger.debug("Overwritten binlog. Final size: " + BinlogUtils.humanReadableByteCountBin(size)))
				.then()
		);
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
				.map(fileProps -> fileProps.size() == 0 ? 0 : fileProps.lastModifiedTime())
				.as(MonoUtils::toMono);
	}

	public Mono<Long> getSize() {
		return filesystem
				.rxProps(path)
				.map(FileProps::size)
				.as(MonoUtils::toMono);
	}
}
