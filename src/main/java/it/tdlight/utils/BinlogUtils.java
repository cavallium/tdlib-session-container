package it.tdlight.utils;

import io.vertx.core.file.OpenOptions;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

public class BinlogUtils {

	private static final Logger logger = LoggerFactory.getLogger(BinlogUtils.class);

	/**
	 *
	 * @return optional result
	 */
	public static Mono<BinlogAsyncFile> retrieveBinlog(FileSystem vertxFilesystem, Path binlogPath) {
		var path = binlogPath.toString();
		var openOptions = new OpenOptions().setWrite(true).setRead(true).setCreate(false).setDsync(true);
		return vertxFilesystem.rxExists(path).filter(exists -> exists)
				.flatMapSingle(x -> vertxFilesystem.rxOpen(path, openOptions))
				.map(file -> new BinlogAsyncFile(vertxFilesystem, path, file))
				.as(MonoUtils::toMono);
	}

	public static Mono<Void> saveBinlog(BinlogAsyncFile binlog, byte[] data) {
		return binlog.overwrite(data);
	}

	public static Mono<Void> chooseBinlog(FileSystem vertxFilesystem,
			Path binlogPath,
			byte[] remoteBinlog,
			long remoteBinlogDate) {
		var path = binlogPath.toString();
		return retrieveBinlog(vertxFilesystem, binlogPath)
				.flatMap(binlog -> Mono
						.just(binlog)
						.zipWith(binlog.getLastModifiedTime())
				)
				// Files older than the remote file will be overwritten
				.filter(tuple -> tuple.getT2() < remoteBinlogDate)
				.map(Tuple2::getT1)
				.switchIfEmpty(Mono
						.fromRunnable(() -> logger.info("Overwriting local binlog: " + binlogPath))
						.then(vertxFilesystem.rxWriteFile(path, Buffer.buffer(remoteBinlog)).as(MonoUtils::toMono))
						.then(retrieveBinlog(vertxFilesystem, binlogPath))
				)
				.single()
				.then();
	}
}
