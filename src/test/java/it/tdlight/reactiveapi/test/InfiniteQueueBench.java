package it.tdlight.reactiveapi.test;

import it.cavallium.filequeue.Deserializer;
import it.cavallium.filequeue.Serializer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.core.util.FileUtils;

public class InfiniteQueueBench {

	public static void main(String[] args) throws IOException {
		var SECOND = Duration.ofSeconds(1).toNanos();
		var tmpFile = Files.createTempFile("tmp", "");
		long startTime = System.nanoTime();
		LongAdder totalCount = new LongAdder();
		AtomicLong internalQueueSize = new AtomicLong();
		AtomicLong lastPrint = new AtomicLong();
		AtomicInteger status = new AtomicInteger();
		tmpFile.toFile().deleteOnExit();
		Files.delete(tmpFile);
		try (var queue = new it.cavallium.filequeue.DiskQueueToConsumer<String>(tmpFile, true, new Serializer<String>() {
			@Override
			public byte[] serialize(String data) throws IOException {
				return data.getBytes(StandardCharsets.US_ASCII);
			}
		}, new Deserializer<String>() {
			@Override
			public String deserialize(byte[] data) throws IOException {
				return new String(data, StandardCharsets.US_ASCII);
			}
		}, text -> {
			var s = internalQueueSize.decrementAndGet();
			var now = System.nanoTime();
			if (lastPrint.updateAndGet(prev -> prev + SECOND <= now ? now : prev) == now) {
				System.out.println(s + " currently queued elements, " + (totalCount.longValue() * SECOND / (now - startTime)) + " ops/s");
			}
			return status.incrementAndGet() % 10 == 0;
		})) {
			queue.startQueue();
			final var text = "a".repeat(32);
			while (true) {
				internalQueueSize.incrementAndGet();
				totalCount.increment();
				queue.add(text);
			}
		}
	}
}
