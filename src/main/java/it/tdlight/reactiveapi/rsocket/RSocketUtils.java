package it.tdlight.reactiveapi.rsocket;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import it.tdlight.reactiveapi.Deserializer;
import it.tdlight.reactiveapi.Serializer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import reactor.core.publisher.Flux;

public class RSocketUtils {

	public static <T> Flux<T> deserialize(Flux<Payload> payloadFlux, Deserializer<T> deserializer) {
		return payloadFlux.map(payload -> {
			try {
				try (var bis = new ByteBufInputStream(payload.sliceData())) {
					return deserializer.deserialize(payload.data().readableBytes(), bis);
				}
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		});
	}

	public static <T> Flux<Payload> serialize(Flux<T> flux, Serializer<T> serializer) {
		return flux.map(element -> {
			var buf = ByteBufAllocator.DEFAULT.ioBuffer();
			try (var baos = new ByteBufOutputStream(buf)) {
				serializer.serialize(element, baos);
				return DefaultPayload.create(baos.buffer().retain());
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} finally {
				buf.release();
			}
		});
	}
}
