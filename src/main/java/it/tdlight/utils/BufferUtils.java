package it.tdlight.utils;

import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import java.io.IOException;
import org.apache.commons.lang3.SerializationException;

public class BufferUtils {

	private static final int CHUNK_SIZE = 8192;

	public static void writeBuf(ByteBufOutputStream os, io.vertx.reactivex.core.buffer.Buffer dataToWrite)
			throws IOException {
		var len = dataToWrite.length();
		os.writeInt(len);
		byte[] part = new byte[CHUNK_SIZE];
		for (int i = 0; i < len; i += CHUNK_SIZE) {
			var end = Math.min(i + CHUNK_SIZE, len);
			dataToWrite.getBytes(i, end, part, 0);
			os.write(part, 0, end - i);
		}
	}

	public static void writeBuf(ByteBufOutputStream os, io.vertx.core.buffer.Buffer dataToWrite) throws IOException {
		var len = dataToWrite.length();
		os.writeInt(len);
		byte[] part = new byte[CHUNK_SIZE];
		for (int i = 0; i < len; i += CHUNK_SIZE) {
			var end = Math.min(i + CHUNK_SIZE, len);
			dataToWrite.getBytes(i, end, part, 0);
			os.write(part, 0, end - i);
		}
	}

	public static io.vertx.core.buffer.Buffer readBuf(ByteBufInputStream is) throws IOException {
		int len = is.readInt();
		Buffer buf = Buffer.buffer(len);
		byte[] part = new byte[1024];
		int readPart = 0;
		for (int i = 0; i < len; i += 1024) {
			var lenx = (Math.min(i + 1024, len)) - i;
			if (lenx > 0) {
				readPart = is.readNBytes(part, 0, lenx);
				buf.appendBytes(part, 0, readPart);
			}
		}
		return buf;
	}

	public static io.vertx.reactivex.core.buffer.Buffer rxReadBuf(ByteBufInputStream is) throws IOException {
		int len = is.readInt();
		io.vertx.reactivex.core.buffer.Buffer buf = io.vertx.reactivex.core.buffer.Buffer.buffer(len);
		byte[] part = new byte[1024];
		int readPart = 0;
		for (int i = 0; i < len; i += 1024) {
			var lenx = (Math.min(i + 1024, len)) - i;
			if (lenx > 0) {
				readPart = is.readNBytes(part, 0, lenx);
				buf.appendBytes(part, 0, readPart);
			}
		}
		return buf;
	}

	public interface Writer {

		void write(ByteBufOutputStream os) throws IOException;
	}

	public interface Reader<T> {

		T read(ByteBufInputStream is) throws IOException;
	}

	public static void encode(Buffer buffer, Writer writer) {
		try (var os = new ByteBufOutputStream(((BufferImpl) buffer).byteBuf())) {
			writer.write(os);
		} catch (IOException ex) {
			throw new SerializationException(ex);
		}
	}


	public static <T> T decode(int pos, Buffer buffer, Reader<T> reader) {
		try (var is = new ByteBufInputStream(buffer.slice(pos, buffer.length()).getByteBuf())) {
			return reader.read(is);
		} catch (IOException ex) {
			throw new SerializationException(ex);
		}
	}

}
