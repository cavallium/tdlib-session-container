package it.tdlight.utils;

import io.vertx.core.buffer.Buffer;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.MeasurableOutputStream;
import it.unimi.dsi.fastutil.io.RepositionableStream;

public class VertxBufferOutputStream  extends MeasurableOutputStream implements RepositionableStream {

	/** The buffer backing the output stream. */
	public Buffer buffer;

	/** Creates a new buffer output stream with an initial capacity of 0 bytes. */
	public VertxBufferOutputStream() {
		this(0);
	}

	/** Creates a new buffer output stream with a given initial capacity.
	 *
	 * @param initialCapacity the initial length of the backing buffer.
	 */
	public VertxBufferOutputStream(final int initialCapacity) {
		buffer = Buffer.buffer(initialCapacity);
	}

	/** Creates a new buffer output stream wrapping a given byte buffer.
	 *
	 * @param a the byte buffer to wrap.
	 */
	public VertxBufferOutputStream(final Buffer a) {
		buffer = a;
	}

	@Override
	public void write(final int b) {
		buffer.appendByte((byte) b);
	}

	@Override
	public void write(final byte[] b, final int off, final int len) {
		ByteArrays.ensureOffsetLength(b, off, len);
		buffer.appendBytes(b, off, len);
	}

	@Override
	public void position(long newPosition) {
		throw new UnsupportedOperationException("Can't change position of a vertx buffer output stream");
	}

	@Override
	public long position() {
		return this.length();
	}

	@Override
	public long length() {
		return buffer.length();
	}
}
