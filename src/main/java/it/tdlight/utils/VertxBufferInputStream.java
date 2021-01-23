package it.tdlight.utils;

import io.vertx.core.buffer.Buffer;
import it.unimi.dsi.fastutil.io.MeasurableInputStream;
import it.unimi.dsi.fastutil.io.RepositionableStream;

public class VertxBufferInputStream extends MeasurableInputStream implements RepositionableStream {

	private final Buffer buffer;

	/** The first valid entry. */
	public int offset;

	/** The number of valid bytes in {@link #buffer} starting from {@link #offset}. */
	public int length;

	/** The current position as a distance from {@link #offset}. */
	private int position;

	/** The current mark as a position, or -1 if no mark exists. */
	private int mark;

	/** Creates a new buffer input stream using a given buffer fragment.
	 *
	 * @param buffer the backing buffer.
	 * @param offset the first valid entry of the buffer.
	 * @param length the number of valid bytes.
	 */
	public VertxBufferInputStream(final Buffer buffer, final int offset, final int length) {
		this.buffer = buffer;
		this.offset = offset;
		this.length = length;
	}

	/** Creates a new buffer input stream using a given buffer.
	 *
	 * @param buffer the backing buffer.
	 */
	public VertxBufferInputStream(final Buffer buffer) {
		this(buffer, 0, buffer.length());
	}

	@Override
	public boolean markSupported() {
		return true;
	}

	@Override
	public void reset() {
		position = mark;
	}

	/** Closing a fast byte buffer input stream has no effect. */
	@Override
	public void close() {}

	@Override
	public void mark(final int dummy) {
		mark = position;
	}

	@Override
	public int available() {
		return length - position;
	}

	@Override
	public long skip(long n) {
		if (n <= length - position) {
			position += (int)n;
			return n;
		}
		n = length - position;
		position = length;
		return n;
	}

	@Override
	public int read() {
		if (length == position) return -1;
		return buffer.getByte(offset + position++) & 0xFF;
	}

	/** Reads bytes from this byte-buffer input stream as
	 * specified in {@link java.io.InputStream#read(byte[], int, int)}.
	 * Note that the implementation given in {@link java.io.ByteArrayInputStream#read(byte[], int, int)}
	 * will return -1 on a zero-length read at EOF, contrarily to the specification. We won't.
	 */

	@Override
	public int read(final byte b[], final int offset, final int length) {
		if (this.length == this.position) return length == 0 ? 0 : -1;
		final int n = Math.min(length, this.length - this.position);
		buffer.getBytes(this.offset + this.position, this.offset + this.position + n, b, offset);
		this.position += n;
		return n;
	}

	@Override
	public long position() {
		return position;
	}

	@Override
	public void position(final long newPosition) {
		position = (int)Math.min(newPosition, length);
	}

	@Override
	public long length() {
		return length;
	}
}
