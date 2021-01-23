package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.utils.VertxBufferInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.warp.commonutils.stream.SafeDataInputStream;

public class TdExecuteObjectMessageCodec implements MessageCodec<ExecuteObject, ExecuteObject> {

	public TdExecuteObjectMessageCodec() {
		super();
	}

	@Override
	public void encodeToWire(Buffer buffer, ExecuteObject t) {
		try (var bos = new FastByteArrayOutputStream()) {
			try (var dos = new DataOutputStream(bos)) {
				dos.writeBoolean(t.isExecuteDirectly());
				t.getRequest().serialize(dos);
			}
			bos.trim();
			buffer.appendBytes(bos.array);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public ExecuteObject decodeFromWire(int pos, Buffer buffer) {
		try (var fis = new VertxBufferInputStream(buffer, pos)) {
			try (var dis = new SafeDataInputStream(fis)) {
				return new ExecuteObject(dis.readBoolean(), (Function) TdApi.Deserializer.deserialize(dis));
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return null;
	}

	@Override
	public ExecuteObject transform(ExecuteObject t) {
		// If a message is sent *locally* across the event bus.
		// This sends message just as is
		return t;
	}

	@Override
	public String name() {
		return "ExecuteObjectCodec";
	}

	@Override
	public byte systemCodecID() {
		// Always -1
		return -1;
	}
}
