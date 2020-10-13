package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Error;
import it.tdlight.jni.TdApi.Update;
import it.tdlight.tdlibsession.td.TdResult;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class TdOptListMessageCodec implements MessageCodec<TdOptionalList, TdOptionalList> {

	public TdOptListMessageCodec() {
		super();
	}

	@Override
	public void encodeToWire(Buffer buffer, TdOptionalList ts) {
		try (var bos = new FastByteArrayOutputStream()) {
			try (var dos = new DataOutputStream(bos)) {
				if (ts.isSet()) {
					var t = ts.getValues();
					dos.writeInt(t.size());
					for (TdResult<Update> t1 : t) {
						if (t1.succeeded()) {
							dos.writeBoolean(true);
							t1.result().serialize(dos);
						} else {
							dos.writeBoolean(false);
							t1.cause().serialize(dos);
						}
					}
				} else {
					dos.writeInt(-1);
				}
			}
			bos.trim();
			buffer.appendBytes(bos.array);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public TdOptionalList decodeFromWire(int pos, Buffer buffer) {
		try (var fis = new FastByteArrayInputStream(buffer.getBytes(pos, buffer.length()))) {
			try (var dis = new DataInputStream(fis)) {
				var size = dis.readInt();
				if (size < 0) {
					return new TdOptionalList(false, Collections.emptyList());
				} else {
					ArrayList<TdResult<TdApi.Update>> list = new ArrayList<>();
					for (int i = 0; i < size; i++) {
						if (dis.readBoolean()) {
							list.add(TdResult.succeeded((Update) TdApi.Deserializer.deserialize(dis)));
						} else {
							list.add(TdResult.failed((Error) TdApi.Deserializer.deserialize(dis)));
						}
					}
					return new TdOptionalList(true, list);
				}
			}
		} catch (IOException | UnsupportedOperationException ex) {
			ex.printStackTrace();
			return new TdOptionalList(false, Collections.emptyList());
		}
	}

	@Override
	public TdOptionalList transform(TdOptionalList ts) {
		return ts;
	}

	@Override
	public String name() {
		return "TdOptListCodec";
	}

	@Override
	public byte systemCodecID() {
		// Always -1
		return -1;
	}
}
