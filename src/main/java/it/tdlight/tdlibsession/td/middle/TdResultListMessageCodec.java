package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Error;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.utils.VertxBufferInputStream;
import it.tdlight.utils.VertxBufferOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class TdResultListMessageCodec implements MessageCodec<TdResultList, TdResultList> {

	public TdResultListMessageCodec() {
		super();
	}

	@Override
	public void encodeToWire(Buffer buffer, TdResultList ts) {
		try (var bos = new VertxBufferOutputStream(buffer)) {
			try (var dos = new DataOutputStream(bos)) {
				var t = ts.getValues();
				dos.writeInt(t.size());
				for (TdResult<TdApi.Object> t1 : t) {
					if (t1.succeeded()) {
						dos.writeBoolean(true);
						t1.result().serialize(dos);
					} else {
						dos.writeBoolean(false);
						t1.cause().serialize(dos);
					}
				}
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public TdResultList decodeFromWire(int pos, Buffer buffer) {
		try (var fis = new VertxBufferInputStream(buffer, pos, buffer.length())) {
			try (var dis = new DataInputStream(fis)) {
				var size = dis.readInt();
				ArrayList<TdResult<TdApi.Object>> list = new ArrayList<>(size);
				for (int i = 0; i < size; i++) {
					if (dis.readBoolean()) {
						list.add(TdResult.succeeded((TdApi.Object) TdApi.Deserializer.deserialize(dis)));
					} else {
						list.add(TdResult.failed((Error) TdApi.Deserializer.deserialize(dis)));
					}
				}
				return new TdResultList(list);
			}
		} catch (IOException | UnsupportedOperationException ex) {
			ex.printStackTrace();
			return new TdResultList(Collections.emptyList());
		}
	}

	@Override
	public TdResultList transform(TdResultList ts) {
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
