package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Error;
import it.tdlight.utils.VertxBufferInputStream;
import it.tdlight.utils.VertxBufferOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.warp.commonutils.stream.SafeDataInputStream;

public class TdResultListMessageCodec implements MessageCodec<TdResultList, TdResultList> {

	public TdResultListMessageCodec() {
		super();
	}

	@Override
	public void encodeToWire(Buffer buffer, TdResultList ts) {
		try (var bos = new VertxBufferOutputStream(buffer)) {
			try (var dos = new DataOutputStream(bos)) {
				if (ts.succeeded()) {
					dos.writeBoolean(true);
					var t = ts.value();
					dos.writeInt(t.size());
					for (TdApi.Object t1 : t) {
						t1.serialize(dos);
					}
				} else {
					dos.writeBoolean(false);
					ts.error().serialize(dos);
				}
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public TdResultList decodeFromWire(int pos, Buffer buffer) {
		try (var fis = new VertxBufferInputStream(buffer, pos)) {
			try (var dis = new SafeDataInputStream(fis)) {
				if (dis.readBoolean()) {
					var size = dis.readInt();
					ArrayList<TdApi.Object> list = new ArrayList<>(size);
					for (int i = 0; i < size; i++) {
						list.add((TdApi.Object) TdApi.Deserializer.deserialize(dis));
					}
					return new TdResultList(list);
				} else {
					return new TdResultList((Error) TdApi.Deserializer.deserialize(dis));
				}
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
