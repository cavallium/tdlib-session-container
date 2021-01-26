package it.tdlight.tdlibsession.td.middle;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Error;
import it.tdlight.utils.BufferUtils;
import java.util.ArrayList;

public class TdResultListMessageCodec implements MessageCodec<TdResultList, TdResultList> {

	public TdResultListMessageCodec() {
		super();
	}

	@Override
	public void encodeToWire(Buffer buffer, TdResultList ts) {
		BufferUtils.encode(buffer, os -> {
			if (ts.succeeded()) {
				os.writeBoolean(true);
				var t = ts.value();
				os.writeInt(t.size());
				for (TdApi.Object t1 : t) {
					t1.serialize(os);
				}
			} else {
				os.writeBoolean(false);
				ts.error().serialize(os);
			}
		});
	}

	@Override
	public TdResultList decodeFromWire(int pos, Buffer buffer) {
		return BufferUtils.decode(pos, buffer, is -> {
			if (is.readBoolean()) {
				var size = is.readInt();
				ArrayList<TdApi.Object> list = new ArrayList<>(size);
				for (int i = 0; i < size; i++) {
					list.add((TdApi.Object) TdApi.Deserializer.deserialize(is));
				}
				return new TdResultList(list);
			} else {
				return new TdResultList((Error) TdApi.Deserializer.deserialize(is));
			}
		});
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
