package it.tdlight.tdlibsession;

import java.util.Objects;
import java.util.StringJoiner;

class SignalMessage<T> {

	private final SignalType signalType;
	private final T item;
	private final String errorMessage;

	private SignalMessage(SignalType signalType, T item, String errorMessage) {
		this.signalType = signalType;
		this.item = item;
		this.errorMessage = errorMessage;
	}

	public static <T> SignalMessage<T> onNext(T item) {
		return new SignalMessage<>(SignalType.ITEM, Objects.requireNonNull(item), null);
	}

	public static <T> SignalMessage<T> onError(Throwable throwable) {
		return new SignalMessage<T>(SignalType.ERROR, null, Objects.requireNonNull(throwable.getMessage()));
	}

	static <T> SignalMessage<T> onDecodedError(String throwable) {
		return new SignalMessage<T>(SignalType.ERROR, null, Objects.requireNonNull(throwable));
	}

	public static <T> SignalMessage<T> onComplete() {
		return new SignalMessage<T>(SignalType.COMPLETE, null, null);
	}

	public SignalType getSignalType() {
		return signalType;
	}

	public String getErrorMessage() {
		return Objects.requireNonNull(errorMessage);
	}

	public T getItem() {
		return Objects.requireNonNull(item);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", SignalMessage.class.getSimpleName() + "[", "]")
				.add("signalType=" + signalType)
				.add("item=" + item)
				.add("errorMessage='" + errorMessage + "'")
				.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SignalMessage<?> that = (SignalMessage<?>) o;

		if (signalType != that.signalType) {
			return false;
		}
		if (!Objects.equals(item, that.item)) {
			return false;
		}
		return Objects.equals(errorMessage, that.errorMessage);
	}

	@Override
	public int hashCode() {
		int result = signalType != null ? signalType.hashCode() : 0;
		result = 31 * result + (item != null ? item.hashCode() : 0);
		result = 31 * result + (errorMessage != null ? errorMessage.hashCode() : 0);
		return result;
	}
}
