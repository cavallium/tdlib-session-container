package it.tdlight.utils;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Error;
import it.tdlight.tdlibsession.td.ResponseError;
import it.tdlight.tdlibsession.td.TdError;
import org.jetbrains.annotations.Nullable;

public class TdLightUtils {

	@SuppressWarnings("RedundantIfStatement")
	public static boolean errorEquals(Throwable ex, @Nullable Integer errorCode, @Nullable String errorText) {
		while (ex != null) {
			TdApi.Error error = null;
			if (ex instanceof TdError) {
				error = ((TdError) ex).getTdError();
			}
			if (ex instanceof ResponseError) {
				error = new Error(((ResponseError) ex).getErrorCode(), ((ResponseError) ex).getErrorMessage());
			}

			if (error != null) {
				if (errorCode != null) {
					if (error.code != errorCode) {
						return false;
					}
				}
				if (errorText != null) {
					if (error.message == null || !error.message.contains(errorText)) {
						return false;
					}
				}
				return true;
			}

			ex = ex.getCause();
		}
		return false;
	}
}
