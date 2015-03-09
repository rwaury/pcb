package de.tuberlin.dima.old.spargel;

import de.tuberlin.dima.old.graph.ConnectionInfo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputView;

import java.io.IOException;

@SuppressWarnings("serial")
public class EdgeInputFormat extends org.apache.flink.api.common.io.BinaryInputFormat<Tuple3<String, String, ConnectionInfo>> {

	@Override
	protected Tuple3<String, String, ConnectionInfo> deserialize(Tuple3<String, String, ConnectionInfo> reuse, DataInputView dataInput)
			throws IOException {
		reuse.f0 = dataInput.readUTF();
		reuse.f1 = dataInput.readUTF();
		reuse.f2.read(dataInput);
		return reuse;
	}

}
