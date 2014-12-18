package com.amadeus.pcb.spargel;

import com.amadeus.pcb.graph.AirportInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;

import java.io.IOException;

@SuppressWarnings("serial")
public class VertexInputFormat extends org.apache.flink.api.common.io.BinaryInputFormat<Tuple2<String, AirportInfo>> {

	@Override
	protected Tuple2<String, AirportInfo> deserialize(
			Tuple2<String, AirportInfo> reuse, DataInputView dataInput)
			throws IOException {
		reuse.f0 = dataInput.readUTF();
		reuse.f1.read(dataInput);
		return reuse;
	}

}
