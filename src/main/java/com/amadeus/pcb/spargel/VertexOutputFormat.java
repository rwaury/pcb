package com.amadeus.pcb.spargel;

import com.amadeus.pcb.graph.AirportInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

@SuppressWarnings("serial")
public class VertexOutputFormat extends org.apache.flink.api.common.io.BinaryOutputFormat<Tuple2<String, AirportInfo>> {

	@Override
	protected void serialize(Tuple2<String, AirportInfo> record,
			DataOutputView dataOutput) throws IOException {
		dataOutput.writeUTF(record.f0);
		record.f1.write(dataOutput);
	}

}
