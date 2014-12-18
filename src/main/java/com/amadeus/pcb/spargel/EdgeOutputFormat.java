package com.amadeus.pcb.spargel;

import com.amadeus.pcb.graph.ConnectionInfo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

@SuppressWarnings("serial")
public class EdgeOutputFormat extends org.apache.flink.api.common.io.BinaryOutputFormat<Tuple3<String, String, ConnectionInfo>> {

	@Override
	protected void serialize(Tuple3<String, String, ConnectionInfo> record,
			DataOutputView dataOutput) throws IOException {
		dataOutput.writeUTF(record.f0);
		dataOutput.writeUTF(record.f1);
		record.f2.write(dataOutput);
	}

}
