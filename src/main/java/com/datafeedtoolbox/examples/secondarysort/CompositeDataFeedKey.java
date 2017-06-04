package com.datafeedtoolbox.examples.secondarysort;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class CompositeDataFeedKey implements Writable {
	String visId;
	int visitNum;
	int visitPageNum;

	public CompositeDataFeedKey() {
		this.visId = "";
		this.visitNum = 0;
		this.visitPageNum = 0;
	}

	public CompositeDataFeedKey(String visId, int visitNum, int visitPageNum) {
		this.visId = visId;
		this.visitNum = visitNum;
		this.visitPageNum = visitPageNum;
	}

	public void set(String visId, int visitNum, int visitPageNum) {
		this.visId = visId;
		this.visitNum = visitNum;
		this.visitPageNum = visitPageNum;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeBytes(visId);
		dataOutput.writeInt(visitNum);
		dataOutput.writeInt(visitPageNum);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.visId = dataInput.readLine();
		this.visitNum = dataInput.readInt();
		this.visitPageNum = dataInput.readInt();
	}
}
