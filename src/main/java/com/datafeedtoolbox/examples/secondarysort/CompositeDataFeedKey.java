package com.datafeedtoolbox.examples.secondarysort;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class CompositeDataFeedKey implements Writable {
	private String visId;
	private int visitNum;
	private int visitPageNum;
	private Double hitOrder;

	public CompositeDataFeedKey() {
		this.visId = "";
		this.visitNum = 0;
		this.visitPageNum = 0;
		this.hitOrder = 0.0;
	}

	public CompositeDataFeedKey(String visId, int visitNum, int visitPageNum) {
		this.visId = visId;
		this.visitNum = visitNum;
		this.visitPageNum = visitPageNum;
		this.hitOrder = Double.valueOf(String.format("%d.%d", this.visitNum, this.visitPageNum));
	}

	public void set(String visId, int visitNum, int visitPageNum) {
		this.visId = visId;
		this.visitNum = visitNum;
		this.visitPageNum = visitPageNum;
		this.hitOrder = Double.valueOf(String.format("%d.%d", this.visitNum, this.visitPageNum));
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeBytes(this.visId);
		dataOutput.writeInt(this.visitNum);
		dataOutput.writeInt(this.visitPageNum);
		dataOutput.writeDouble(this.hitOrder);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.visId = dataInput.readLine();
		this.visitNum = dataInput.readInt();
		this.visitPageNum = dataInput.readInt();
		this.hitOrder = dataInput.readDouble();
	}

	public String getVisId() {
		return visId;
	}

	public void setVisId(String visId) {
		this.visId = visId;
	}

	public int getVisitNum() {
		return visitNum;
	}

	public void setVisitNum(int visitNum) {
		this.visitNum = visitNum;
	}

	public int getVisitPageNum() {
		return visitPageNum;
	}

	public void setVisitPageNum(int visitPageNum) {
		this.visitPageNum = visitPageNum;
	}

	public Double getHitOrder() {
		return hitOrder;
	}

	public void setHitOrder(Double hitOrder) {
		this.hitOrder = hitOrder;
	}
}
