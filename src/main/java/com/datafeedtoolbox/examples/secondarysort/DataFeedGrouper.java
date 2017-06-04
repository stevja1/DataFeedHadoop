package com.datafeedtoolbox.examples.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class DataFeedGrouper extends WritableComparator {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataFeedGrouper.class);
	private static int comparisons = 0;
	public DataFeedGrouper() {
		super(Text.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		++DataFeedGrouper.comparisons;
		if(System.currentTimeMillis() % 5000 == 0) {
			DataFeedGrouper.LOGGER.info("Made "+DataFeedGrouper.comparisons+" comparisons in 5 seconds.");
			DataFeedGrouper.comparisons = 0;
		}
		final String visId1 = a.toString().split("\\|", -1)[0];
		final String visId2 = b.toString().split("\\|", -1)[0];
		return visId1.compareTo(visId2);
	}
}
