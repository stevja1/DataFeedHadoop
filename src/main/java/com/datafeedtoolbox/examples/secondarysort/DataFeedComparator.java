package com.datafeedtoolbox.examples.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class DataFeedComparator extends WritableComparator {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataFeedComparator.class);
	private static int comparisons = 0;

	public DataFeedComparator() {
		super(Text.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		++DataFeedComparator.comparisons;
		if(System.currentTimeMillis() % 5000 == 0) {
			DataFeedComparator.LOGGER.info("Made "+DataFeedComparator.comparisons+" comparisons in 5 seconds.");
			DataFeedComparator.comparisons = 0;
		}
		// Should have three parts -- visid, visit_num, and visit_page_num
		// Example: abc:123|2|13
		final String[] key1Parts = a.toString().split("\\|", -1);
		final String[] key2Parts = b.toString().split("\\|", -1);
		final String visId1 = key1Parts[0];
		final String visId2 = key2Parts[0];

		final int result = visId1.compareTo(visId2);
		if(result == 0) {
			final Double hitRank1 = Double.valueOf(
							String.format("%s.%s", key1Parts[1], key1Parts[2])
			);
			final Double hitRank2 = Double.valueOf(
							String.format("%s.%s", key2Parts[1], key2Parts[2])
			);
			if(hitRank1 < hitRank2) return -1;
			else if(hitRank1 > hitRank2) return 1;
			else return 0;
		} else return result;
	}
}
