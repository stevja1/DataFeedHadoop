package com.datafeedtoolbox.examples.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class DataFeedPartitioner extends Partitioner<Text, Text> {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataFeedPartitioner.class);
	private static int partitions = 0;
	@Override
	public int getPartition(Text key, Text value, int partitions) {
		++DataFeedPartitioner.partitions;
		if(System.currentTimeMillis() % 5000 == 0) {
			DataFeedPartitioner.LOGGER.info("Processed "+DataFeedPartitioner.partitions+" partitions in 5 seconds.");
			DataFeedPartitioner.partitions = 0;
		}

		final String visId = key.toString().split("\\|", -1)[0];
		return (visId.hashCode() & Integer.MAX_VALUE) % partitions;
	}
}
