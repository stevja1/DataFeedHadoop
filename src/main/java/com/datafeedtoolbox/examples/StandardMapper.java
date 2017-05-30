package com.datafeedtoolbox.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.*;

public class StandardMapper extends Mapper<Object, Text, Text, Text> {
	private static final Logger LOGGER = LoggerFactory.getLogger(StandardMapper.class);
	private static final String FIELD_SEPARATOR = "\t";
	public enum MapperCounters {
		CORRUPT_ROW, COLUMN_COUNT, INPUT_COLUMN_COUNT
	}

	private final List<String> columnHeaders = new ArrayList<>();
	private Configuration conf;
	private final Text key = new Text();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		URI[] columnHeadersFiles = Job.getInstance(conf).getCacheFiles();
		if(columnHeadersFiles != null && columnHeadersFiles.length > 0) {
			for (URI columnHeadersFile : columnHeadersFiles) {
				Path patternsPath = new Path(columnHeadersFile.getPath());
				String columnHeadersFileName = patternsPath.getName();
				StandardMapper.LOGGER.info("Reading config file: {}", columnHeadersFileName);
				try {
					readColumnHeaders(columnHeadersFileName);
					context.getCounter(MapperCounters.COLUMN_COUNT).setValue(this.columnHeaders.size());
				} catch(ParseException e) {
					StandardMapper.LOGGER.error("There was a problem parsing the column headers!");
				}
			}
		}
	}

	private void readColumnHeaders(final String fileName) throws ParseException, IOException {
		final BufferedReader fis = new BufferedReader(new FileReader(fileName));
		final String line = fis.readLine();

		if(line != null && line.length() > 0 && line.contains(StandardMapper.FIELD_SEPARATOR)) {
			final String[] columnHeaders = line.split(StandardMapper.FIELD_SEPARATOR, -1);
			for(String columnHeader : columnHeaders) {
				StandardMapper.LOGGER.info("Storing column {}", columnHeader);
				this.columnHeaders.add(columnHeader);
			}
		} else {
			throw new ParseException("There was a problem reading the column headers!", 0);
		}
	}

	private String getValue(String columnName, String[] columns) {
		int index = this.columnHeaders.lastIndexOf(columnName);
		if(index < 0) {
			StandardMapper.LOGGER.warn("There was an error locating column "+columnName+ " in column headers.");
			return "";
		} else {
			return columns[index];
		}
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		final String[] columns = value.toString().split(StandardMapper.FIELD_SEPARATOR, -1);
		if(columns.length != this.columnHeaders.size()) {
			context.getCounter(MapperCounters.INPUT_COLUMN_COUNT).setValue(columns.length);
			context.getCounter(MapperCounters.CORRUPT_ROW).increment(1);
			return;
		}
		final String visidHigh = this.getValue("post_visid_high", columns);
		final String visidLow = this.getValue("post_visid_low", columns);
		this.key.set(String.format("%s:%s", visidHigh, visidLow));
		context.write(this.key, value);
	}
}