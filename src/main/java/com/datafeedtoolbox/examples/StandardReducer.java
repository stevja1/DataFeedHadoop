package com.datafeedtoolbox.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class StandardReducer extends Reducer<Text,Text,Text,DoubleWritable> {
	private final DoubleWritable result = new DoubleWritable();
	private Configuration conf;
	private final List<String> columnHeaders = new ArrayList<>();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		URI[] columnHeadersFiles = Job.getInstance(conf).getCacheFiles();
		if(columnHeadersFiles != null && columnHeadersFiles.length > 0) {
			for (URI columnHeadersFile : columnHeadersFiles) {
				Path patternsPath = new Path(columnHeadersFile.getPath());
				String patternsFileName = patternsPath.getName().toString();
				try {
					readColumnHeaders(patternsFileName);
				} catch(ParseException e) {
					System.err.println("There was a problem parsing the column headers!");
				}
			}
		}
	}

	/**
	 * Reads in the column headers from a configuration file.
	 * @param fileName The filename containing the column headers
	 * @throws IOException Thrown if there is a problem reading from the file
	 * @throws ParseException Thrown if there is a problem with the column header format.
	 */
	private void readColumnHeaders(final String fileName) throws IOException, ParseException {
		final BufferedReader fis = new BufferedReader(new FileReader(fileName));
		final String line = fis.readLine();
		if(line != null && line.length() > 0 && line.contains("\t")) {
			final String[] columnHeaders = line.split("\t", -1);
			for(String columnHeader : columnHeaders) {
				this.columnHeaders.add(columnHeader);
			}
		} else {
			throw new ParseException("There was a problem reading the column headers!", 0);
		}
	}

	private String getValue(String columnName, String[] columns) {
		return columns[this.columnHeaders.lastIndexOf(columnName)];
	}

	private double calculateRevenue(String productList) {
		final String PRODUCT_ITEM_DELIM = ",";
		final String PRODUCT_PART_DELIM = ";";
		double revenue = 0.0;
		final String[] productItems = productList.split(PRODUCT_ITEM_DELIM, -1);
		String[] productParts;
		for(String productItem : productItems) {
			productParts = productItem.split(PRODUCT_PART_DELIM, -1);
			// Fields:
			// 0: Product Category
			// 1: Product SKU
			// 2: Units
			// 3: Total Revenue
			revenue += Double.valueOf(productParts[3]);
		}
		return revenue;
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String[] columns;
		String eventList;
		String productList;
		Double revenue = 0.0;
		for(Text hit : values) {
			columns = hit.toString().split("\t", -1);
			eventList = this.getValue("post_event_list", columns);
			eventList = String.format(",%s,", eventList);
			// Was there a purchase?
			if(eventList.indexOf(",1,") >= 0) {
				productList = this.getValue("post_product_list", columns);
				revenue += this.calculateRevenue(productList);
			}
		}
		this.result.set(revenue);
		context.write(key, this.result);
	}
}