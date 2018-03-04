package com.mapreduce.invertedindex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Mapper_InveretedIndex extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable opcode, Text lines, Context context) throws IOException, InterruptedException {

		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String docName = fileSplit.getPath().getName();

		String records = lines.toString().replaceAll("[^A-Za-z\\s]", "");
		String[] words = records.split(" ");
		for (int i = 0; i < words.length; i++) {
			if (words[i].length() > 0)
				context.write(new Text(words[i]), new Text(docName));
		}
	}
}
