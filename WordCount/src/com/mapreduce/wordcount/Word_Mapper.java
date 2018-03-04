package com.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Word_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable opcode, Text lines, Context context) throws IOException, InterruptedException {
		String text = lines.toString().replaceAll("[^a-zA-Z\\s]", "");
		String[] words = text.split(" ");

		for (int i = 0; i < words.length; i++) {
			if (words[i].length() > 0)
				context.write(new Text(words[i]), new IntWritable(1));
		}
	}
}
