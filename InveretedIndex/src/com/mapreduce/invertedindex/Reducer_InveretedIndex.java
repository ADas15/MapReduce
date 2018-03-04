package com.mapreduce.invertedindex;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer_InveretedIndex extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Set<Text> document_set = new HashSet<>();
		StringBuilder sb = new StringBuilder();

		for (Text document : values) {
			if (document_set.add(document)) {
				String append_doc = (document_set.size() > 1) ? " | " + document.toString() : document.toString();
				sb.append(append_doc);
			}
		}
		context.write(key, new Text(sb.toString()));
	}

}
