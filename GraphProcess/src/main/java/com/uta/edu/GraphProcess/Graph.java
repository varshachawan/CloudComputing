package com.uta.edu.GraphProcess ;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

class Vertex implements Writable {
	public short tag; 
	public long group;
	public long VID;
	public Vector<Long> adjacent;

	public Vertex() {
		this.tag = 0;
		this.group = 0;
		this.VID = 0;
		this.adjacent = new Vector<Long>();
	}
	public Short getTag() {
		return tag;
	}

	public Long getGroup() {
		return group;
	}

	public Vector<Long> getAdjacent() {
		return adjacent;
	}

	public Long getVID() {
		return VID;
	}

	public Vertex(short tag, long group, long VID, Vector<Long> adjacent) {
		this.tag = tag;
		this.group = group;
		this.VID = VID;
		this.adjacent = adjacent;
	}

	public Vertex(short tag, long group) {
		this.tag = tag;
		this.group = group;
		this.adjacent = new Vector<Long>();
	}

	public void write(DataOutput out) throws IOException {
		out.writeShort(tag);
		out.writeLong(group);
		out.writeLong(VID);
		LongWritable size = new LongWritable(adjacent.size());
		size.write(out);
		for (int i = 0; i < adjacent.size(); i++) {
			out.writeLong(adjacent.get(i));
		}

	}
	
	public void readFields(DataInput in) throws IOException {
		tag = in.readShort();
		group = in.readLong();
		VID = in.readLong();
		adjacent.clear();
		LongWritable size = new LongWritable();
		size.readFields(in);
		for (int i = 0; i < size.get(); i++) {
			LongWritable adj = new LongWritable();
			adj.readFields(in);
			adjacent.add(adj.get());
		}

	}

	@Override
	public String toString() {
		return this.tag + "\t" + this.group + "\t" + this.VID + "\t" + this.adjacent;
	}
}

public class Graph{

	public static class Mapper1 extends Mapper<Object, Text, LongWritable, Vertex> {
 
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			Long VID = s.nextLong();
			Vector<Long> adjacent = new Vector<Long>();
			while (s.hasNext()) {
				Long token = s.nextLong();
				adjacent.add(token);

			}
			short tag = 0;
			context.write(new LongWritable(VID), new Vertex(tag, VID, VID, adjacent));
			s.close();

		}
	}

	public static class Mapper2 extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {
 
		@Override
		public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {
			context.write(new LongWritable(value.getVID()), value);

			for (Long n : value.getAdjacent()) {
				short tag = 1;
				LongWritable tmp = new LongWritable(n);
				Long grp = value.getGroup();
				Vertex v2 = new Vertex(tag, grp);
				context.write(tmp, v2);
			}
		}
	}

	public static class Reducer2 extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
 
		@Override
		public void reduce(LongWritable key, Iterable<Vertex> values, Context context)
				throws IOException, InterruptedException {

			long m = Long.MAX_VALUE;
			Vector<Long> adj = new Vector<Long>();
			short tag = 0;
			long VID = key.get();

			for (Vertex v : values) {
				if (v.tag == 0) {
					adj = (Vector<Long>) v.adjacent.clone();
				}
				if (v.group < m) {
					m = v.group;
				} else {
					m = m;
				}
			}
			Vertex a = new Vertex(tag, m, VID, adj);
			context.write(new LongWritable(m), a);
		}
	}

	public static class Mapper3 extends Mapper<LongWritable, Vertex, LongWritable, LongWritable> {
 
		@Override
		public void map(LongWritable group, Vertex value, Context context) throws IOException, InterruptedException {
			context.write(group, new LongWritable(1));

		}
	}

	public static class Reducer3 extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
 
		@Override
		public void reduce(LongWritable group, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			long m = 0;
			for (LongWritable value : values) {
				m += value.get();
			}
			context.write(group, new LongWritable(m));
		}
	}

	public static void main(String[] args) throws Exception {
		Job job1 = Job.getInstance();
		job1.setJobName("Job1");
		job1.setJarByClass(Graph.class);

		job1.setMapperClass(Mapper1.class);
		job1.setNumReduceTasks(0);

		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(Vertex.class);

		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Vertex.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/f0"));

		job1.waitForCompletion(true);

		for (int i = 0; i < 5; i++) {
			Job job2 = Job.getInstance();
			job2.setJobName("Job2 " + i);
			job2.setJarByClass(Graph.class);

			job2.setMapperClass(Mapper2.class);
			job2.setReducerClass(Reducer2.class);

			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Vertex.class);

			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(Vertex.class);

			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);

			FileInputFormat.setInputPaths(job2, new Path(args[1] + "/f" + i));
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/f" + (i + 1)));

			job2.waitForCompletion(true);
		}

		Job job3 = Job.getInstance();
		job3.setJobName("Job3");
		job3.setJarByClass(Graph.class);

		job3.setMapperClass(Mapper3.class);
		job3.setReducerClass(Reducer3.class);

		job3.setMapOutputKeyClass(LongWritable.class);
		job3.setMapOutputValueClass(LongWritable.class);

		job3.setOutputKeyClass(LongWritable.class);
		job3.setOutputValueClass(LongWritable.class);

		job3.setInputFormatClass(SequenceFileInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job3, new Path(args[1] + "/f5"));
		FileOutputFormat.setOutputPath(job3, new Path(args[2]));

		job3.waitForCompletion(true);

	}
}

