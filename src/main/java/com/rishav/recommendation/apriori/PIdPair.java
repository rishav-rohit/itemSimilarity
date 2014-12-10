package com.rishav.recommendation.apriori;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PIdPair implements WritableComparable<PIdPair> {
	public Text first, second;

	public PIdPair() {
		this.first = new Text();
		this.second = new Text();
	}

	public PIdPair(Text x, Text y) {
		this.first = x;
		this.second = y;
	}

	public PIdPair(String x, String y) {
		this.first = new Text(x);
		this.second = new Text(y);
	}

	public static List<PIdPair> getAllPairs(List<Text> pIds) {
		List<PIdPair> pairs = new ArrayList<PIdPair>();
		int total = pIds.size();
		for (int i = 0; i < total; i++) {
			Text num1 = pIds.get(i);
			for (int j = 0; j < total; j++) {
				if (i != j) {
					Text num2 = pIds.get(j);
//					System.out.println("PidPairs constructed: " + num1 + "#"+num2);
					pairs.add(new PIdPair(num1, num2));
				}
			}
		}
		return pairs;
	}
	
	public static PIdPair toPidPair(String pidPairStr) {
		return new PIdPair(pidPairStr.split("#")[0], pidPairStr.split("#")[1]);
	}

	@Override
	public String toString() {
		return first + "#" + second;
	}

	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	public int compareTo(PIdPair pIdPair) {
		int cmp = first.compareTo(pIdPair.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(pIdPair.second);
	}

	public Text getFirst() {
		return first;
	}

	public void setFirst(Text first) {
		this.first = first;
	}

	public Text getSecond() {
		return second;
	}

	public void setSecond(Text second) {
		this.second = second;
	}
}
