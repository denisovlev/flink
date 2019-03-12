import org.apache.commons.math3.stat.descriptive.rank.Median;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class CheckpointTestParser {
	private Long zeroTime = null;

	public static void main(String[] args) throws IOException {
		new CheckpointTestParser(args[0], args[1]);
	}

	public CheckpointTestParser(String filename, String mode) throws IOException {
		System.out.println(filename + " " + mode);
		//sort logs by timestamp. if mode=load, sort backwards
		filename = sortFile(filename, mode.equals("load"));

		BufferedReader br = new BufferedReader(new FileReader(filename));
		HashMap<String, String> searchBuffer = new HashMap<String, String>();
		List<Long> durations = new ArrayList<Long>();
		String line;

		String marker = "ChecksumChecker " + mode;  //marker to be measured
		String matchMarker = "sum=";    //match to compare marker to
		while ((line = br.readLine()) != null) {
			//Get thread id of the line - enclosed in ()
			String thread = line.substring(line.indexOf("("), line.indexOf(")") + 1);
			if (line.contains(marker)) {
				if (searchBuffer.get(thread) != null) {
					System.out.println("WARNING: overwriting element in searchBuffer");
				}
				//If it is one of the markers we are looking for (load or save), add to search buffer, using the thread id as key
				searchBuffer.put(thread, line);
			} else if (line.contains(matchMarker) && searchBuffer.size() > 0 && searchBuffer.get(thread) != null) {
				//If the line is a "normal operation" line and it matches a thread number in the search buffer,
				//pop it from the searchBuffer and send to output.
				//Parse the timestamps and save to a list so we can make aggregations later
				String match = searchBuffer.remove(thread);
				Long fromTs = getTimestamp(match) - zeroTime;
				Long toTs = getTimestamp(line) - zeroTime;

				Long diff = toTs - fromTs;
				//because the logs are read backwards in mode=load, the difference will be negative. multiply by -1 to compensate
				diff = mode.equals("load") ? diff * -1 : diff;
				durations.add(diff);

				System.out.println(thread + "\t" + fromTs + "\t" + toTs + "\t" + diff);
			}
		}

		if (searchBuffer.size() > 0) {
			System.out.printf("WARNING: %d item/s remain in search buffer\n", searchBuffer.size());
		}

		//Aggregate the durations -- compute median and average
		double median = new Median().evaluate(durations.stream().mapToDouble(value -> (double) value).toArray());
		double average = durations.stream().mapToLong(value -> value).average().getAsDouble();
		System.out.println("average=" + average + " median=" + median);

		br.close();
	}

	private String sortFile(String filename, boolean reverse) throws IOException {
		List<String> lines = new ArrayList<String>();
		String line;

		//Make a list of files from power4/5/6 if * is present on the filename arg
		List<String> readFileNames = new ArrayList<>();
		if (filename.contains("*")) {
			for (int i = 4; i <= 6; i++) {
				readFileNames.add(filename.replace("*", String.valueOf(i)));
			}
		} else {
			readFileNames.add(filename);
		}

		//Combine the lines from the files
		for (String readFileName : readFileNames) {
			BufferedReader br = new BufferedReader(new FileReader(readFileName));
			while ((line = br.readLine()) != null) {
				//append the source of the file to the line (power4/5/6)
				lines.add(line + " " + readFileName.substring(readFileName.indexOf(" p") + 1, readFileName.indexOf(".log")));
			}
			br.close();
		}

		//Sort all lines according to the timestamp enclosed in []
		lines.sort(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				Long ts1 = getTimestamp(o1);
				Long ts2 = getTimestamp(o2);
				return ts1.compareTo(ts2);
			}
		});

		//Initialize zero time from the very first log line
		zeroTime = getTimestamp(lines.get(0));

		//Output sorted lines to a new file, ordered by time ascending
		File newFile = new File("sorted.log");
		if (newFile.exists()) {
			newFile.delete();
		}
		BufferedWriter bw = new BufferedWriter(new FileWriter(newFile));
		for (int i = 0; i < lines.size(); i++) {
			bw.write((getTimestamp(lines.get(i)) - zeroTime) + " " + lines.get(i) + "\n");
		}
		bw.close();

		//If reverse order is required (e.g. mode=load), write lines to another file, ordered by time descending
		if (reverse) {
			newFile = new File("reverse.log");
			if (newFile.exists()) {
				newFile.delete();
			}
			bw = new BufferedWriter(new FileWriter(newFile));
			for (int i = lines.size() - 1; i >= 0; i--) {
				bw.write((getTimestamp(lines.get(i)) - zeroTime) + " " + lines.get(i) + "\n");
			}
			bw.close();
		}

		//Return the filename
		return newFile.getName();
	}

	private Long getTimestamp(String s) {
		return Long.parseLong(s.substring(s.indexOf("[") + 1, s.indexOf("]")));
	}
}
