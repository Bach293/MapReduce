import java.io.BufferedReader;
import java.io.BufferedWriter;
//import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

	// Khởi tạo tâm cụm ngẫu nhiên
	public static PointWritable[] initRandomCentroids(int kClusters, int nLineOfInputFile, String inputFilePath,
			Configuration conf) throws IOException {
		System.out.println("Initializing random " + kClusters + " centroids...");
		PointWritable[] points = new PointWritable[kClusters]; // Mảng để lưu các tâm cụm

		List<Integer> lstLinePos = new ArrayList<Integer>(); // Danh sách lưu các vị trí dòng được chọn.
		Random random = new Random();
		int pos;
		while (lstLinePos.size() < kClusters) {
			pos = random.nextInt(nLineOfInputFile); // Chọn ngẫu nhiên một số từ 0 đến nLineOfInputFile-1
			if (!lstLinePos.contains(pos)) {
				lstLinePos.add(pos); // Nếu vị trí này chưa được chọn, thêm vào danh sách
			}
		}
		Collections.sort(lstLinePos); // Sắp xếp danh sách các dòng đã chọn theo thứ tự tăng dần

		FileSystem hdfs = FileSystem.get(conf);
		FSDataInputStream in = hdfs.open(new Path(inputFilePath));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		
		int row = 0;
		int i = 0;
		while (i < lstLinePos.size()) {
			pos = lstLinePos.get(i);
			String point = br.readLine();
			if (row == pos) {
				points[i] = new PointWritable(point.split(",")); // Tách 25,33 => ["25", "33"] bằng ','
				i++;
			}
			row++;
		}
		br.close();
		return points;

	}

	// Lưu tâm cụm
	public static void saveCentroidsForShared(Configuration conf, PointWritable[] points) {
		for (int i = 0; i < points.length; i++) {
			String centroidName = "C" + i;
			conf.unset(centroidName);
			conf.set(centroidName, points[i].toString());
		}
	}

	// Đọc tâm cụm từ đầu ra của Reducer
	public static PointWritable[] readCentroidsFromReducerOutput(Configuration conf, int kClusters, String folderOutputPath) throws IOException {
	    PointWritable[] points = new PointWritable[kClusters];
	    FileSystem hdfs = FileSystem.get(conf);
	    FileStatus[] status = hdfs.listStatus(new Path(folderOutputPath));

	    for (int i = 0; i < status.length; i++) {
	        if (!status[i].getPath().toString().endsWith("_SUCCESS")) {
	            Path outFilePath = status[i].getPath();
	            System.out.println("read " + outFilePath.toString());
	            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(outFilePath)));
	            String line;

	            while ((line = br.readLine()) != null) {
	                System.out.println(line);

	                // Bỏ qua các dòng không chứa dữ liệu centroid
	                if (line.startsWith("Centroid[") || line.startsWith("Cluster Size:") || line.startsWith("Point:")) {
	                    continue;
	                }

	                // Xử lý các dòng chứa thông tin centroid
	                String[] strCentroidInfo = line.split("\t");
	                if (strCentroidInfo.length == 2) {
	                    int centroidId = Integer.parseInt(strCentroidInfo[0].trim());
	                    String[] attrPoint = strCentroidInfo[1].split(",");
	                    points[centroidId] = new PointWritable(attrPoint);
	                }
	            }
	            br.close();
	        }
	    }

	    hdfs.delete(new Path(folderOutputPath), true);

	    return points;
	}

	// Check điều kiện dừng
	private static boolean checkStopKMean(PointWritable[] oldCentroids, PointWritable[] newCentroids, float threshold) {
	    boolean needStop = true;

	    System.out.println("Check for stop K-Means if distance <= " + threshold);
	    for (int i = 0; i < oldCentroids.length; i++) {

	        // Kiểm tra nếu centroid bị null
	        if (oldCentroids[i] == null || newCentroids[i] == null) {
	            System.out.println("Centroid[" + i + "] is null. Skipping comparison.");
	            needStop = false;
	            continue;
	        }

	        double dist = oldCentroids[i].calcDistance(newCentroids[i]);
	        System.out.println("distance centroid[" + i + "] changed: " + dist + " (threshold:" + threshold + ")");
	        if (dist > threshold) {
	            needStop = false;
	        }
	    }
	    return needStop;
	}


	// Ghi kết quả cuối cùng
	private static void writeFinalResult(Configuration conf, PointWritable[] centroidsFound, String outputFilePath,
            PointWritable[] centroidsInit, List<List<PointWritable>> clusterPoints) throws IOException {
		FileSystem hdfs = FileSystem.get(conf);
		FSDataOutputStream dos = hdfs.create(new Path(outputFilePath), true);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));

		for (int i = 0; i < centroidsFound.length; i++) {
			br.write("Centroid[" + i + "]:  (" + centroidsFound[i].toString() + ")  init: (" + centroidsInit[i].toString() + ")");
			br.newLine();
			br.write("Cluster Size: " + clusterPoints.get(i).size());
			br.newLine();
			br.write("Points:");
			br.newLine();
			for (PointWritable point : clusterPoints.get(i)) {
				br.write("Point: " + point.toString());
				br.newLine();
			}
		br.newLine();
		}

	br.close();
	hdfs.close();
	}


	// 1 bản tâm cũ 
	public static PointWritable[] copyCentroids(PointWritable[] points) {
		PointWritable[] savedPoints = new PointWritable[points.length];
		for (int i = 0; i < savedPoints.length; i++) {
			savedPoints[i] = PointWritable.copy(points[i]);
		}
		return savedPoints;
	}

	public static int MAX_LOOP = 50;

	// In tâm cụm
	public static void printCentroids(PointWritable[] points, String name) {
		System.out.println("=> CURRENT CENTROIDS:");
		for (int i = 0; i < points.length; i++)
			System.out.println("centroids(" + name + ")[" + i + "]=> :" + points[i]);
		System.out.println("----------------------------------");
	}

	@SuppressWarnings("deprecation") // Bỏ qua các cảnh báo về việc sử dụng những phương thức hoặc lớp đã lỗi thời 
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		String inputFilePath = conf.get("in", null);
		String outputFolderPath = conf.get("out", null);
		String outputFileName = conf.get("result", "result.txt");

		int nClusters = conf.getInt("k", 3);
		float thresholdStop = conf.getFloat("thresh", 0.001f);
		int numLineOfInputFile = conf.getInt("lines", 0); // Số dòng trong tệp đầu vào, đại diện cho số điểm dữ liệu
		MAX_LOOP = conf.getInt("maxloop", 50);
		int nReduceTask = conf.getInt("NumReduceTask", 1);
		
		// Kiểm tra tính hợp lệ của tham số
		if (inputFilePath == null || outputFolderPath == null || numLineOfInputFile == 0) {
			System.err.printf(
					"Usage: %s -Din <input file name> -Dlines <number of lines in input file> -Dout <Folder ouput> -Dresult <output file result> -Dk <number of clusters> -Dthresh <Threshold>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		System.out.println("---------------INPUT PARAMETERS---------------");
		System.out.println("inputFilePath:" + inputFilePath);
		System.out.println("outputFolderPath:" + outputFolderPath);
		System.out.println("outputFileName:" + outputFileName);
		System.out.println("maxloop:" + MAX_LOOP);
		System.out.println("numLineOfInputFile:" + numLineOfInputFile);
		System.out.println("nClusters:" + nClusters);
		System.out.println("threshold:" + thresholdStop);
		System.out.println("NumReduceTask:" + nReduceTask);

		System.out.println("--------------- STATR ---------------");
		PointWritable[] oldCentroidPoints = initRandomCentroids(nClusters, numLineOfInputFile, inputFilePath, conf);
		PointWritable[] centroidsInit = copyCentroids(oldCentroidPoints);
		printCentroids(oldCentroidPoints, "init");
		saveCentroidsForShared(conf, oldCentroidPoints);
		
		// Vòng lặp Kmeans
		int nLoop = 0;
		PointWritable[] newCentroidPoints = null;
		List<List<PointWritable>> clusterPoints = new ArrayList<>();
		for (int i = 0; i < nClusters; i++) {
		    clusterPoints.add(new ArrayList<>());
		}
		long t1 = (new Date()).getTime();
		while (true) {
			nLoop++;
			if (nLoop == MAX_LOOP) {
				break;
			}
			Job job = new Job(conf, "K-Mean");// Job thực hiện deepCopy conf
			job.setJarByClass(Main.class);
			job.setMapperClass(KMapper.class);
//			job.setCombinerClass(KCombiner.class);
			job.setReducerClass(KReducer.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(PointWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(inputFilePath));

			FileOutputFormat.setOutputPath(job, new Path(outputFolderPath));
			job.setOutputFormatClass(TextOutputFormat.class); 

			// Xác định số lượng task Reducer sẽ được sử dụng
			job.setNumReduceTasks(nReduceTask);

			// Chờ cho công việc MapReduce hoàn thành
			boolean ret = job.waitForCompletion(true);
			if (!ret) {
				return -1;

			}

			newCentroidPoints = readCentroidsFromReducerOutput(conf, nClusters, outputFolderPath);
			
		    for (int i = 0; i < nClusters; i++) {
		        clusterPoints.get(i).clear();  // Làm rỗng danh sách trước khi lưu mới
		        // Thêm logic lưu point vào danh sách clusterPoints.get(i)
		    }
			
			printCentroids(newCentroidPoints, "new");
			boolean needStop = checkStopKMean(newCentroidPoints, oldCentroidPoints, thresholdStop);

			oldCentroidPoints = copyCentroids(newCentroidPoints);

			if (needStop) {
				break;
			} else {
				saveCentroidsForShared(conf, newCentroidPoints);
			}

		}
		if (newCentroidPoints != null) {
			System.out.println("------------------- FINAL RESULT -------------------");
			writeFinalResult(conf, newCentroidPoints, outputFolderPath + "/" + outputFileName, centroidsInit, clusterPoints);
		}
		System.out.println("----------------------------------------------");
		System.out.println("K-MEANS CLUSTERING FINISHED!");
		System.out.println("Loop:" + nLoop);
		System.out.println("Time:" + ((new Date()).getTime() - t1) + "ms");

		return 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Main(), args);
		System.exit(exitCode);
	}
}