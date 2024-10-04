import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class KMapper extends Mapper<LongWritable, Text, LongWritable, PointWritable> {
    private PointWritable[] currCentroids;  // Mảng chứa các tâm cụm hiện tại.
    private final LongWritable centroidId = new LongWritable();  // ID của cụm gần nhất.
    private final PointWritable pointInput = new PointWritable();  // Điểm dữ liệu được xử lý.

    @Override
    public void setup(Context context) {
        int nClusters = Integer.parseInt(context.getConfiguration().get("k"));  // Lấy số lượng cụm từ cấu hình.

        this.currCentroids = new PointWritable[nClusters];  // Khởi tạo mảng tâm cụm với kích thước là số lượng cụm.
        for (int i = 0; i < nClusters; i++) {
            String[] centroid = context.getConfiguration().getStrings("C" + i);  // Lấy tọa độ của từng tâm cụm từ cấu hình.
            this.currCentroids[i] = new PointWritable(centroid);  // Khởi tạo các tâm cụm dưới dạng PointWritable.
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arrPropPoint = value.toString().split(",");  // Tách giá trị điểm dữ liệu từ chuỗi đầu vào.
        pointInput.set(arrPropPoint);  // Thiết lập đối tượng PointWritable từ dữ liệu đầu vào.
        
        double minDistance = Double.MAX_VALUE;  // Biến lưu khoảng cách nhỏ nhất tới các tâm cụm.
        int centroidIdNearest = 0;  // ID của tâm cụm gần nhất.

        // Tính toán khoảng cách từ điểm dữ liệu tới từng tâm cụm.
        for (int i = 0; i < currCentroids.length; i++) {
            double distance = pointInput.calcDistance(currCentroids[i]);  // Tính khoảng cách giữa điểm và tâm cụm.
            if (distance < minDistance) {
                centroidIdNearest = i;  // Cập nhật tâm cụm gần nhất.
                minDistance = distance;  // Cập nhật khoảng cách nhỏ nhất.
            }
        }

        centroidId.set(centroidIdNearest);  // Thiết lập ID của cụm gần nhất.
        context.write(centroidId, pointInput);  // Ghi cặp (centroidId, pointInput) vào kết quả Map.
        
    }
}

/*
 * setup(): Đọc tọa độ của các tâm cụm hiện tại từ cấu hình và lưu trữ chúng trong mảng currCentroids.
 * map(): Tính toán khoảng cách giữa mỗi điểm dữ liệu và các tâm cụm hiện tại, xác định tâm cụm gần nhất, 
 * và phát ra (ID của cụm gần nhất, điểm dữ liệu).
 * Mục đích: Mapper xử lý mỗi điểm dữ liệu và gán nó vào cụm gần nhất.
 */
