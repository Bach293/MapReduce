import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KReducer extends Reducer<LongWritable, PointWritable, Text, Text> {

    private final Text newCentroidValue = new Text();

    @Override
    public void reduce(LongWritable centroidId, Iterable<PointWritable> points, Context context)
            throws IOException, InterruptedException {

        PointWritable ptFinalSum = PointWritable.copy(points.iterator().next());
        StringBuilder pointList = new StringBuilder();  // StringBuilder để lưu danh sách các điểm
        int pointCount = 1;
        pointList.append(ptFinalSum.toString());

        while (points.iterator().hasNext()) {
            PointWritable p = points.iterator().next();
            ptFinalSum.sum(p);
            pointList.append(" | ").append(p.toString());  // Thêm điểm vào danh sách
            pointCount++;
        }

        ptFinalSum.calcAverage();

        // In ra centroid, số lượng điểm, và danh sách các điểm thuộc cụm đó
        String result = "Centroid: " + ptFinalSum.toString() + " | Points: " + pointCount + " | List: " + pointList.toString();
        newCentroidValue.set(result);
        
        // Ghi kết quả ra tệp
        context.write(new Text("Cluster " + centroidId.toString()), newCentroidValue);
    }
}
