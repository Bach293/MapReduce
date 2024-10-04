import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KCombiner extends Reducer<LongWritable, PointWritable, LongWritable, PointWritable> {

    public void reduce(LongWritable centroidId, Iterable<PointWritable> points, Context context)
            throws IOException, InterruptedException {

        PointWritable ptSum = PointWritable.copy(points.iterator().next());
        int count = 1; // Đếm số lượng point trong cluster
        while (points.iterator().hasNext()) {
            ptSum.sum(points.iterator().next());
            count++;
        }

        // Cập nhật lại số lượng point trong cluster
        ptSum.setNumPoints(count);

        context.write(centroidId, ptSum);
    }
}


/*
 * reduce(): Kết hợp các điểm dữ liệu từ Mapper cho từng cụm (ID của tâm cụm là khóa). 
 * Tổng hợp các điểm thành một tổng tạm thời.
 * Mục đích: Combiner giảm lượng dữ liệu truyền từ Mapper tới Reducer bằng 
 * cách tổng hợp các điểm dữ liệu tại Mapper.
 */
