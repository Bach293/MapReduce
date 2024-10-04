import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PointWritable implements Writable {

    private float[] attributes = null;  // Mảng thuộc tính của điểm.
    private int dim;  // Số chiều (thuộc tính) của điểm.
    private int nPoints;  // Số lượng điểm được cộng dồn.

    public PointWritable() {
        this.dim = 0;
    }

    public PointWritable(final float[] c) {
        this.set(c);
    }

    public PointWritable(final String[] s) {
        this.set(s);
    }

    public static PointWritable copy(final PointWritable p) {
        PointWritable ret = new PointWritable(p.attributes);
        ret.nPoints = p.nPoints;
        return ret;
    }

    public void set(final float[] c) {
        this.attributes = c;
        this.dim = c.length;
        this.nPoints = 1;
    }

    public void set(final String[] s) {
        this.attributes = new float[s.length];
        this.dim = s.length;
        this.nPoints = 1;
        
        for (int i = 0; i < s.length; i++) {
            String value = s[i];
            
            // Kiểm tra và loại bỏ "Centroid: " khỏi chuỗi
            if (value.contains("Centroid:")) {
                value = value.replace("Centroid:", "").trim();
            }

            this.attributes[i] = Float.parseFloat(value);  // Chuyển đổi chuỗi thành số
        }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.dim = in.readInt();
        this.nPoints = in.readInt();
        this.attributes = new float[this.dim];

        for (int i = 0; i < this.dim; i++) {
            this.attributes[i] = in.readFloat();
        }
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.dim);
        out.writeInt(this.nPoints);

        for (int i = 0; i < this.dim; i++) {
            out.writeFloat(this.attributes[i]);
        }
    }

    @Override
    public String toString() {
        StringBuilder point = new StringBuilder();
        for (int i = 0; i < this.dim; i++) {
            point.append(Float.toString(this.attributes[i]));
            if (i != dim - 1) {
                point.append(",");
            }
        }
        return point.toString();
    }

    public void sum(PointWritable p) {
        for (int i = 0; i < this.dim; i++) {
            this.attributes[i] += p.attributes[i];
        }
        this.nPoints += p.nPoints;
    }

    public double calcDistance(PointWritable p) {
        double dist = 0.0f;
        for (int i = 0; i < this.dim; i++) {
            dist += Math.pow(Math.abs(this.attributes[i] - p.attributes[i]), 2);
        }
        dist = Math.sqrt(dist);
        return dist;
    }

    public void calcAverage() {
        for (int i = 0; i < this.dim; i++) {
            float temp = this.attributes[i] / this.nPoints;
            this.attributes[i] = (float) Math.round(temp * 100000) / 100000.0f;
        }
        this.nPoints = 1;
    }
}

/*
 * Writable: Lớp này thực hiện giao diện Writable, cho phép đọc và ghi dữ liệu trong môi trường Hadoop. 
 * Phương thức sum(): Cộng dồn các giá trị thuộc tính của điểm để tổng hợp các điểm lại.
 * Phương thức calcDistance(): Tính toán khoảng cách Euclidean giữa hai điểm.
 * Phương thức calcAverage(): Tính toán trung bình các điểm được cộng dồn để tìm tọa độ của tâm cụm mới.
 */