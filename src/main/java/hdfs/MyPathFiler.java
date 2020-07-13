package hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class MyPathFiler implements PathFilter {
    String reg = null;

    public MyPathFiler(String reg) {
        this.reg = reg;
    }

    @Override
    public boolean accept(Path path) {
        if (!(path.toString().matches(this.reg))) {
            return true;
        }
        return false;
    }
}
