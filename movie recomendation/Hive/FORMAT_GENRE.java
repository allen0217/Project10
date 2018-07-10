import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


public class FORMAT_GENRE extends UDF {

    public Text evaluate(Text s){
        Text res = new Text("");
        if(s!=null){
            res.set(format(s.toString()));
        }
        return res;
    }

    private String format(String s){

        if(s.contains("|")){
            int lidx = s.lastIndexOf("|");

            s = s.replace("|",",");

            return s.substring(0,lidx)+",&"+s.substring(lidx+1)+" - <zxt130430>";
        }
        else
            return s;

    }
}
