package bytefile;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by Administrator on 2017/11/28.
 */
public class MainApp {
    public static void main(String[] args){
        String s = "Some text";
        byte[] bs= s.getBytes();
        try{
            OutputStream out = new FileOutputStream("./conf/bbb.txt");
            InputStream is = new ByteArrayInputStream(bs);
            byte[] buff = new byte[1024];
            int len = 0;
            while((len=is.read(buff))!=-1){
                out.write(buff, 0, len);
            }
            is.close();
            out.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
