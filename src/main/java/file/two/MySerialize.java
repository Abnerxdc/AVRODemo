package file.two;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;

/**
 * Created by Administrator on 2017/11/23.
 */
public class MySerialize {
    public static void main(String[] args){
        Column column = new Column();
        Column column1 = new Column("1","Abner","24");
        Column column2 = new Column("2","leo","24");
        Column column3 = new Column("3","Abner1","25");
        DatumWriter<Column> userDatumWriter = new SpecificDatumWriter<Column>(Column.class);
        DataFileWriter<Column> dataFileWriter = new DataFileWriter<Column>(userDatumWriter);
        try{
            dataFileWriter.create(column.getSchema(), new File("./src/main/avro/column.avro"));
            dataFileWriter.append(column1);
            dataFileWriter.append(column2);
            dataFileWriter.append(column3);
            dataFileWriter.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
