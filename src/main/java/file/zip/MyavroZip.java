package file.zip;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipOutputStream;

/**
 * Created by Administrator on 2017/11/24.
 */
public class MyavroZip {
    public static String getSchemaDefine() {
        String schemaStr = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"abner.com\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"string\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";
        return schemaStr;
    }

    public static void main(String[] args) {
        Map<String, String> map1 = new HashMap<String, String>(5);
        map1.put("name", "Abner");
        map1.put("favorite_number", "23");
        map1.put("favorite_color", "red");
//        map1.put("age", "23");
        Schema schema = new Schema.Parser().parse(getSchemaDefine());
        System.out.println(getSerizilizeZipByMap(map1,schema));

        Map<String, String> map2 = new HashMap<String, String>(5);
        map2.put("name", "Abner");
        map2.put("favorite_number", "23");
//        map2.put("favorite_color", "red");
//        map2.put("age", "23");
        System.out.println(getSerizilizeZipByMap(map2,schema));

    }

    public static byte[] getSerizilizeZipByMap(Map<String, String> map, Schema schema) {
        byte[] avroValusStr = null;
        GenericData.Record record = new GenericData.Record(schema);
        for (String key : map.keySet()) {
            record.put(key, map.get(key));
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWrite = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWrite);
        try {
            dataFileWriter.create(schema, outputStream);
            dataFileWriter.append(record);
            dataFileWriter.close();
            avroValusStr = outputStream.toByteArray();
            ZipOutputStream out = new ZipOutputStream(outputStream);

            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return avroValusStr;
    }
}
