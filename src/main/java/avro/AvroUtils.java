package avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by Administrator on 2017/12/21.
 */
public class AvroUtils {
    public static String getSchemaDefine() {
        String schemaStr = "{\"namespace\":\"datatype.avro\",\"type\":\"record\",\"name\":\"RecordList\",\"fields\":[{\"name\":\"schema_16\",\"type\":[{\"type\":\"array\",\"items\":{\"namespace\":\"datatype.avro\",\"type\":\"record\",\"name\":\"DATA_DNSRecord\",\"fields\":[{\"name\":\"CDR_ID\",\"type\":[\"string\",\"null\"],\"default\":\"\"}, {\"name\":\"LOCATION_CODE\",\"type\":[ \"string\",\"null\"],\"default\":\"\"},{\"name\":\"SYSTEM_ID\",\"type\":[\"string\",\"null\"],\"default\":\"\"}]}}]}]}";
        return schemaStr;
    }
    public static String getChildSchemaDefine() {
        String schemaStr = "{\"namespace\":\"datatype.avro\",\"type\":\"record\",\"name\":\"DATA_DNSRecord\",\"fields\":[{\"name\":\"CDR_ID\",\"type\":[\"string\",\"null\"],\"default\":\"\"}, {\"name\":\"LOCATION_CODE\",\"type\":[ \"string\",\"null\"],\"default\":\"\"},{\"name\":\"SYSTEM_ID\",\"type\":[\"string\",\"null\"],\"default\":\"\"}]}";
        return schemaStr;
    }
    public static void main(String[] args) {
        String rawSchemaName = "schema_16";
        Map<String,String> map1 = new HashMap<String,String>();
        map1.put("CDR_ID","123");
        map1.put("LOCATION_CODE","234");
        map1.put("SYSTEM_ID","999");
        Map<String,String> map2 = new HashMap<String,String>();
        map2.put("CDR_ID","1234");
        map2.put("LOCATION_CODE","2345");
        map2.put("SYSTEM_ID","9999");
        List<Map<String,String>> list = new ArrayList<>();
        list.add(map1);
        list.add(map2);
        //序列化逻辑
        byte[] valueBytes = getSerizilizeDeep(rawSchemaName,new Schema.Parser().parse(getSchemaDefine()),new Schema.Parser().parse(getChildSchemaDefine()),list);
        System.out.println(valueBytes.toString());
        //反序列化逻辑
        GenericRecord records = getDeserialize(valueBytes,new Schema.Parser().parse(getSchemaDefine()));
        System.out.println(records);
        //反序列化后获取某个值 再强制类型转化为array 每一个里面都是一个record，里面是key -> value
        GenericData.Array<GenericData.Record> array = parseInnerRecordArray(records,rawSchemaName);
        System.out.println(array.get(0).toString());
        System.out.println(array.get(1).toString());
    }

    /**
     * 序列化逻辑（复杂版）
     * @param rawSchemaName
     * @param parentSchema 父schema
     * @param childSchema 子 schema
     * @param list 数据
     * @return 序列化后的数据
     */
    public static byte[] getSerizilizeDeep(String rawSchemaName, Schema parentSchema, Schema childSchema , List<Map<String,String>> list) {

        byte[] avroValusStr = null;
        GenericData.Record parentRecord = new GenericData.Record(parentSchema);
        List<GenericData.Record> records = new ArrayList<>();

        for(int i=0; i<list.size();i++){
            GenericData.Record record = new GenericData.Record(childSchema);
            Map map = list.get(i);
            Iterator it = map.entrySet().iterator();
            while (it.hasNext()){
                Map.Entry<String,String> mapEntry = (Map.Entry) it.next();
                String key = mapEntry.getKey();
                String value = mapEntry.getValue();
                record.put(key,value);
            }
            records.add(record);
        }
        parentRecord.put(rawSchemaName,records);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWrite = new GenericDatumWriter<GenericRecord>(new Schema.Parser().parse(getSchemaDefine()));
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWrite);
        try {
            dataFileWriter.create(parentSchema, outputStream);
            dataFileWriter.append(parentRecord);
            dataFileWriter.close();
            avroValusStr = outputStream.toByteArray();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return avroValusStr;
    }

    /**
     * 序列化逻辑（简单版）
     * @param schema 规则
     * @param map 数据AA
     * @return 序列化后的数据
     */
    public static byte[] getSerizilizeSimple(Schema schema , Map<String,String> map) {
        byte[] avroValusStr = null;
        GenericData.Record record = new GenericData.Record(schema);
        Iterator it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> mapEntry = (Map.Entry) it.next();
            String key = mapEntry.getKey();
            String value = mapEntry.getValue();
            record.put(key, value);
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWrite = new GenericDatumWriter<GenericRecord>(new Schema.Parser().parse(getSchemaDefine()));
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWrite);
        try {
            dataFileWriter.create(schema, outputStream);
            dataFileWriter.append(record);
            dataFileWriter.close();
            avroValusStr = outputStream.toByteArray();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return avroValusStr;
    }

    /**
     * 反序列化逻辑
     * @param serilizedValue 序列化之后的数据
     * @param schema 序列化规则
     * @return 反序列化后的数据
     */
    public static GenericRecord getDeserialize(byte[] serilizedValue,Schema schema){
        GenericRecord record = null;
        SeekableByteArrayInput seekableFileInput = new SeekableByteArrayInput(serilizedValue);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        try{
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(seekableFileInput, datumReader);
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return record;
    }

    /**
     * 获取 某个值后，再强制类型转化为array 每一个为Record <key,value>
     * @param genericRecord
     * @param rawSchemaName
     * @return array<Record<key,value>>
     */
    public static GenericData.Array<GenericData.Record> parseInnerRecordArray(GenericRecord genericRecord,String rawSchemaName){
        return ((GenericData.Array<GenericData.Record>) genericRecord.get(rawSchemaName));
    }

}
