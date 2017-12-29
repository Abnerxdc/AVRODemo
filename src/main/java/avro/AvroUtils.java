package avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by Administrator on 2017/12/21.
 */
public class AvroUtils {
    public static String getSchemaDefine() {
        String schemaStr = "{\"namespace\":\"datatype.avro\",\"type\":\"record\",\"name\":\"RecordList\",\"fields\":[{\"name\":\"schema_16\",\"type\":[{\"type\":\"array\",\"items\":{\"namespace\":\"datatype.avro\",\"type\":\"record\",\"name\":\"DATA_DNSRecord\",\"fields\":[{\"name\":\"CDR_ID\",\"type\":[\"string\",\"null\"],\"default\":\"\"}, {\"name\":\"LOCATION_CODE\",\"type\":[ \"string\",\"null\"],\"default\":\"\"},{\"name\":\"SYSTEM_ID\",\"type\":[\"string\",\"null\"],\"default\":\"\"},{\"name\":\"HOST_IP\",\"type\":[{\"type\":\"array\",\"items\":\"string\"},\"null\"],\"default\":\"\"}]}}]}]}";
        return schemaStr;
    }
    public static String getChildSchemaDefine() {
        String schemaStr = "{\"namespace\":\"datatype.avro\",\"type\":\"record\",\"name\":\"DATA_DNSRecord\",\"fields\":[{\"name\":\"CDR_ID\",\"type\":[\"string\",\"null\"],\"default\":\"\"}, {\"name\":\"LOCATION_CODE\",\"type\":[ \"string\",\"null\"],\"default\":\"\"},{\"name\":\"SYSTEM_ID\",\"type\":[\"string\",\"null\"],\"default\":\"\"},{\"name\":\"HOST_IP\",\"type\":[{\"type\":\"array\",\"items\":\"string\"},\"null\"],\"default\":\"\"}]}";
        return schemaStr;
    }
    public static void main(String[] args) {
        String rawSchemaName = "schema_16";
        Map<String,String> map1 = new HashMap<String,String>(3);
        map1.put("CDR_ID","123");
        map1.put("LOCATION_CODE","234");
        map1.put("SYSTEM_ID","999");
        Map<String,String> map2 = new HashMap<String,String>(3);
        map2.put("CDR_ID","1234");
        map2.put("LOCATION_CODE","2345");
//        map2.put("SYSTEM_ID","9999");
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

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        byte[] valueBytes1 = getSerizilizeMiddle(new Schema.Parser().parse(getChildSchemaDefine()),list);
        //反序列化逻辑
        List<GenericRecord> record1 = getDeserializeIterator(valueBytes1,new Schema.Parser().parse(getChildSchemaDefine()));
        System.out.println(record1);

        writeFile(new Schema.Parser().parse(getChildSchemaDefine()),list);
        System.out.println("=======================================================");
        byte[] a = serialize(list , new Schema.Parser().parse(getSchemaDefine()),new Schema.Parser().parse(getChildSchemaDefine()),"schema_16");
        System.out.println(a);
        System.out.println(getAvroDe_9(valueBytes, new Schema.Parser().parse(getSchemaDefine())));
        System.out.println(getAvroDe_9(a,new Schema.Parser().parse(getSchemaDefine())));
    }

    /**
     * 序列化逻辑（简单版）
     * @param schema 规则
     * @param map 数据
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
        DatumWriter<GenericRecord> datumWrite = new GenericDatumWriter<GenericRecord>(schema);
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
     * 序列化逻辑（中间版）
     * @param schema 规则
     * @param list 数据
     * @return 序列化后的数据
     */
    public static byte[] getSerizilizeMiddle(Schema schema , List<Map<String,String>> list) {
        byte[] avroValusStr  = null;

        DatumWriter<GenericRecord> datumWrite = new GenericDatumWriter<GenericRecord>(schema);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWrite);){
            dataFileWriter.create(schema, outputStream);
            for(int i=0;i<list.size();i++){
                Map<String,String> map = list.get(i);
                GenericData.Record record = new GenericData.Record(schema);
                Iterator it = map.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, String> mapEntry = (Map.Entry) it.next();
                    String key = mapEntry.getKey();
                    String value = mapEntry.getValue();
                    record.put(key, value);
                }
                dataFileWriter.append(record);
            }
            dataFileWriter.close();
            avroValusStr = outputStream.toByteArray();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return avroValusStr;
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
        DatumWriter<GenericRecord> datumWrite = new GenericDatumWriter<GenericRecord>(parentSchema);
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
                System.out.println(">>>>>>>>>>>>>>>"+record);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return record;
    }
    /**
     * 反序列化逻辑
     * @param serilizedValue 序列化之后的数据
     * @param schema 序列化规则
     * @return 反序列化后的数据
     */
    public static List<GenericRecord> getDeserializeIterator(byte[] serilizedValue,Schema schema){
        List<GenericRecord> list = new ArrayList<>();
        GenericRecord record = null;
        SeekableByteArrayInput seekableFileInput = new SeekableByteArrayInput(serilizedValue);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        try{
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(seekableFileInput, datumReader);
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                list.add(record);
                System.out.println(">>>>>>>>>>>>>>>"+record);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return list;
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
    public static void writeFile(Schema schema,List<Map<String,String>> list){
        try {
            DatumWriter<GenericRecord> dwriter = new SpecificDatumWriter<GenericRecord>(schema);
            //DatumWriter 将数据对象翻译成Encoder对象可以理解的类型，
            DataFileWriter<GenericRecord> write=new DataFileWriter<GenericRecord>(dwriter);
            write.create(schema,  new File("./conf/a.txt"));
            for(int i=0;i<list.size();i++){
                Map<String,String> map = list.get(i);
                GenericData.Record record = new GenericData.Record(schema);
                Iterator it = map.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, String> mapEntry = (Map.Entry) it.next();
                    String key = mapEntry.getKey();
                    String value = mapEntry.getValue();
                    record.put(key, value);
                }
                write.append(record);
            }
            write.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static GenericRecord getAvroDe_9(byte[] ser , Schema schema){
        GenericRecord record = null;
        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);
        try{
            Decoder decoder = DecoderFactory.get().binaryDecoder(ser,null);
            record = datumReader.read(null,decoder);
        }catch (Exception e){
            e.printStackTrace();
        }
        return record;
    }
    public static byte[] serialize(List<Map<String,String>> list, Schema parentSchema,Schema childSchema,String rawSchemaName) {

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

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<GenericRecord> writer = new SpecificDatumWriter<GenericRecord>(parentSchema);
        try {
            writer.write(parentRecord, encoder);
            encoder.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return out.toByteArray();
    }
}
