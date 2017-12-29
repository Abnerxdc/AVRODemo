package avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.*;
//import org.apache.avro.mapred.AvroAsTextInputFormat;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by Administrator on 2017/12/28.
 */
public class CHDAvro {
    //RandomSchema
//    public static byte[] serialize(RandomSchema aRecord ,BinaryEncoder encoder) {
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        encoder = EncoderFactory.get().binaryEncoder(out, encoder);
//        DatumWriter<RandomSchema> writer = new SpecificDatumWriter<RandomSchema>(aRecord.getSchema());
//        try {
//            writer.write(aRecord, encoder);
//            encoder.flush();
//            out.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return out.toByteArray();
//    }
//
//    public static RandomSchema deserialize(byte[] serializedBytes, Schema mySchema) {
//        DatumReader<RandomSchema> reader = new SpecificDatumReader<RandomSchema>(mySchema);
//        decoder = DecoderFactory.get().binaryDecoder(serializedBytes, null);
//        try {
//            return reader.read(null, decoder);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//    public static void AvroFormat(){
//        AvroAsTextInputFormat
//    }
}
