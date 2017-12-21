package file.one;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * Created by Administrator on 2017/11/23.
 */
public class UserSerizlizer {
    public static void main(String[] args) throws IOException {
 // 声明并初始化User对象
 // 方式一
 User user1 = new User();
        user1.setName("zhangsan");
        user1.setFavoriteNumber(21);
        user1.setFavoriteColor(null);

 // 方式二 使用构造函数
        // Alternate constructor
        User user2 = new User("Ben", 7, "red");

 // 方式三，使用Build方式
        // Construct via builder
        User user3 = User.newBuilder()
                .setName("Charlie")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();
        // avro文件存放目录
        String path = "./src/main/avro/user.avro";
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(),  new File(path));
 // 把生成的user对象写入到avro文件
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();
    }
}
