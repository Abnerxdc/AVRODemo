package file.two;

import org.apache.avro.Schema;

import java.io.Serializable;


/**
 * Created by Administrator on 2017/11/23.
 */
public class Column implements Serializable {
    public static final Schema SCHEMA$ = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"abner.com\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}");
    private String id;
    private String name;
    private String age;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public Column(String id, String name, String age){
        this.id = id;
        this.name = name;
        this.age = age;
    }
    public Column(){
    }

    public Schema getSchema() { return SCHEMA$; }
}
