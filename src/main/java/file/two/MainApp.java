package file.two;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/11/23.
 */
public class MainApp {
    public static void main(String[] args){
        List list = new ArrayList<Column>();
        Column column1 = new Column("1","Abner","24");
        list.add(column1);
        Column column2 = new Column("2","leo","24");
        list.add(column2);
        Column column3 = new Column("3","Abner1","25");
        list.add(column3);

        List list1 = new ArrayList<String>();
        list1.add(0,"abner");
        list1.add(2,"leo");
//        list1.add(1,"leo");
        list1.add(3,"leo");
        System.out.println(list1);

    }

    public String getSchemaDefine(String schemeId, List<Column> columnList, int i){
        return String.format("{\"namespace\": \"datatype.avro\", \"type\": \"record\", \"name\": \"User\", \"fields\":" +
                " [{\"name\": \"id\", \"type\": \"int\"},{\"name\": \"name\",  \"type\": \"string\"},{\"name\": \"age\", \"type\": \"int\"}]}", columnList.get(i));
    }
}
