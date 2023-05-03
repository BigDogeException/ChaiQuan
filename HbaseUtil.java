package MyHbase1019;

import MyHbaseUtil1016.ConnUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;


public class HbaseUtil {
    private Table table;

    public HbaseUtil(String tablename) {
        try {
            table = ConnUtil.getConnection().getTable(TableName.valueOf(tablename));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public  void gets(String rowkey, String family, String qualifer, Integer versions, Long timestap){
        Get get = new Get(Bytes.toBytes(rowkey));
        if (family!= null&& qualifer!= null){
            //说明两个都有需要添加
            get.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifer));

        }else if (family != null&& qualifer== null){
            get.addFamily(Bytes.toBytes(family));
        }else if (family == null&& qualifer!= null){
            throw new RuntimeException("Error");
        }
        if (versions != null && versions>0){
            try {
                get.setMaxVersions(versions);
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (timestap!= null){
            try {
                get.setTimeStamp(timestap);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
           Result result =  table.get(get);
            List<Cell> list = result.listCells();
            showListCells(list,true);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public  void gets(String rowkey) {
            this.gets(rowkey,null,null,null,null);
    }
    public  void gets(String rowkey,String family, String qualifer) {
        this.gets(rowkey,family,qualifer,null,null);

    }
    public  void gets(String rowkey,String family) {
        this.gets(rowkey,family,null,null,null);

    }
    public  void gets(String rowkey,String family, String qualifer,Integer versions) {
        this.gets(rowkey,family,qualifer,versions,null);

    }
    public  void gets(String rowkey,Integer versions) {
        this.gets(rowkey,null,null,versions,null);
    }

        public static void showListCells(List<Cell> list , boolean oneLine) {
        if (oneLine) {
            StringBuilder sb = new StringBuilder();

            for (Cell cell : list) {
                sb.append(new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                sb.append(",");
            }
            sb.delete(sb.length()-1,sb.length());
            System.out.println(sb);
        } else {
            for (Cell cell : list) {
                System.out.println(new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
        }

    }



    public  String  showListCells(List<Cell> list) {

        StringBuilder sb = new StringBuilder();

        for (Cell cell : list) {
            sb.append(new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            sb.append(",");
        }
        sb.delete(sb.length() - 1, sb.length());
        System.out.println(sb);
        return sb.toString();
    }


    public  String getsForstr(String rowkey, String family, String qualifer, Integer versions, Long timestap){
        String str = null;
        Get get = new Get(Bytes.toBytes(rowkey));
        if (family!= null&& qualifer!= null){
            //说明两个都有需要添加
            get.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifer));

        }else if (family != null&& qualifer== null){
            get.addFamily(Bytes.toBytes(family));
        }else if (family == null&& qualifer!= null){
            throw new RuntimeException("Error");
        }
        if (versions != null && versions>0){
            try {
                get.setMaxVersions(versions);
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (timestap!= null){
            try {
                get.setTimeStamp(timestap);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            Result result =  table.get(get);
            List<Cell> list = result.listCells();
            str = showListCells(list);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return str;
    }

    public  String getsForstr(String rowkey) {
       return this.getsForstr(rowkey,null,null,null,null);
    }
    public   String getsForstr(String rowkey,String family, String qualifer) {
        return this.getsForstr(rowkey,family,qualifer,null,null);

    }
    public   String getsForstr(String rowkey,String family) {
        return  this.getsForstr(rowkey,family,null,null,null);

    }
    public   String getsForstr(String rowkey,String family, String qualifer,Integer versions) {
        return  this.getsForstr(rowkey,family,qualifer,versions,null);

    }
    public   String getsForstr(String rowkey,Integer versions) {
        return this.getsForstr(rowkey,null,null,versions,null);
    }



    //用于显示Scan结果的方法
    public static void showForResultScanner(ResultScanner rs){
        for (Result r : rs){
            System.out.println(new String(r.getRow())+",");
            List<Cell> list = r.listCells();
            for (Cell cell : list){
                System.out.print(
                        "("+
                        new String(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())
                + new String(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength())
                        + " - "
                        + cell.getTimestamp()
                        + " = "
                        + new String(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength())
                        + ")\t"
                );

            }
            System.out.println(new String(r.getRow()));
        }

    }



    public void forShowFilter(Filter filter){
        Scan scan = new Scan();
        scan.setFilter(filter);
        /*Get get = new Get(Bytes.toBytes("aa"));
        get.setFilter(filter);*/
        ResultScanner rs = null;
        try {
            rs = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        HbaseUtil.showForResultScanner(rs);

    }



    public static void main(String[] args) {
        /*HbaseUtil zy = new HbaseUtil("newTable");
        System.out.println(zy.getsForstr("rk10","info"));*/
    }
}
