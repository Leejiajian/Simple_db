package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
// 目录类
public class Catalog {

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    ConcurrentHashMap<Integer, Table> tables;  // <tableId, Table> tableId == DbFileId;
    int size;
    public Catalog() {
        tables = new ConcurrentHashMap<>();
        size = 0;
    }
    public class Table {
        private DbFile file;
        private String name;
        private String primary_key;
        //private int tableId;
        public Table(DbFile f, String n, String primary_key) {
            this.file = f;
            this.name = n;
            this.primary_key = primary_key;
            ++size;
        }
    }
    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    // 要确保添加的table与tables中的ID和name都不相同,删除操作需要判断
    public void addTable(DbFile file, String name, String pkeyField) {
        try {
            int id = getTableId(name);

            tables.remove(id);
        }catch(NoSuchElementException e){

        }
        tables.put(file.getId(), new Table(file, name, pkeyField));
        /*for(Table t : tables) {
            if((t.name != null && t.name.equals(name)) || file.getId() == t.file.getId()){
                t.name = name;
                t.primary_key = pkeyField;
                t.file = file;
                return;
            }
        }
        tables.add(new Table(file, name, pkeyField));
        ++size;

         */
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        if(name == null) throw new NoSuchElementException();
        for(Integer tableId : tables.keySet()){
            if(name.equals(tables.get(tableId).name))
                return tableId;
        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        for(Integer tableId : tables.keySet()) {
            if(tableId == tableid)
                return tables.get(tableId).file.getTupleDesc();;
        }
        throw new NoSuchElementException("No Such Table");
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        for(Integer tableId : tables.keySet()) {
            if(tableId == tableid)
                return tables.get(tableId).file;
        }
        throw new NoSuchElementException("No Such DatabaseFile");
    }

    public String getPrimaryKey(int tableid) {
        for(Integer tableId : tables.keySet()) {
            if(tableId == tableid)
                return tables.get(tableId).primary_key;
        }
        throw new NoSuchElementException("No Such DatabaseFile");

    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return tables.keySet().iterator();

    }

    public String getTableName(int id) {
        for(Integer tableId : tables.keySet()) {
            if(tableId == id)
                return tables.get(tableId).name;
        }
        throw new NoSuchElementException("No Such Table");
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        tables.clear();
        size = 0;
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

