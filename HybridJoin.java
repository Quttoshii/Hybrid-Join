import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.LinkedList;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import java.util.ArrayList;
// import com.mysql.cj.x.protobuf.MysqlxCrud.Collection;
import java.util.Collection;

//download
//  from https://jar-download.com/download-handling.php

public class HybridJoin {
    MultiValuedMap<Integer, Object[]> hashTable;
    // MultiValuedMap<Integer, Object[]> diskBuffer;
    MultiValuedMap<Integer, Object[]> diskBasedPartition;

    StreamGenerator database;

    ResultSet transactions;
    ResultSet masterData;

    LinkedList<Integer> queue;
    ArrayList<Object[]> finalOutput;

    public static void main(String[] args) {
        HybridJoin run = new HybridJoin();
        run.applyHybridJoin();
    }

    public HybridJoin() {
        this.hashTable = new ArrayListValuedHashMap<>();
        this.diskBasedPartition = new ArrayListValuedHashMap<>();

        this.database = new StreamGenerator();

        this.transactions = database.getData("SELECT * FROM orders");
        this.masterData = database.getData("SELECT * FROM products");

        this.queue = new LinkedList<>();
        this.finalOutput = new ArrayList<>();
    }

    public void applyHybridJoin() {
        fetchMasterDataTuple();

        Integer w = 999;
        Integer proc = 0;

        do {
            try {
                while (transactions.next()) {
                    Object[] transactionsColumns = new Object[7];

                    transactionsColumns[0] = transactions.getInt("OrderID"); // use the transactionalClumns to the
                                                                             // stream generator as a streambuffer
                    transactionsColumns[1] = transactions.getDate("OrderDate"); // use in run mthod, store the values
                                                                                // in stream buffer (a variable in
                                                                                // that class)
                    transactionsColumns[2] = transactions.getInt("ProductID"); // use that variable here.
                    transactionsColumns[3] = transactions.getInt("CustomerID");
                    transactionsColumns[4] = transactions.getString("CustomerName");
                    transactionsColumns[5] = transactions.getString("Gender");
                    transactionsColumns[6] = transactions.getInt("QuantityOrdered");

                    insertCustomerTuple((Integer) transactionsColumns[2], transactionsColumns);
                    if (w-- == 0) {
                        break;
                    }
                }
            } catch (Exception error) {
                System.out.println(error);
            }

            Integer oldestJoinAttribute = queue.getFirst();

            Collection<Object[]> masterDataTuple = diskBasedPartition.get(oldestJoinAttribute);

            Collection<Object[]> transactionTuple = hashTable.remove(oldestJoinAttribute);

            for (Object[] tTuple : transactionTuple) {
                for (Object[] mdTuple : masterDataTuple) {

                    Object[] joinedOutput = getJoinOutput(tTuple, mdTuple);

                    finalOutput.add(joinedOutput);

                    // for (Object value : joinedOutput) {
                    // System.out.print(value + " ");
                    // }
                    // System.out.println();

                    database.insertData(joinedOutput);

                    switch (proc++) {
                        case 0:
                            System.out.println("Processing.");
                            break;
                        case 1:
                            // System.out.print("\b\b\b\b\b\b\b\b\b\b\b");
                            System.out.print(String.format("\033[%dA", 1)); // Move up
                            System.out.print("\033[2K"); // Erase line content
                            System.out.println("Processing..");
                            break;
                        case 2:
                            // System.out.print("\b\b\b\b\b\b\b\b\b\b\b\b");

                            System.out.print(String.format("\033[%dA", 1)); // Move up
                            System.out.print("\033[2K"); // Erase line content
                            System.out.println("Processing...");
                            break;
                        case 3:
                            proc = 0;
                            // System.out.print("\b\b\b\b\b\b\b\b\b\b\b\b\b");

                            System.out.print(String.format("\033[%dA", 1)); // Move up
                            System.out.print("\033[2K"); // Erase line content

                    }

                }
                w++;
            }

            queue.removeFirst();
        } while (!queue.isEmpty());

        System.out.print(String.format("\033[%dA", 1)); // Move up
        System.out.print("\033[2K"); // Erase line content

        System.out.println("Process Completed!");

        // System.out.println(finalOutput.size());
    }

    public Object[] getJoinOutput(Object[] transactionTuple, Object[] masterDataTuple) {
        Object[] output = new Object[14];

        // Node temp = (Node) transactionTuple[2];
        // transactionTuple[2] = temp.data;
        Integer j = 0;
        for (int i = 0; i < 13; i++) {
            if (i < 7) {
                output[i] = transactionTuple[i];
            } else {
                output[i] = masterDataTuple[j++];
            }

        }
        output[13] = BigDecimal.valueOf((Integer) transactionTuple[6]).multiply((BigDecimal) masterDataTuple[1])
                .doubleValue();
        return output;
    }

    public void insertCustomerTuple(Integer joinAttribute, Object[] tuple) {
        queue.add(joinAttribute);
        // tuple[2] = queue.getOldestNode();
        hashTable.put(joinAttribute, tuple);
    }

    public void fetchMasterDataTuple() {
        Integer index;
        try {
            while (masterData.next()) {
                Object[] masterDataColumns = new Object[6];

                index = masterData.getInt("productID");
                masterDataColumns[0] = masterData.getString("productName");
                masterDataColumns[1] = masterData.getBigDecimal("productPrice");
                masterDataColumns[2] = masterData.getInt("supplierID");
                masterDataColumns[3] = masterData.getString("supplierName");
                masterDataColumns[4] = masterData.getInt("storeID");
                masterDataColumns[5] = masterData.getString("storeName");

                diskBasedPartition.put(index, masterDataColumns);
            }
        } catch (Exception error) {
            System.out.println(error);
        }
    }

}