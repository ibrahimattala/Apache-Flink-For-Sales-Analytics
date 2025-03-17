package salesAnalysis;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import salesAnalysis.dto.CategorySalesDTO;
import salesAnalysis.entities.OrderItem;
import salesAnalysis.entities.Product;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class DataBatchJob {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<OrderItem> orderItems = env
                .readCsvFile("~/mylocalsystem/Datasets/order_items.csv")
                .ignoreFirstLine()
                .pojoType(OrderItem.class, "orderItemId", "orderId", "productId", "quantity", "pricePerUnit");

        DataSource<Product> products = env
                .readCsvFile("~/mylocalsystem/Datasets/products.csv")
                .ignoreFirstLine()
                .pojoType(Product.class, "productId", "name", "description", "price", "category");

        //join the datasets on the product Id
        DataSet<Tuple6<String, String, Float, Integer, Float, String>> joined = orderItems
                .join(products)
                .where("productId")
                .equalTo("productId")
                .with((JoinFunction<OrderItem, Product, Tuple6<String, String, Float, Integer, Float, String>>) (first, second)
                        -> new Tuple6<>(
                        second.productId.toString(),
                        second.name,
                        first.pricePerUnit,
                        first.quantity,
                        first.pricePerUnit * first.quantity,
                        second.category
                ))
                .returns(TypeInformation.of(new TypeHint<Tuple6<String, String, Float, Integer, Float, String>>() {
                }));

        // Execute program, beginning computation.
        env.execute("Sales Analysis");
    }
}