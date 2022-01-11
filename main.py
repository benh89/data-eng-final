
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery


if __name__ == '__main__':

    #table_schema for BigQuery
    cnt_schema = {
        'fields': [
            {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'Required'},
            {'name': 'sku', 'type': 'INTEGER', 'mode': 'Required'},
            {'name': 'total_no_of_product_views', 'type': 'INTEGER', 'mode': 'Required'}
        ]
    }

    sum_schema = {
        'fields': [
            {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'Required'},
            {'name': 'sku', 'type': 'INTEGER', 'mode': 'Required'},
            {'name': 'total_sales_amount', 'type': 'FLOAT', 'mode': 'Required'}
        ]
    }

    table_spec1 = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="final_ben_huang",
        tableId="cust_tier_code-sku-total_no_of_product_views")

    table_spec2 = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="final_ben_huang",
        tableId="cust_tier_code-sku-total_sales_amount")

    read_options = PipelineOptions(
        runner="DataflowRunner",
        project="york-cdf-start",
        region="us-central1",
        temp_location="gs://york_temp_files/tmp",
        job_name="ben-huang-final-job")

    with beam.Pipeline(options=read_options) as p:
        product_views = p | 'Read-cnt' >> beam.io.ReadFromBigQuery(query="""SELECT cast(c.CUST_TIER_CODE as string) as cust_tier_code, cast(p.SKU as integer) as sku, count(p.EVENT_TM) as total_no_of_product_views                                                                   
                                                                            FROM `york-cdf-start.final_input_data.product_views` p inner join 
                                                                                 `york-cdf-start.final_input_data.customers` c 
                                                                                  on c.CUSTOMER_ID = p.CUSTOMER_ID                                                                            
                                                                            group by cast(c.CUST_TIER_CODE as string),cast(p.SKU as integer) """, project = "york-cdf-start", use_standard_sql=True)

        orders_sum = p | 'Read-sum' >> beam.io.ReadFromBigQuery(query="""SELECT cast(c.CUST_TIER_CODE as string) as cust_tier_code, o.SKU as sku, sum(o.ORDER_AMT) as total_sales_amount                                                                   
                                                                         FROM `york-cdf-start.final_input_data.orders` o inner join 
                                                                              `york-cdf-start.final_input_data.customers` c 
                                                                               on c.CUSTOMER_ID = o.CUSTOMER_ID                                                                            
                                                                         group by cast(c.CUST_TIER_CODE as string),o.SKU  """, project="york-cdf-start", use_standard_sql=True)



        product_views | "Write to bigquery1" >> beam.io.WriteToBigQuery(
            table_spec1,
            schema=cnt_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        orders_sum | "Write to bigquery2" >> beam.io.WriteToBigQuery(
            table_spec2,
            schema=sum_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

    # print
    print("Hello World")








