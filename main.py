
import psycopg2
import pandas as pd
import json
import apache_beam as beam
import ast
from apache_beam.io import WriteToText, fileio
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.internal.clients import bigquery
from datetime import datetime
from google.cloud import pubsub_v1




if __name__ == '__main__':
    # table_schema = {
    #     'fields': [
    #         {'name': 'title', 'type': 'STRING', 'mode': 'Required'},
    #         {'name': 'release_year', 'type': 'INTEGER', 'mode': 'Nullable'},
    #         {'name': 'rating', 'type': 'STRING', 'mode': 'Nullable'},
    #         {'name': 'special_features', 'type': 'STRING', 'mode': 'Nullable'},
    #         {'name': 'category', 'type': 'STRING', 'mode': 'Nullable'}
    #     ]}
    #
    # table_spec1 = bigquery.TableReference(
    #     projectId = 'york-cdf-start',
    #     datasetId='final-ben-huang',
    #     tableId='special_films')

    # read_options = PipelineOptions(
    #     #runner="DataflowRunner",
    #     project="york-cdf-start",
    #     region="us-central1",
    #     custom_gcs_temp_location="gs://york_temp_files/tmp6")
    #     #job_name="ben-huang-final-job")

      ## Read from pgAdmin on aws
    conn = psycopg2.connect(database="benhuangus1", user="dbmasteruser",
                            password="...",
                            host="ls-41d379b19b475ed294babb170cfa0f93b3011e47.cq2f1e9koedo.us-east-2.rds.amazonaws.com")
    print("Opened database successfully");
    #cur = conn.cursor()

    sql = """select f.title, 
                    f.release_year,
        	        f.rating,
        	        f.special_features,
        	        c.name as category
                    from public.film f inner join public.film_category fc
                         on f.film_id = fc.film_id inner join public.category c
        	             on fc.category_id = c.category_id
                    where f.title in (
        	                        select f.title from public.film f inner join public.film_actor fa
        	                        on f.film_id = fa.film_id inner join public.actor ac
        	                        on fa.actor_id = ac.actor_id
        	                        where ac.first_name LIKE 'B%')
                    limit 1000 """

    df = pd.read_sql(sql, conn)
    df.to_csv('gs://york-project-bucket/films2')

    ## convert to Json if needed
    # output = df.to_json(orient='records')[1:-1].replace('},{', '} {')

    from google.cloud import bigquery
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.

    table_id = "york-cdf-start.final_ben_huang.aws-table-load_special-films"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("release_year", "INTEGER"),
            bigquery.SchemaField("rating", "STRING"),
            bigquery.SchemaField("special_features", "STRING"),
            bigquery.SchemaField("category", "STRING"),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV...
        source_format=bigquery.SourceFormat.CSV,
    )
    uri = "https://storage.cloud.google.com/york-project-bucket/films2"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows..complete".format(destination_table.num_rows))

    #options = beam.options.pipeline_options.PipelineOptions(streaming=True)
    # with beam.Pipeline() as p:
    #
    #     data = (p | beam.io.ReadFromText(
    #             "gs://york-project-bucket/temp9")
    #               | beam.Map(print)
    #             )

        # data | "Write-Table" >> beam.io.WriteToBigQuery(
        #     table_spec1,
        #     schema=table_schema,
        #     custom_gcs_temp_location = "gs://york-project-bucket/temp6",
        #     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)





    pass












