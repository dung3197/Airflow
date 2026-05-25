# Update the code **thoroughly** and **carefully** in **table_etl_base.py** so that **transform_data** function will look in to the PostgreSQL table named public.t_cust_customer_map** each time it is called to find which row in the upcoming dataframe has field **C_CUSTOMER_CODE**'s value exists in and which hasn't.

# The current **transform_data** follows planning of 4 steps: find new customer codes => if found non-existed customer code then call procedure => add to postgres table => drop column **C_CUSTOMER_CODE** in **final_upsert_df** and replace with **C_CUSTOMER_CODE_MAP** accordingly for each record in  **final_upsert_df**.  

## There are some requirements. 
*  **Read the README.md file to understand the purpose and the structure of the project**
*  **src/scripts/table_etl_base.py** is the file that describe specificly how each table present in **src/scripts/live/** are extract, transform and loaded
*  The  **transform_data** function is still in coding process and hasn't been finished yet
*  The line *#TODO: Logic check if c_customer_code and c_account_code in upcoming upsert batch exists in map table or not* is where the code is left hanging, you can dive into those codes bellow that line to understand which is being processed.
*  The "final_upsert_df" represent the transformed upsert data read by avro CDC file batches stored in **staging** bucket. 
*  It is required that for information security, all data from avro CDC file batches that has sensitive data like "C_CUSTOMER_CODE" must not use the exact value of its own but a mapped column which is "C_CUSTOMER_CODE_MAP" instead.
*  There's a procedure called **public.safe_insert_t_cust_customer_map** has already been created in PostgreSQL Server that serve adding new **C_CUSTOMER_CODE** and **C_CUSTOMER_CODE_MAP** if not exist to the **public.t_cust_customer_map** table and when on conflict meaning if there're more than 1 attempt to insert the same **C_CUSTOMER_CODE** to that table then do nothing.
*  If there're existed customer code in PosgreSQL table then drop column **C_CUSTOMER_CODE** in **final_upsert_df** and replace with **C_CUSTOMER_CODE_MAP** accordingly for each record in  **final_upsert_df** .
*  Don't waste time to search for "database-table-existed" **C_CUSTOMER_CODE** in **delete_df**. That's not necessary.
*  The updated code in **transform_data** function **must be clean** and **easy to understand**
*  **If it is possible, optimize the transform codes** to not follow 4 steps but may be less
*  With each table present in **final_upsert_df** that **C_CUSTOMER_CODE** column doesn't exists in then do nothing, just **final_upsert_df**. **transform_-data** function **WILL NOT** do anything with those upsert dataframes that do not contains **C_CUSTOMER_CODE**
*  **Remember** to add comments on each code updates to explain **specifically** what do those codes do.
*  Write a log in append only mode with timestamp to list out th diary of the whole code generation process
