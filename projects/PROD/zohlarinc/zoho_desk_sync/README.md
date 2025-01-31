# README: Zoho Data and Vector Database Sync Process

This guide outlines the process for syncing Zoho Desk article metadata with vector data and managing the data workflow in two main scenarios:

1. **Scenario 1: Vector Database is Already Available**  
2. **Scenario 2: Only Zoho Data is Available**  

Each scenario requires specific steps to ensure that the data is processed, formatted, and stored correctly in the S3 bucket and the vector database (PostgreSQL).

---

## Scenario 1: Vector Database Already Exists

In this case, both Zoho Desk data and vector data are already available and synced. The process is straightforward, as you begin by running the scripts in the `initial_sync` folder starting with `01`.

### Process

1. **Initial Sync**  
   Begin by running the scripts in the `initial_sync` directory, starting from `01`. These scripts will ensure that both Zoho Desk data and vector data are aligned, following the same format.  
   After this process, the synced data (both Zoho and vector data) will be available in an S3 bucket.

2. **Switch to Upload Cycle**  
   Once the initial sync is completed, subsequent updates or changes to the data will follow the `upload_cycle` process. Any changes to Zoho Desk data or vector data will be managed via the upload cycle scripts to keep both datasets in sync.


---

## Scenario 2: Only Zoho Data is Available (No Vector Data Yet)

In this scenario, there is no existing vector database, and the process begins with Zoho Desk data only. For instance, if the customer is using only the Zoho knowledge base as a starting point, follow this process:

### Process

1. **Retrieve Zoho Metadata**  
   Start by running the script to extract the Zoho Desk metadata:

   ```bash
   project/zoho_desk_sync/initial_sync/04_get_zohodata_metadata_v2.0.py
   ```

2. **Convert Zoho Data to Vector Data Format**  
   Use the following script to convert the Zoho data into the required vector data format:

   ```bash
   project/zoho_desk_sync/initial_sync/07a_convert_zohodata_to_vectordata_format_v2.0.py
   ```

3. **S3 Bucket Setup**  
   Create an S3 bucket if not already done. Use the general bucket `agaile-shared-scraper-data-b9046561` and create a folder within this bucket for each tenant using their tenant name. Example:

   ```
   S3 Bucket: agaile-shared-scraper-data-b9046561
   Folder: agaile-aia-zohlarinc/
   ```

4. **Upload Synced Vector Data to S3**  
   After the conversion, upload the synced vector data to the S3 bucket using the script:

   ```bash
   project/zoho_desk_sync/initial_sync/09a_upload_synced_vectordata_2_s3_v2.0.py
   ```

5. **Identify Synced Vector Data to Enrich**  
   Once the vector data is uploaded, you can identify the synced vector data that needs enrichment by running the following script:

   ```bash
   project/zoho_desk_sync/update_cycle_GHA/04_identify_synced_vectordata_to_enrich_v2.0.py
   ```

6. **Update Vector Data**  
   Proceed with updating the vector data:

   ```bash
   project/zoho_desk_sync/update_cycle_GHA/05_update_vectordata_v2.0.py
   ```

7. **Upload Enriched Data to PostgreSQL**  
   Finally, upload the enriched data to the PostgreSQL database with Titan embeddings:

   ```bash
   project/zoho_desk_sync/update_cycle_GHA/06a_upload_to_postgres_with_titan_v2.0.py
   ```

---

## Additional Notes

### Checking for Titan Model Availability

If you encounter any issues related to the Titan model (such as a naming convention error or unavailability), you can check the available models using the following command:

```bash
aws bedrock list-foundation-models
```

This will list all available models in Bedrock, allowing you to verify whether the required model is present.

### Important Reminder: S3 Bucket and Folder Structure

Ensure that the S3 bucket is set up correctly:
- **Bucket**: `agaile-shared-scraper-data-b9046561`
- **Tenant Folder**: Create a separate folder for each tenant, such as `agaile-aia-zohlarinc/`.

---
