# Dataproc Pi Calculation Script

## Overview
This Python script automates the process of setting up a Google Cloud Dataproc cluster, running a Spark job to calculate the value of Pi, and cleaning up resources after execution. The results are stored in a designated Google Cloud Storage bucket.

---

## Prerequisites
Before running this script, ensure the following:

1. **Google Cloud SDK** is installed and authenticated with appropriate permissions.
2. The **Dataproc API** is enabled for your Google Cloud project.
3. A **Google Cloud Storage bucket** is available for storing Spark job files and output results.
4. Python dependencies installed via pip:
   ```bash
   pip install google-cloud-dataproc google-cloud-storage
   ```

---

## Configuration
Modify the following parameters in the script to match your setup:

- `PROJECT_ID`: Your Google Cloud project ID.
- `REGION`: The region where the Dataproc cluster will be created (e.g., `us-central1`).
- `CLUSTER_NAME`: The name of the Dataproc cluster (e.g., `example-cluster`).
- `BUCKET_NAME`: The name of your Google Cloud Storage bucket.
- `JOB_FILE_NAME`: The name of the Python file for the Spark job (e.g., `pi-calculation.py`).

---

## How It Works
1. **Delete Existing Cluster**:
   - The script attempts to delete any existing Dataproc cluster with the specified name.

2. **Create a New Cluster**:
   - A new Dataproc cluster is created with the following configurations:
     - **1 Master Node**: `n2-standard-2` with 50 GB boot disk.
     - **2 Worker Nodes**: `n2-standard-2` with 50 GB boot disk each.

3. **Upload Spark Job to GCS**:
   - The Spark job code is uploaded to the specified Google Cloud Storage bucket.

4. **Submit Spark Job**:
   - The Spark job calculates the value of Pi using a Monte Carlo simulation with 10 partitions.
   - The result is stored in the bucket at `gs://<BUCKET_NAME>/pi-results`.

5. **Clean Up**:
   - The script does not automatically delete the cluster after the job finishes. Run the script again to manually trigger cleanup if needed.

---

## Script Execution
Run the script using Python:

```bash
python <script_name>.py
```

---

## Expected Output
The script will print logs indicating the status of each step. Upon successful execution:

1. The Dataproc cluster will be created and used to execute the Spark job.
2. The Spark job will calculate the value of Pi.
3. Results will be saved in your Cloud Storage bucket at:
   ```
   gs://<BUCKET_NAME>/pi-results
   ```
4. Logs will indicate the driver output location, which can be checked for detailed execution results.

---

## Notes
- Ensure sufficient quotas are available in your Google Cloud project for creating Dataproc clusters.
- Results stored in `pi-results` are overwritten upon subsequent executions. To preserve results, consider using unique paths for each execution.
- Debug any issues by checking the logs in Cloud Storage or the Dataproc Job page in the Google Cloud Console.

---

## Troubleshooting
- **Cluster Creation Fails**:
  - Check quotas for CPUs and disk storage in your Google Cloud project.
  - Ensure the Dataproc API is enabled.

- **Spark Job Fails**:
  - Verify the Spark job file is correctly uploaded to the bucket.
  - Check the logs in the driver output for errors.

---

## Cleanup
To avoid unnecessary costs, delete the Dataproc cluster and other unused resources after execution.

```bash
gcloud dataproc clusters delete <CLUSTER_NAME> --region <REGION>
```

