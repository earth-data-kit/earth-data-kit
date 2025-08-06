# How to Run Earth Data Kit Examples

Follow these steps to set up and run the Earth Data Kit examples:

1. **Add a `.env` file from the provided `sample.env`**

   Copy the sample environment file to `.env`:
   ```bash
   cp sample.env .env
   ```

2. **Set up Google Cloud credentials and configure AWS CLI**

   - For Google Cloud, create a service account and download the credentials JSON file. Set the environment variable:
     ```bash
     export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"
     ```
   - For AWS, configure your credentials:
     ```bash
     aws configure
     ```
     Enter your AWS Access Key, Secret Key, region, and output format as prompted.

3. **Start the EDK container**

   Run the initialization script to start the Earth Data Kit container:
   ```bash
   bash init.sh
   ```

4. **Run the example script**

   Use the launcher script to run the example (e.g., `stitching.py`). Update any file paths in the script as needed for your environment:
   ```bash
   bash launcher.sh stitching.py
   ```

**Note:**  
- Make sure to update any file paths (such as credentials or data locations) in the scripts to match your local setup.
- For more details, refer to the documentation or the example scripts in the `examples/` directory.
