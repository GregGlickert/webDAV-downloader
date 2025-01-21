# webDAV-downloader
### I could not find a webDAV interface that would work for me so I wrote my own. The WebDAVDownloader class can be ran with or without SLURM. For files that took a very long time to download or HPC clusters i prefer to use SLURM.

## Use guide
### First init the class. If you are not using SLURM it would look something like this
```python
downloader = WebDAVDownloader(
    url="https://your-webdav-url.com",
    username="your_username",
    password="your_password",
    num_cpus=4,  # Optional: defaults to os.cpu_count()
    use_slurm=False  # Optional: defaults to False
)
```

### If you wish to use SLRUM then init the class like this.
```python
downloader = WebDAVDownloader(use_slurm=True)
```

### For SLURM use case then have the batch file look something like this 
``` bash
#!/bin/bash
#SBATCH --job-name=download           # Job name
#SBATCH --nodes=1                     # Run on a single node
#SBATCH --ntasks=1                    # Run a single task
#SBATCH --cpus-per-task=2             # Number of CPU cores per task
#SBATCH --mem=32G                     # Memory limit
#SBATCH --time 7-0:00:00              # Time limit hrs:min:sec
#SBATCH --output=download.out         # Standard output and error log
#SBATCH --error=download.err          # Error output file


# Set environment variables for credentials
export WEBDAV_USERNAME="your_username"
export WEBDAV_PASSWORD="your_password"
export WEBDAV_URL="https://your-webdav-url.com"

# Run the Python script
python your_script.py
```
### Then you can download files using the download folder method
```python    
downloader.download_folder(
    remote_folder="folder_to_download",
    local_folder="local_name",
    base_dir="base_dir_of_remote_folder",
)
```