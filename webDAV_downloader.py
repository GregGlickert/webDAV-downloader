import re
import os
import time
import logging
import requests
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class WebDAVDownloader:
    def __init__(
        self,
        url: str = None,
        username: str = None,
        password: str = None,
        num_cpus: int = None,
        use_slurm: bool = False
    ):
        # Initialize with either direct parameters or environment variables
        self.use_slurm = use_slurm
        
        # Get SLURM information if enabled
        if self.use_slurm:
            self.slurm_cpus = int(os.environ.get('SLURM_CPUS_PER_TASK', 1))
            self.slurm_node = os.environ.get('SLURM_NODENAME', 'local')
            self.job_id = os.environ.get('SLURM_JOB_ID', 'no_job')
        else:
            self.slurm_cpus = num_cpus or os.cpu_count() or 1
            self.slurm_node = 'local'
            self.job_id = 'no_job'

        # Set credentials with priority to direct parameters
        self.base_url = url or os.environ.get('WEBDAV_URL')
        self.username = username or os.environ.get('WEBDAV_USERNAME')
        self.password = password or os.environ.get('WEBDAV_PASSWORD')
        
        if not all([self.base_url, self.username, self.password]):
            raise ValueError("Missing required credentials. Provide either direct parameters or environment variables: WEBDAV_URL, WEBDAV_USERNAME, WEBDAV_PASSWORD")
            
        self.base_url = self.base_url.rstrip('/')
        self.auth = (self.username, self.password)
        
        # Configure logging
        self._setup_logging()
        
        # Setup session with optimized settings
        self.session = self._create_session()
        
        self.logger.info(f"Initializing WebDAV downloader on node {self.slurm_node} "
                        f"with {self.slurm_cpus} CPUs")

    def _setup_logging(self):
        """Setup logging with node information"""
        env_type = "slurm" if self.use_slurm else "local"
        log_file = f"download_{env_type}_node_{self.slurm_node}_job_{self.job_id}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - [Node:%(node_name)s] %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger = logging.LoggerAdapter(
            self.logger,
            {"node_name": self.slurm_node}
        )

    def _create_session(self) -> requests.Session:
        """Create an optimized session"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "PROPFIND"]
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=self.slurm_cpus,
            pool_maxsize=self.slurm_cpus * 2
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    # The rest of the methods remain the same as they don't need modification
    def list_directory(self, remote_folder: str) -> List[str]:
        """List contents of a WebDAV directory with error handling"""
        headers = {'Depth': '1'}
        try:
            response = self.session.request(
                'PROPFIND', 
                f"{self.base_url}{remote_folder}", 
                auth=self.auth, 
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            if response.status_code == 207:
                tree = ET.fromstring(response.content)
                return [elem.text for elem in tree.findall('.//{DAV:}href')]
            return []
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to list directory {remote_folder}: {str(e)}")
            raise

    def filter_string(self, base_string: str, input_string: str) -> Optional[str]:
        """Filter string with error handling"""
        try:
            pattern = rf"/{re.escape(base_string)}.*"
            match = re.search(pattern, input_string)
            return match.group(0) if match else None
        except re.error as e:
            self.logger.error(f"Regex error in filter_string: {str(e)}")
            raise

    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download a single file with progress tracking"""
        temp_path = local_path + f".{self.job_id}.temp"
        
        try:
            response = self.session.get(
                f"{self.base_url}{remote_path}", 
                auth=self.auth, 
                stream=True,
                timeout=60
            )
            response.raise_for_status()
            
            file_size = int(response.headers.get('content-length', 0))
            file_size_gb = file_size / (1024**3)
            local_path_obj = Path(local_path)
            
            local_path_obj.parent.mkdir(parents=True, exist_ok=True)
            
            if local_path_obj.exists() and local_path_obj.stat().st_size == file_size:
                self.logger.info(f"File already exists and complete: {local_path} ({file_size_gb:.2f} GB)")
                return True
            
            self.logger.info(f"Starting download of {local_path} (Total size: {file_size_gb:.2f} GB)")
            
            with open(temp_path, "wb") as file:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
                        downloaded += len(chunk)
                        
                        if file_size > 100*1024*1024 and downloaded % (50*1024*1024) == 0:
                            progress = (downloaded / file_size) * 100
                            downloaded_gb = downloaded / (1024**3)
                            self.logger.info(
                                f"Node {self.slurm_node} downloading {local_path}: "
                                f"{progress:.1f}% ({downloaded_gb:.2f} GB of {file_size_gb:.2f} GB)"
                            )
            
            if os.path.getsize(temp_path) != file_size:
                raise ValueError(f"Downloaded file size mismatch for {local_path}")
            
            os.rename(temp_path, local_path)
            self.logger.info(f"Node {self.slurm_node} completed: {local_path} ({file_size_gb:.2f} GB)")
            return True
            
        except Exception as e:
            self.logger.error(f"Node {self.slurm_node} failed to download {remote_path}: {str(e)}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return False

    def download_folder(self, remote_folder: str, local_folder: str, base_dir: str) -> None:
        """Download a folder using allocated CPUs"""
        try:
            Path(local_folder).mkdir(parents=True, exist_ok=True)
            items = self.list_directory(remote_folder)
            if not items:
                return
                
            items.pop(0)  # Remove the first element (directory name)
            
            failed_downloads = []
            with ThreadPoolExecutor(max_workers=self.slurm_cpus) as executor:
                future_to_path = {}
                
                for item in items:
                    item = self.filter_string(base_dir, item)
                    if not item:
                        continue
                        
                    if item.endswith('/'):
                        self.download_folder(
                            item, 
                            str(Path(local_folder) / Path(item[:-1]).name),
                            base_dir
                        )
                    else:
                        local_path = str(Path(local_folder) / Path(item).name)
                        future = executor.submit(self.download_file, item, local_path)
                        future_to_path[future] = local_path

                for future in as_completed(future_to_path):
                    local_path = future_to_path[future]
                    try:
                        if not future.result():
                            failed_downloads.append(local_path)
                    except Exception as e:
                        self.logger.error(
                            f"Node {self.slurm_node} error downloading {local_path}: {str(e)}"
                        )
                        failed_downloads.append(local_path)

            if failed_downloads:
                with open(f"failed_downloads_{self.job_id}.txt", "a") as f:
                    for path in failed_downloads:
                        f.write(f"{path}\n")
                
                self.logger.error(f"Failed downloads ({len(failed_downloads)}): {failed_downloads}")

        except Exception as e:
            self.logger.error(f"Node {self.slurm_node} error in download_folder: {str(e)}")
            raise