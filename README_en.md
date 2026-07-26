# TGD

*Read this in other languages: [Русский](README.md)*

[![PyPI version](https://img.shields.io/badge/version-2.0.0-blue?style=flat-square)](https://github.com/tatarinovs/tgd/releases)
[![Python](https://img.shields.io/badge/python-3.9%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)](LICENSE)
[![Stars](https://img.shields.io/github/stars/tatarinovs/tgd?style=flat-square&logo=github)](https://github.com/tatarinovs/tgd/stargazers)
[![Issues](https://img.shields.io/github/issues/tatarinovs/tgd?style=flat-square)](https://github.com/tatarinovs/tgd/issues)
[![Last Commit](https://img.shields.io/github/last-commit/tatarinovs/tgd?style=flat-square)](https://github.com/tatarinovs/tgd/commits/main)

A powerful media file downloader for Telegram channels and groups, optimized for handling a large number of files.

## Key Features

- **Smart Download Manager & High Speed**:
  - Separate queues for "heavy" and "light" files.
  - Heavy files (default > 100 MB) are downloaded via `FastTelethonhelper` using multiple parallel connections.
  - Light files are downloaded concurrently in multiple threads without waiting for heavy downloads to finish.
- **Resilience**: Automatic retries on failure, timeout handling, and **2FA (Two-Factor Authentication)** support.
- **Daemon Mode (Daemon / WebUI)**:
  - Standalone background execution on NAS / servers with a web interface (`http://localhost:8080`).
  - Manage channel and topic lists: interactive start ("Start") and safe soft cancellation ("Stop").
  - **Soft-Stop**: cancellation instantly stops taking new files from the Telegram queue, allowing currently active downloads to finish without corruption.
  - Real-time display of current speed in MB/s, progress, and file count via Server-Sent Events (SSE).
- **Proxy Support (MTProto/SOCKS5/HTTP)**: Built-in capability to route traffic through proxies to bypass restrictions. Official Telegram links (`tg://proxy?...`) are supported, including **FakeTLS MTProxy** (secrets starting with `ee`).
- **Progress Bars**: Clear visual representation of the download process for each file using `tqdm` in the console.

## Installation

You can use the compiled version for Windows (no Python installation required) or run the script from the source code.

### Option 1: For Windows (Easy Method)

1. Go to the [Releases](https://github.com/tatarinovs/tgd/releases) section and download the latest version of `tgd.exe`.
2. Place the file in a convenient folder.
3. Run `tgd.exe`. The program will create a configuration file (`.env`), which you need to edit (see Configuration section).

### Option 2: For Linux / NAS / Running from source

**Requirements:** Python 3.9 or newer (Python 3.11+ is recommended).

1. Clone the repository:
   ```bash
   git clone https://github.com/tatarinovs/tgd.git
   cd tgd
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   *To verify media file integrity (`verify.py`), install additional libraries:*
   ```bash
   pip install Pillow av
   ```

3. Launch by executing the source files from the `src` folder:
   ```bash
   python src/tgd.py
   python src/verify.py
   ```

> [!NOTE]
> Below in the command examples, `tgd` is used. On Windows, this means running `tgd.exe`, and when working from source, it means `python src/tgd.py`.

## Configuration

Create a `.env` file in the root directory of the project or the program folder and add your Telegram API credentials:

```env
APP_API_ID=your_api_id
APP_API_HASH=your_api_hash
PHONE_NUMBER=+79991234567

# Additional parameters (optional):
# DEFAULT_DOWNLOAD_DIR=downloads # Default folder for saving files
# PROXY=socks5://192.168.1.10:10808
# PROXY=tg://proxy?server=192.168.1.10&port=443&secret=...
# TIMEOUT=3600
# RETRIES=3
# WORKERS=6              # Number of threads for light files
# HEAVY_WORKERS=1        # Number of threads for heavy files
# HEAVY_THRESHOLD=100    # Threshold for a "heavy" file in MB
# QUEUE_SIZE=50          # Maximum file queue length (prefetch depth)
```

### How to get an API ID and Hash:

1. Go to [my.telegram.org](https://my.telegram.org).
2. Log in using your phone number (the code will be sent to the Telegram app itself).
3. Go to the **"API development tools"** section.
4. Create a new application (fill in `App title` and `Short name` with any values, e.g., `MyDownloader`).
5. After clicking "Create application", you will see your `App api_id` and `App api_hash`.
6. Copy them and paste them into the corresponding fields in the `.env` file.

> [!IMPORTANT]
> Never share your `api_hash` with third parties. It is the secret key of your application.

## Daemon Mode (Daemon Mode & WebUI)

TGD can work as a 24/7 background service on a home server or NAS (Synology, QNAP, Raspberry Pi, Linux, OpenWrt, Windows).

### Starting the Daemon

```bash
# Start the web server on the local address (127.0.0.1:8080)
tgd --daemon

# Start with local network / external access (0.0.0.0:8080)
tgd --daemon --addr 0.0.0.0:8080

# Start with a specific folder for saving data and sessions
tgd --daemon --addr 0.0.0.0:8080 --data /var/lib/tgd
```

After launching, open your browser at `http://<SERVER_IP>:8080`.

> [!WARNING]
> The WebUI has no built-in authentication. **It is highly recommended not to** expose it to the open internet. Use the WebUI only within your local network, or set up access via VPN or a Reverse Proxy (e.g., Nginx) with basic HTTP authentication.

### WebUI Features:
- **Task Management**: Add channels/groups/topics via links `https://t.me/...`, `@username` or ID.
- **Start / Stop**:
  - `Start`: Launch downloading of the selected channel or topic.
  - `Stop`: Soft stop. Stops fetching new files from the Telegram queue and safely completes currently active file downloads without corrupting them.
- **Live Tracking (SSE)**: Download speed in MB/s, number of new and existing files are updated in the WebUI without page reloads.
- **Resilience**: Saves the list of groups and their statuses in `groups.json`. Upon daemon restart, incomplete tasks are auto-recovered to the "Interrupted" status.

### Autostart setup on Linux / NAS

#### 1. Via systemd (recommended)
Create a file `/etc/systemd/system/tgd.service`:

```ini
[Unit]
Description=TGD Telegram Downloader Daemon
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/tgd
ExecStart=/opt/tgd/venv/bin/python src/tgd.py --daemon --addr 0.0.0.0:8080
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start the service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tgd
```

#### 2. Via nohup (simple run)
```bash
nohup python src/tgd.py --daemon --addr 0.0.0.0:8080 > tgd.log 2>&1 &
```

## CLI Usage

Run the script through the terminal for a one-time download:

```bash
tgd [GROUP_ID] [OUTPUT_DIR]
# or from source:
python src/tgd.py [GROUP_ID] [OUTPUT_DIR]
```

### Parameters:
- `group_id`: Group or channel ID: number, link (`https://t.me/...`) or `@username`.
- `output_dir`: Path to the folder where files will be saved.

### Additional options (take priority over `.env`):

| Option | Default | Description |
|---|---|---|
| `-d`, `--daemon` | `False` | Run in background daemon mode with WebUI |
| `--addr` | `127.0.0.1:8080` | Web server address and port (in `-d` mode) |
| `--data` | — | Path to data folder (`groups.json`, `tg_session`) |
| `--env` | `.env` | Path to the `.env` file |
| `--timeout` | `3600` | Timeout for downloading a single file (sec) |
| `--retries` | `3` | Number of retry attempts on failure |
| `--workers` | `6` | Threads for light files |
| `--heavy-workers` | `1` | Threads for heavy files |
| `--heavy-threshold` | `100` | Threshold for a "heavy" file (MB) |
| `--queue-size` | `50` | Maximum file queue length (prefetch depth) |
| `--proxy` | — | Proxy server (SOCKS5/HTTP/MTProto) |
| `--topic` | — | Topic name or ID (forum/supergroup) |

> [!TIP]
> If a channel has a very large amount of heavy files, increase `--queue-size` (e.g. to `500`), so that light workers don't sit idle.

## CLI Usage Examples

### Regular channel or chat
```bash
tgd -1001234567890 downloads/my_channel
```

### Specific section (topic) in a supergroup/forum
You can specify either the **topic name** (the script will find it automatically) or its **ID**:
```bash
# By topic name (e.g. "Photos")
tgd -1001234567890 downloads/photos --topic "Photos"

# By topic ID (e.g. 42)
tgd -1001234567890 downloads/photos --topic 42
```

> [!TIP]
> If the specified topic name is not found, the downloader will automatically show you a list of all available sections in the group along with their IDs.

## File Integrity Verification

To bulk verify downloaded files (check if they are corrupted and can be opened), the `verify.py` script is provided.

```bash
tgv [FILE_DIR]
# or from source:
python src/verify.py [FILE_DIR]
```

### Features:
- Verify image structure (`.jpg`, `.png`, `.webp`, `.jpeg`).
- Verify video files (`.mp4`, `.mov`, `.mkv`, `.avi`, `.webm`) via the **PyAV** library.
- Recursive file search in all subdirectories.
- Interactive input for parameters if they are not provided at launch.
- Automatic removal of broken files using the `--delete` flag.

### Example:
```bash
tgv downloads/my_channel --delete
```
