## MongoDB Slow Query Analyzer

This Python script analyzes your MongoDB log files to identify slow "find" queries, suggest potential indexes, and notify you via Google Chat if certain thresholds are met.

### Features

  * **Slow Query Identification:** Parses MongoDB log entries to find slow queries.
  * **Index Suggestion:** For "find" operations, it suggests indexes based on the query's filter and sort criteria.
  * **Aggregation:** Groups similar slow queries and calculates their count and average duration.
  * **Thresholding:** Only reports queries that exceed a configured occurrence threshold within a specified time frame.
  * **Google Chat Integration:** Sends a formatted report to a Google Chat webhook if slow queries are found.
  * **Configurable:** All key parameters are configurable via a JSON file, with the option to override them using command-line arguments.

### Prerequisites

  * Python 3.x
  * `requests` Python library: `pip install requests`
  * MongoDB log file in JSON format (usually configured in `mongod.conf` with `jsonFormat: true` under `systemLog`).

### Setup

1.  **Save the Python Script:** Save the provided Python code as `slow_analyzer.py`.

2.  **Create Configuration File:** In the same directory as `slow_analyzer.py`, create a file named `config.json` with your desired settings.

    **`config.json` example:**

    ```json
    {
      "MONGO_LOG_FILE": "/var/log/mongodb/mongod.log",
      "WEBHOOK_URL": "https://chat.googleapis.com/v1/spaces/AAAAI__Fp04/messages?key=XXXXXX&token=YYYYYY",
      "LAST_MINUTES": 5,
      "QUERY_THRESHOLD": 5
    }
    ```

      * `MONGO_LOG_FILE`: The absolute path to your MongoDB log file.
      * `WEBHOOK_URL`: Your Google Chat incoming webhook URL.
      * `LAST_MINUTES`: The time window (in minutes) to look back for slow queries.
      * `QUERY_THRESHOLD`: The minimum number of occurrences a specific slow "find" query must have within the `LAST_MINUTES` window to be included in the report and trigger a notification.

### Usage

The script can be run directly from the command line. It will use the configurations from `config.json` by default, but you can override them using command-line arguments.

**Basic Execution (using all defaults from `config.json`):**

```bash
python3 slow_analyzer.py
```

**Specifying a Custom Log File (overrides `MONGO_LOG_FILE` from `config.json`):**

```bash
python3 slow_analyzer.py /var/log/mongodb/my_custom_mongod.log
```

**Overriding Specific Configuration Parameters:**

You can override any parameter from `config.json` using the corresponding command-line argument:

  * `--last-minutes <minutes>`: Analyze logs from the last N minutes.
  * `--threshold <count>`: Set the minimum query occurrence count for reporting.
  * `--webhook-url <url>`: Provide a custom Google Chat webhook URL.
  * `--cron-mode`: Run in a less verbose mode suitable for cron jobs (only critical output to stdout/stderr).
  * `--config <path_to_config.json>`: Specify an alternative path for the configuration file.

**Examples:**

  * **Analyze last 10 minutes, report queries with \>= 3 occurrences:**
    ```bash
    python3 slow_analyzer.py --last-minutes 10 --threshold 3
    ```
  * **Run in cron mode with specific webhook and log file:**
    ```bash
    python3 slow_analyzer.py /path/to/your/mongod.log --cron-mode --webhook-url "https://chat.googleapis.com/v1/spaces/ANOTHER_WEBHOOK_URL"
    ```
  * **Use a custom configuration file:**
    ```bash
    python3 slow_analyzer.py --config /etc/my_mongo_analyzer/production_config.json
    ```

### Cron Job Example

To run this script periodically (e.g., every 5 minutes), you can set up a cron job.

1.  **Edit your crontab:**

    ```bash
    crontab -e
    ```

2.  **Add the following line:**

    ```cron
    */5 * * * * /usr/bin/python3 /path/to/your/slow_analyzer.py --cron-mode > /dev/null 2>&1
    ```

      * Replace `/path/to/your/slow_analyzer.py` with the actual path to your script.
      * `--cron-mode`: Ensures minimal output to stdout, making cron logs cleaner.
      * `> /dev/null 2>&1`: Redirects all standard output and standard error to `/dev/null` to prevent cron from sending email notifications for every run. Errors that trigger a webhook notification will still be sent to Google Chat by the Python script itself.

    If you want to log the script's output in cron, you can redirect it to a file:

    ```cron
    */5 * * * * /usr/bin/python3 /path/to/your/slow_analyzer.py --cron-mode >> /var/log/mongo_analyzer_cron.log 2>&1
    ```

### Exit Codes

The script uses specific exit codes when run in `--cron-mode`:

  * `0`: Slow "find" queries were found, and a report was generated (and potentially sent via webhook).
  * `1`: No slow "find" queries were found that met the specified criteria (e.g., `QUERY_THRESHOLD`). This is not an error state, simply "nothing to report".
  * `2`: A serious error occurred during script execution (e.g., log file not found, configuration file issues, JSON parsing errors). An error notification might be sent to the webhook if configured.
