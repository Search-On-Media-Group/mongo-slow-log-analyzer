#!/usr/bin/env python3

import argparse
import json
from collections import defaultdict
import sys
from datetime import datetime, timedelta, timezone
import requests
import os # Added for path validation and configuration file loading

# --- Configuration File Path ---
CONFIG_FILE_PATH = "config.json" # Default config file name in the same directory

# Function to load configuration from JSON file
def load_config(config_path):
    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found at '{config_path}'.", file=sys.stderr)
        sys.exit(2)
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in configuration file '{config_path}': {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"Error loading configuration file '{config_path}': {e}", file=sys.stderr)
        sys.exit(2)


def normalize_structure_by_keys(item, placeholder="?"):
    """
    Sostituisce ricorsivamente tutti i valori in un dizionario o lista
    con un placeholder, mantenendo la struttura delle chiavi.
    Ordina le chiavi dei dizionari per una rappresentazione canonica.
    Questo è usato per operazioni non-'find' se si volesse analizzare la loro struttura.
    """
    if isinstance(item, dict):
        return {k: normalize_structure_by_keys(item[k], placeholder) for k in sorted(item.keys())}
    elif isinstance(item, list):
        return [normalize_structure_by_keys(elem, placeholder) for elem in item]
    else:
        return placeholder

def get_query_signature_and_duration(log_entry):
    """
    Estrae una firma univoca e la durata dalla voce di log di una query lenta.
    Per le query 'find', la firma è un suggerimento di indice JSON.
    Per altre operazioni, è una struttura normalizzata con '?'.
    Restituisce una tupla: (namespace, op_type, details_str_signature, duration_ms) o None.
    """
    try:
        attributes = log_entry.get("attr", {})
        command = attributes.get("command", {})
        namespace = attributes.get("ns", "unknown_namespace")
        duration_ms = attributes.get("durationMillis")

        if duration_ms is None:
            return None

        op_type = "unknown_op"
        details_obj_for_normalization = {}
        details_str_signature = ""

        if "find" in command:
            op_type = "find"

            raw_filter = command.get("filter", command.get("query", {}))
            raw_sort = command.get("sort", {})

            suggested_index = {}

            if isinstance(raw_filter, dict):
                for key in sorted(raw_filter.keys()):
                    suggested_index[key] = 1

            if isinstance(raw_sort, dict):
                for key in sorted(raw_sort.keys()):
                    if key not in suggested_index:
                        value = raw_sort[key]
                        is_descending = False
                        try:
                            if int(value) == -1:
                                is_descending = True
                        except (ValueError, TypeError):
                            pass

                        suggested_index[key] = -1 if is_descending else 1

            details_obj_for_json = suggested_index if suggested_index else {"note": "no_specific_fields_in_filter_or_sort"}
            details_str_signature = json.dumps(details_obj_for_json)

        else:
            if "aggregate" in command:
                op_type = "aggregate"
                pipeline = command.get("pipeline", [])
                details_obj_for_normalization = pipeline if isinstance(pipeline, list) else []
            elif "count" in command:
                op_type = "count"
                details_obj_for_normalization = command.get("query", {})
            elif "update" in command:
                op_type = "update"
                updates = command.get("updates", [])
                details_obj_for_normalization = updates[0].get("q", {}) if updates and isinstance(updates, list) and isinstance(updates[0], dict) else command.get("q", {})
            elif "delete" in command:
                op_type = "delete"
                deletes = command.get("deletes", [])
                details_obj_for_normalization = deletes[0].get("q", {}) if deletes and isinstance(deletes, list) and isinstance(deletes[0], dict) else command.get("q", {})
            elif "insert" in command:
                op_type = "insert"
                details_obj_for_normalization = {"docs_count": len(command.get("documents", []))}
            elif "getMore" in command:
                op_type = "getMore"
                details_obj_for_normalization = {"cursorId": "?", "collection": "?"}
            elif "distinct" in command:
                op_type = "distinct"
                details_obj_for_normalization = {"key": command.get("key", "?"), "query": command.get("query", {})}
            elif "findAndModify" in command:
                op_type = "findAndModify"
                details_obj_for_normalization = {
                    "query": command.get("query",{}), "sort": command.get("sort", {}),
                    "update": command.get("update", {}), "new": "?", "remove": "?"
                }
            else:
                op_type = list(command.keys())[0] if command and command.keys() else "unknown_op_fallback"
                details_obj_for_normalization = command

            normalized_structure = normalize_structure_by_keys(details_obj_for_normalization)
            details_str_signature = json.dumps(normalized_structure, sort_keys=True)

        return (namespace, op_type, details_str_signature, int(duration_ms))

    except Exception as e:
        # print(f"Warning: Could not parse signature from entry: {log_entry}. Error: {e}", file=sys.stderr)
        return None

def analyze_slow_logs(logfile_path, last_minutes=None, cron_mode=False):
    """
    Analizza il file di log di MongoDB, estrae le query lente,
    le raggruppa e aggrega conteggi e durate.
    Filtra opzionalmente per i log degli ultimi X minuti.
    """
    query_aggregator = defaultdict(lambda: {'count': 0, 'total_duration': 0})
    processed_lines = 0
    slow_query_entries = 0
    filtered_by_time = 0

    cutoff_time_utc = None
    if last_minutes is not None:
        current_time_utc = datetime.now(timezone.utc)
        cutoff_time_utc = current_time_utc - timedelta(minutes=last_minutes)
        if not cron_mode:
            print(f"Filtraggio log: verranno considerati solo i log a partire da {cutoff_time_utc.isoformat()}")

    try:
        with open(logfile_path, 'r', encoding='utf-8') as f:
            for line_number, line in enumerate(f, 1):
                processed_lines += 1
                try:
                    log_entry = json.loads(line)

                    if cutoff_time_utc:
                        ts_data = log_entry.get("t", {}).get("$date")
                        if not ts_data:
                            continue

                        try:
                            if ts_data.endswith('Z'):
                                ts_data_parsed = ts_data[:-1] + '+00:00'
                            else:
                                ts_data_parsed = ts_data
                            log_ts_utc = datetime.fromisoformat(ts_data_parsed)
                            if log_ts_utc.tzinfo is None:
                                log_ts_utc = log_ts_utc.replace(tzinfo=timezone.utc)
                            if log_ts_utc < cutoff_time_utc:
                                filtered_by_time +=1
                                continue
                        except ValueError as ve:
                            # print(f"Warning: Could not parse timestamp '{ts_data}' at line {line_number}: {ve}", file=sys.stderr)
                            continue

                    signature_data = get_query_signature_and_duration(log_entry)
                    if signature_data:
                        slow_query_entries +=1
                        namespace, op_type, details_str_signature, duration_ms = signature_data
                        key = (namespace, op_type, details_str_signature)
                        query_aggregator[key]['count'] += 1
                        query_aggregator[key]['total_duration'] += duration_ms

                except json.JSONDecodeError:
                    # print(f"Warning: Skipping non-JSON line {line_number}: {line.strip()}", file=sys.stderr)
                    pass
                except Exception as e:
                    print(f"Warning: Error processing line {line_number}: {e}", file=sys.stderr)
                    pass

    except FileNotFoundError:
        print(f"Errore: File di log '{logfile_path}' non trovato.", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Errore durante la lettura del file '{logfile_path}': {e}", file=sys.stderr)
        return None

    if not cron_mode:
        print(f"Righe totali elaborate: {processed_lines}")
        if last_minutes is not None:
            print(f"Righe filtrate per tempo (più vecchie di {last_minutes} minuti): {filtered_by_time}")
        print(f"Voci di query lente analizzate (dopo il filtro temporale): {slow_query_entries}")
    return query_aggregator

def print_report(query_aggregator, cron_mode=False, threshold=0):
    """
    Stampa il report delle query lente (solo 'find') in un formato tabellare,
    ordinate per durata media decrescente. Mostra il suggerimento di indice.
    Filtra le query 'find' basandosi sulla soglia di conteggio.
    Returns the formatted report string if 'find' queries were found and met the threshold,
    otherwise returns an empty string.
    """
    if not query_aggregator:
        if not cron_mode:
            print("Nessuna query lenta trovata o analizzabile (dopo i filtri applicati).")
        return ""

    find_queries_data = []
    for key, data in query_aggregator.items():
        namespace, op_type, details_str_signature = key
        if op_type == "find":
            count = data['count']
            # Apply the threshold here
            if count >= threshold:
                total_duration = data['total_duration']
                avg_duration = total_duration / count if count > 0 else 0
                find_queries_data.append({
                    "namespace": namespace,
                    "op_type": op_type,
                    "details_signature": details_str_signature,
                    "count": count,
                    "avg_duration": avg_duration
                })

    if not find_queries_data:
        if not cron_mode:
            print(f"Nessuna query 'find' lenta trovata nel log che superi la soglia di {threshold} occorrenze (dopo i filtri applicati).")
        return ""

    sorted_find_queries = sorted(find_queries_data, key=lambda item: item['avg_duration'], reverse=True)

    header_format = "| {:<5} | {:<17} | {:<30} | {:<15} | {} |"
    header_str = header_format.format(
        "Count", "Avg Duration (ms)", "Namespace", "Operation", "Suggested Index"
    )
    separator_parts = ["-" * 7, "-" * 19, "-" * 32, "-" * 17]
    
    max_sig_len = 0
    if sorted_find_queries: # Check if list is not empty
        # Ensure details_signature exists and is a string for len()
        sig_lengths = [len(str(q.get('details_signature', ''))) for q in sorted_find_queries]
        if sig_lengths: # Ensure there are lengths to compare
             max_sig_len = max(sig_lengths)

    # Determine suggested_index_col_width based on max_sig_len or a default
    default_suggested_index_col_width = 50 # A reasonable default if no signatures or all are short
    suggested_index_col_width = max(default_suggested_index_col_width, max_sig_len)

    separator = f"+{separator_parts[0]}+{separator_parts[1]}+{separator_parts[2]}+{separator_parts[3]}+{'-' * (suggested_index_col_width + 2)}+"

    report_output = []
    report_output.append(separator)
    report_output.append(header_str)
    report_output.append(separator)

    for query_data in sorted_find_queries:
        report_output.append(f"| {query_data['count']:<5} | {query_data['avg_duration']:<17.2f} | {query_data['namespace']:<30} | {query_data['op_type']:<15} | {str(query_data.get('details_signature', '')):<{suggested_index_col_width}} |")
    report_output.append(separator)

    return "\n".join(report_output)

def send_webhook_notification(webhook_url, message_content):
    """
    Sends a message to the specified Google Chat webhook URL.
    """
    json_payload = {
        "text": message_content
    }
    try:
        response = requests.post(webhook_url, json=json_payload)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
        print("Notification sent successfully to Google Chat.")
    except requests.exceptions.RequestException as e:
        print(f"Error sending notification to Google Chat: {e}", file=sys.stderr)
        print(f"Payload: {json.dumps(json_payload)}", file=sys.stderr)

if __name__ == "__main__":
    # Load default configurations from JSON file first
    # This happens before argparse, so command-line arguments can override these defaults
    config_defaults = load_config(CONFIG_FILE_PATH)

    parser = argparse.ArgumentParser(
        description="Analizza i log delle query 'find' lente di MongoDB, suggerisce indici, raggruppa per indice e ordina per tempo medio. Le configurazioni sono lette da un file JSON e possono essere sovrascritte da argomenti da riga di comando."
    )
    # Logfile can still be a positional argument or use default from config
    parser.add_argument("logfile", nargs='?', default=config_defaults.get("MONGO_LOG_FILE"),
                        help=f"Percorso del file di log di MongoDB (formato JSON). Default: {config_defaults.get('MONGO_LOG_FILE')}")
    
    parser.add_argument(
        "--last-minutes",
        type=int,
        default=config_defaults.get("LAST_MINUTES"),
        help=f"Analizza solo i log degli ultimi X minuti. Default: {config_defaults.get('LAST_MINUTES')}"
    )
    parser.add_argument(
        "--cron-mode",
        action="store_true",
        help="Modalità Cron: output ridotto e exit code specifici (0=queries 'find' riportate, 1=nessuna query 'find' o errore lieve, 2=errore grave)."
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=config_defaults.get("QUERY_THRESHOLD"),
        help=f"Soglia minima di occorrenze per una query 'find' per essere riportata. Default: {config_defaults.get('QUERY_THRESHOLD')}"
    )
    parser.add_argument(
        "--webhook-url",
        default=config_defaults.get("WEBHOOK_URL"),
        help=f"URL del webhook di Google Chat per inviare le notifiche. Default: {config_defaults.get('WEBHOOK_URL')}"
    )
    # Optional argument to specify a different config file path
    parser.add_argument(
        "--config",
        default=CONFIG_FILE_PATH,
        help=f"Percorso del file di configurazione JSON. Default: {CONFIG_FILE_PATH}"
    )


    args = parser.parse_args()

    # If --config argument was used, reload configuration from that path
    # This allows overriding the default config file path
    if args.config != CONFIG_FILE_PATH:
        print(f"Using custom configuration file: {args.config}")
        config_overrides = load_config(args.config)
        # Apply overrides from the custom config file
        for key, value in config_overrides.items():
            if hasattr(args, key.lower()): # Check if an argparse arg matches config key
                setattr(args, key.lower(), value)
        # Re-set logfile if it was not explicitly provided but present in new config
        if not args.logfile and config_overrides.get("MONGO_LOG_FILE"):
             args.logfile = config_overrides.get("MONGO_LOG_FILE")


    # Use the resolved log file path (from arg or config)
    log_file_to_use = args.logfile

    if not args.cron_mode:
        print(f" Elaborazione delle query lente da: {log_file_to_use}")
        print(f" Soglia di occorrenze per il report: {args.threshold}")
        print(f" Durata analisi (minuti): {args.last_minutes}")
        print(f" URL Webhook: {args.webhook_url}")

    # Validate log file path
    if not log_file_to_use:
        error_msg = "Error: MongoDB log file path not specified in config or as argument."
        print(error_msg, file=sys.stderr)
        if args.webhook_url:
            send_webhook_notification(args.webhook_url, error_msg)
        sys.exit(2)
        
    if not os.path.exists(log_file_to_use):
        error_msg = f"Error: MongoDB log file not found at '{log_file_to_use}'."
        print(error_msg, file=sys.stderr)
        if args.webhook_url:
            send_webhook_notification(args.webhook_url, error_msg)
        sys.exit(2)


    aggregated_queries = analyze_slow_logs(log_file_to_use, args.last_minutes, args.cron_mode)

    if aggregated_queries is None: # Error during analysis (e.g., file read error)
        if not args.cron_mode:
            print(" Analisi fallita (es. errore di lettura file).")
        if args.webhook_url:
            send_webhook_notification(args.webhook_url, f"Errore: Analisi dei log fallita per {log_file_to_use} (es. errore di lettura).")
        sys.exit(2) # Serious error

    report_content = print_report(aggregated_queries, args.cron_mode, args.threshold)

    if not args.cron_mode:
        print(" Analisi completata.")

    if report_content:
        # This means print_report found 'find' queries and returned content.
        if not args.cron_mode:
            print(report_content) # Print to stdout if not in cron mode
        
        if args.webhook_url:
            message_content = f"Recent slow MongoDB queries found (last {args.last_minutes} minutes, threshold: {args.threshold}):\n\`\`\`\n{report_content}\n\`\`\`"
            send_webhook_notification(args.webhook_url, message_content)
        
        sys.exit(0) # Success, 'find' query report was generated and printed.
    else:
        # This means aggregated_queries was not empty (so some slow queries might have been found),
        # but print_report did not return anything (e.g., no 'find' queries, or none met the threshold).
        if not args.cron_mode:
            print(f"No 'find' queries found that meet the threshold of {args.threshold} occurrences.")
        sys.exit(1) # No 'find' report generated, treat as "nothing to notify" for the shell script.
