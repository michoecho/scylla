#!/usr/bin/env python3

# Given a sqlite file containing a database like this:

# CREATE TABLE tests (
#         id INTEGER PRIMARY KEY,
#         architecture VARCHAR(15) NOT NULL,
#         directory VARCHAR(255),
#         mode VARCHAR(15) NOT NULL,
#         run_id INTEGER,
#         test_name VARCHAR(255) NOT NULL
#     );
# INSERT INTO tests VALUES(1,'x86_64','cqlpy','release',2,'cassandra_tests/validation/entities/uf_types_test');
# INSERT INTO tests VALUES(2,'x86_64','cqlpy','release',1,'cassandra_tests/validation/entities/uf_types_test');
# INSERT INTO tests VALUES(3,'x86_64','cqlpy','release',3,'cassandra_tests/validation/entities/uf_types_test');
# INSERT INTO tests VALUES(4,'x86_64','cqlpy','release',2,'cassandra_tests/validation/entities/types_test');
# INSERT INTO tests VALUES(5,'x86_64','cqlpy','release',1,'cassandra_tests/validation/entities/types_test');
# INSERT INTO tests VALUES(6,'x86_64','cqlpy','release',3,'cassandra_tests/validation/entities/types_test');
# CREATE TABLE test_metrics (
#         id INTEGER PRIMARY KEY,
#         test_id INT NOT NULL,
#         user_sec REAL,
#         system_sec REAL,
#         usage_sec REAL,
#         memory_peak INTEGER,
#         time_taken REAL,
#         time_start DATETIME,
#         time_end DATETIME,
#         success BOOLEAN,
#         FOREIGN KEY(test_id) REFERENCES tests(id)
#     );
# INSERT INTO test_metrics VALUES(1,2,0.7899070000000000257,0.3572560000000000179,1.147164000000000073,121507840,43.75275468826293945,'2025-06-13T14:45:33.671953','2025-06-13T14:46:17.424708',1);
# INSERT INTO test_metrics VALUES(2,3,0.7552670000000000216,0.3714330000000000131,1.126700000000000034,120729600,45.65960192680358887,'2025-06-13T14:45:33.694637','2025-06-13T14:46:19.354239',1);
# INSERT INTO test_metrics VALUES(3,4,0.7239489999999999537,0.3850939999999999919,1.109043999999999918,121126912,45.97333788871765136,'2025-06-13T14:45:33.698658','2025-06-13T14:46:19.671996',1);
# INSERT INTO test_metrics VALUES(4,1,0.7441330000000000444,0.3736200000000000076,1.117752999999999997,120827904,46.37005019187927246,'2025-06-13T14:45:33.597585','2025-06-13T14:46:19.967635',1);
# INSERT INTO test_metrics VALUES(5,5,0.6424950000000000382,0.2777640000000000108,0.920259999999999967,120987648,32.64649248123168945,'2025-06-13T14:46:17.819143','2025-06-13T14:46:50.465636',1);

# Write a python function (which takes the sqlite file path as the argument) which opens the database,
# computes the max memory_peak, the mean usage_sec, and the average time_taken for each (mode,directory,test_name) triple,
# and returns the result as a python dictionary.

# Add debug logging to the function.

# Add a main/init function which takes --verbose and the filename path via argparse,
# sets up the logger (possibly with verbose)
# and runs the function on the path.
#
# Add typing hints.
#
# Do the aggregations in SQL rather than Python if possible.
#
#


# Here's a complete Python script that meets your requirements:

# ```python
#!/usr/bin/env python3

import argparse
import logging
import sqlite3
from typing import Dict, Tuple, Any, List

def analyze_test_metrics(db_path: str) -> Dict[Tuple[str, str, str], Dict[str, float]]:
    """
    Analyzes test metrics from a SQLite database and returns aggregated statistics.

    Args:
        db_path: Path to the SQLite database file

    Returns:
        Dictionary with (mode, directory, test_name) tuples as keys and dictionaries of metrics as values
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Opening database at {db_path}")

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        logger.debug("Executing SQL query to aggregate metrics")
        query = """
        SELECT
            t.mode,
            t.directory,
            t.test_name,
            MAX(tm.memory_peak) as max_memory_peak,
            AVG(tm.usage_sec) as mean_usage_sec,
            AVG(tm.time_taken) as mean_time_taken
        FROM
            tests t
        JOIN
            test_metrics tm ON t.id = tm.test_id
        GROUP BY
            t.mode, t.directory, t.test_name
        """

        cursor.execute(query)
        results = cursor.fetchall()

        logger.debug(f"Found {len(results)} unique test groups")

        # Build the dictionary to return
        metrics_dict = {}
        for row in results:
            mode, directory, test_name, max_memory, mean_usage, mean_time = row

            key = (mode, directory, test_name)
            metrics_dict[key] = {
                'max_memory_peak': max_memory,
                'mean_usage_sec': mean_usage,
                'mean_time_taken': mean_time
            }

            logger.debug(f"Metrics for {key}: {metrics_dict[key]}")

        conn.close()
        logger.info("Database analysis completed successfully")
        return metrics_dict

    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

def main() -> None:
    """
    Main function that parses command line arguments and calls the analysis function.
    """
    parser = argparse.ArgumentParser(description='Analyze test metrics from a SQLite database')
    parser.add_argument('db_path', help='Path to the SQLite database file')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger(__name__)
    logger.info("Starting test metrics analysis")

    try:
        results = analyze_test_metrics(args.db_path)
        for key, metrics in results.items():
            mode, directory, test_name = key
            print(f"Mode: {mode}, Directory: {directory}, Test: {test_name}")
            print(f"  Max Memory Peak: {metrics['max_memory_peak']} bytes")
            print(f"  Mean Usage Sec: {metrics['mean_usage_sec']:.4f} seconds")
            print(f"  Mean Time Taken: {metrics['mean_time_taken']:.4f} seconds")
            print()
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()

# ```

# This script:
# 1. Defines a function to analyze test metrics using SQL for aggregation
# 2. Adds debug logging throughout the process
# 3. Includes a main function with argparse for handling the filename and --verbose flag
# 4. Uses proper typing hints
# 5. Performs the aggregation directly in SQL for better efficiency
# 6. Formats and prints the results in a readable way#

# ```python
# #!/usr/bin/env python3

# import argparse
# import logging
# import sqlite3
# from typing import Dict, List, Tuple, Any
# import sys

# def analyze_test_metrics(db_path: str, verbose: bool = False) -> Dict[Tuple[str, str, str], Dict[str, float]]:
#     """
#     Analyze test metrics from a SQLite database.

#     Args:
#         db_path: Path to SQLite database file
#         verbose: Whether to enable verbose logging

#     Returns:
#         Dictionary with (mode, directory, test_name) as keys and
#         metrics (max_memory_peak, mean_usage_sec, avg_time_taken) as values
#     """
#     # Set up logging
#     log_level = logging.DEBUG if verbose else logging.INFO
#     logging.basicConfig(
#         level=log_level,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     )
#     logger = logging.getLogger(__name__)

#     logger.debug(f"Opening database at {db_path}")

#     results = {}
#     try:
#         with sqlite3.connect(db_path) as conn:
#             cursor = conn.cursor()

#             # Using SQL aggregation functions for better performance
#             query = """
#             SELECT
#                 t.mode,
#                 t.directory,
#                 t.test_name,
#                 MAX(tm.memory_peak) as max_memory_peak,
#                 AVG(tm.usage_sec) as mean_usage_sec,
#                 AVG(tm.time_taken) as avg_time_taken
#             FROM tests t
#             JOIN test_metrics tm ON t.id = tm.test_id
#             GROUP BY t.mode, t.directory, t.test_name
#             """

#             logger.debug(f"Executing query: {query}")
#             cursor.execute(query)

#             for row in cursor.fetchall():
#                 mode, directory, test_name, max_memory, mean_usage, avg_time = row
#                 key = (mode, directory, test_name)
#                 results[key] = {
#                     'max_memory_peak': max_memory,
#                     'mean_usage_sec': mean_usage,
#                     'avg_time_taken': avg_time
#                 }
#                 logger.debug(f"Processed metrics for {key}: {results[key]}")

#             logger.debug(f"Found {len(results)} distinct test configurations")

#     except sqlite3.Error as e:
#         logger.error(f"Database error: {e}")
#         raise
#     except Exception as e:
#         logger.error(f"Unexpected error: {e}")
#         raise

#     return results

# def main() -> None:
#     parser = argparse.ArgumentParser(description='Analyze test metrics from SQLite database')
#     parser.add_argument('db_path', help='Path to SQLite database file')
#     parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')

#     args = parser.parse_args()

#     try:
#         results = analyze_test_metrics(args.db_path, args.verbose)
#         print("Results:")
#         for key, metrics in results.items():
#             mode, directory, test_name = key
#             print(f"Mode: {mode}, Directory: {directory}, Test: {test_name}")
#             print(f"  Max Memory Peak: {metrics['max_memory_peak']} bytes")
#             print(f"  Mean Usage Sec: {metrics['mean_usage_sec']:.6f} seconds")
#             print(f"  Avg Time Taken: {metrics['avg_time_taken']:.6f} seconds")
#             print()
#     except Exception as e:
#         print(f"Error: {e}", file=sys.stderr)
#         sys.exit(1)

# if __name__ == "__main__":
#     main()
# ```

# # BREAK

# import argparse
# import logging
# import sqlite3
# from collections import defaultdict

# logger = logging.getLogger(__name__)

# def compute_metrics(db_path):
#     """
#     Compute max memory_peak and mean usage_sec for each (mode,directory,test_name) triple.

#     Args:
#         db_path: Path to the SQLite database file

#     Returns:
#         Dictionary with (mode,directory,test_name) keys and metrics as values
#     """
#     logger.debug(f"Opening database at {db_path}")
#     result = {}

#     try:
#         conn = sqlite3.connect(db_path)
#         cursor = conn.cursor()

#         logger.debug("Executing SQL query to join tests and metrics tables")
#         query = """
#         SELECT t.mode, t.directory, t.test_name, tm.memory_peak, tm.usage_sec
#         FROM tests t
#         JOIN test_metrics tm ON t.id = tm.test_id
#         """

#         cursor.execute(query)
#         rows = cursor.fetchall()

#         logger.debug(f"Retrieved {len(rows)} rows from database")

#         # Group metrics by (mode, directory, test_name)
#         metrics_by_group = defaultdict(list)
#         for mode, directory, test_name, memory_peak, usage_sec in rows:
#             key = (mode, directory, test_name)
#             metrics_by_group[key].append((memory_peak, usage_sec))
#             logger.debug(f"Added metrics for {key}: memory_peak={memory_peak}, usage_sec={usage_sec}")

#         # Compute stats for each group
#         for key, metrics in metrics_by_group.items():
#             memory_peaks = [m[0] for m in metrics]
#             usage_secs = [m[1] for m in metrics]

#             max_memory = max(memory_peaks)
#             mean_usage = sum(usage_secs) / len(usage_secs)

#             result[key] = {
#                 'max_memory_peak': max_memory,
#                 'mean_usage_sec': mean_usage
#             }

#             logger.debug(f"Computed metrics for {key}: max_memory_peak={max_memory}, mean_usage_sec={mean_usage}")

#         conn.close()
#         logger.debug("Database connection closed")

#     except Exception as e:
#         logger.error(f"Error processing database: {str(e)}")
#         raise

#     return result

# def main():
#     parser = argparse.ArgumentParser(description='Compute test metrics from SQLite database')
#     parser.add_argument('db_path', help='Path to the SQLite database file')
#     parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')

#     args = parser.parse_args()

#     if args.verbose:
#         # Set up logging
#         log_level = logging.DEBUG if verbose else logging.INFO
#         logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

#     result = compute_metrics(args.db_path)
#     print(result)

# if __name__ == "__main__":
#     main()
