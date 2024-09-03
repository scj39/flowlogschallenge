import argparse
import asyncio
import concurrent.futures as cf
import csv
import os
from collections import defaultdict
from functools import partial, reduce

from constants import int2protocolmap

"""
A script that consumes a flowlog and a lookuptable csv and 
produces two output files. Uses a simple version of MapReduce 
and argparse for parsing. Uses no dependencies external 
to the standard library.

Sample invocation:
python processflows.py -f flowlogs -l lookuptable.csv
"""

parser_metadata = {
    "prog": "scj39's Flow Parser",
    "description": "Parses v2 flow log files",
}
parser = argparse.ArgumentParser(**parser_metadata)
parser.add_argument("--filename", "-f", help="What file you want to process.", type=str)
parser.add_argument(
    "--lookup", "-l", help="The lookup table you'll use to do the processing.", type=str
)
parser.add_argument(
    "--chunk_size",
    "-c",
    help="How large each chunk should be when reading the file.",
    type=int,
    default=100,
)
args = parser.parse_args()

"""
Utils
"""


def write_to_csv(data: defaultdict, foldername: str, name: str, columns: list[str]):
    """
    Writes dictionaries to disk using the built-in csv writer
    """
    os.makedirs(os.path.dirname(f"./{foldername}/{name}.csv"), exist_ok=True)
    with open(f"output/{name}.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for key, value in data.items():
            (
                writer.writerow([*key, value])
                if isinstance(key, tuple)
                else writer.writerow([key, value])
            )


def merge(left: defaultdict, right: defaultdict) -> defaultdict:
    """
    Merges two default dicts together by modifying the leftmost dictionary
    """
    for key in right:
        left[key] += right[key]
    return left


def chunk(logs: list, chunk_size: int) -> list:
    """
    Breaks up a list into chunks. Chunk size should be greater than 0
    """
    logs_length = len(logs)
    for index in range(0, logs_length, chunk_size):
        yield logs[index : index + chunk_size]


def get_tag_count(
    protocolportcount: defaultdict, tag2protocolport: defaultdict
) -> defaultdict:
    """
    Returns a defaultdict with the counts associated with each tag
    """
    totalentries = sum(protocolportcount.values())
    tagged_entries = 0
    tag2count: defaultdict = defaultdict(int)
    for tag, array in tag2protocolport.items():
        for protocolport in array:
            if occurences := protocolportcount.get(protocolport, 0):
                tag2count[tag] += occurences
                tagged_entries += occurences

    if untagged_count := totalentries - tagged_entries:
        tag2count["untagged"] = untagged_count

    # everything we see that doesn't have an entry in the lookup table
    # is labeled untagged.
    return tag2count


"""
File Processing Preferences
"""


class LogPrefs:
    """
    Encapsulates logic specific to processing files
    of a certain type
    """

    def process_line(self, line: str) -> list:
        return line.strip().split(" ")

    def process_entry(self, chunk: list[str]) -> dict:
        frequency: defaultdict = defaultdict(int)
        for line in chunk:
            processed_line = self.process_line(line)
            (
                version,
                account_id,
                interface_id,
                srcaddr,
                dstaddr,
                srcport,
                dstport,
                protocol,
                packets,
                size,
                start,
                end,
                action,
                log_status,
            ) = processed_line
            frequency[(dstport, int2protocolmap[int(protocol)])] += 1
        return frequency


class LookupPrefs(LogPrefs):
    def process_line(self, line: str) -> list:
        return line.strip().split(",")

    def process_entry(self, chunk: list[str]) -> dict:
        frequency = defaultdict(list)
        for line in chunk:
            processed_line = self.process_line(line)
            (dest_port, protocol, tag) = processed_line
            # exclude header
            if tag != "tag":
                frequency[tag.lower()].append((dest_port, protocol))
        return frequency


"""
MapReduce
"""


async def process_file(filename: str, chunk_size: int, prefs) -> defaultdict:
    """
    Asynchronously processes mappings on each chunk.
    """
    with open(filename, encoding="utf-8") as f:
        logs = f.readlines()
        routines = []
        loop = asyncio.get_running_loop()
        with cf.ProcessPoolExecutor() as pool:
            for selection in chunk(logs, chunk_size):
                routines.append(
                    loop.run_in_executor(pool, partial(prefs.process_entry, selection))
                )

            mapped_dictionaries = await asyncio.gather(*routines)
            consolidated_dictionary: defaultdict = reduce(merge, mapped_dictionaries)
        return consolidated_dictionary


async def main(filename, lookup, chunk_size) -> None:
    processed_logs = await process_file(filename, chunk_size, prefs=LogPrefs())
    lookup_table = await process_file(lookup, chunk_size, prefs=LookupPrefs())
    tag2count = get_tag_count(processed_logs, lookup_table)
    print("Writing derived files to disk...")
    write_to_csv(
        processed_logs,
        name="portprotocolcount",
        foldername="output",
        columns=["Port", "Protocol", "Count"],
    )
    write_to_csv(
        tag2count, name="tag2count", foldername="output", columns=["Tag", "Count"]
    )
    print(f"Generated files may be found in the output folder.")


"""
Main
"""
if __name__ == "__main__":
    print("Processing flow logs...")
    filename, lookup, chunk_size = args.filename, args.lookup, args.chunk_size
    try:
        asyncio.run(main(filename, lookup, chunk_size))
    except FileNotFoundError:
        print(
            "Please double-check that the files you specified actually exist in the current directory."
        )
