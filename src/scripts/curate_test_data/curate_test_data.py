'''
This script accepts a synapse table query and overwrites the contents of
the test data folder with the records matched by the query. The table being
queried is assumed to be a file view scoped over a "Raw Bridge Data" folder
which has all annotations included in its schema. A metadata manifest is
written in JSON format to the working directory. This is meant to be used
with setup_test_data.py.
'''

import os
import shutil
import argparse
import synapseclient

# Exclude these Synapse file entity metadata.
# Anything that is not in this list is assumed to be a file annotation.
EXCLUDE_FIELDS = [
        "id", "name", "createdOn", "createdBy", "etag", "type", "currentVersion",
        "parentId", "benefactorId", "projectId", "modifiedOn", "modifiedBy",
        "dataFileHandleId", "dataFileSizeBytes", "dataFileMD5Hex"]

def read_args():
    parser = argparse.ArgumentParser(
            description=(""))
    parser.add_argument("--file-view",
                        help="Synapse ID of the table to query.")
    parser.add_argument("--query",
                        help="The table query to run against `--file-view`. "
                             "Use {source_table} in the query's FROM clause.")
    parser.add_argument("--data-dir",
                        help="The local path to the test data directory.")
    parser.add_argument("--overwrite",
                        action="store_true",
                        help=("If omitted, move new data to `--data-dir` "
                              "without affecting existing data. If included, "
                              "deletes existing test data before moving new "
                              "data."))
    args = parser.parse_args()
    return args

def create_metadata_manifest(table):
    table = table.drop(EXCLUDE_FIELDS, axis=1)
    metadata = table.set_index("recordId", drop=False)
    metadata.to_json("metadata.json", orient="index")
    return metadata

def main():
    args = read_args()
    syn = synapseclient.login()
    table = syn.tableQuery(
            args.query.format(source_table=args.file_view)).asDataFrame()
    new_test_data = table.id.apply(syn.get).values
    create_metadata_manifest(table=table)
    if args.overwrite:
        for f in os.listdir(args.data_dir):
            os.remove(os.path.join(args.data_dir, f))
    for file in new_test_data:
        record_id = file["recordId"][0]
        dest_path = os.path.join(args.data_dir, f"{record_id}-raw.zip")
        shutil.copy2(file.path, dest_path)



if __name__ == "__main__":
    main()
