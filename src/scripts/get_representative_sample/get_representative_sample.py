import argparse
import json
import synapseclient as sc


def read_args():
    parser = argparse.ArgumentParser(
            description=("Construct a SQL query string for use with "
                         "the backfill_json_datasets script which "
                         "selects one representative record for each "
                         "appVersion which includes this --dataset and "
                         "--dataset-version"))
    parser.add_argument("--dataset-mapping",
                        help="The path to the dataset mapping file.")
    parser.add_argument("--dataset",
                        help="The name of the dataset.")
    parser.add_argument("--dataset-version",
                        help="The version of the dataset, formatted 'v*'.")
    parser.add_argument("--file-view",
                        help=("The Synapse ID of a file view containing "
                              "files to be sampled."))
    args = parser.parse_args()
    return args


def find_app_versions(dataset_mapping, dataset, dataset_version):
    relevant_app_versions = []
    for app_version in dataset_mapping["appVersion"]:
        these_datasets = dataset_mapping["appVersion"][app_version]["dataset"]
        if dataset in these_datasets and these_datasets[dataset] == dataset_version:
            relevant_app_versions.append(app_version)
    return relevant_app_versions


def sample_app_versions(syn, relevant_app_versions, file_view):
    app_version_str = "('" + "','".join(relevant_app_versions) + "')"
    query_str = (f"SELECT * FROM {file_view} "
                 "WHERE appVersion in ") + app_version_str
    app_version_q = syn.tableQuery(query_str)
    app_version_df = app_version_q.asDataFrame()
    app_version_df_sample = (app_version_df
            .sample(len(app_version_df))
            .drop_duplicates(subset="appVersion"))
    sampled_record_ids = app_version_df_sample["recordId"].values
    return sampled_record_ids


def main():
    args = read_args()
    with open(args.dataset_mapping, "r") as f:
        dataset_mapping = json.load(f)
    syn = sc.login()
    relevant_app_versions = find_app_versions(
            dataset_mapping=dataset_mapping,
            dataset=args.dataset,
            dataset_version=args.dataset_version)
    sampled_record_ids = sample_app_versions(
            syn=syn,
            relevant_app_versions=relevant_app_versions,
            file_view=args.file_view)
    record_id_str = "('" + "','".join(sampled_record_ids) + "')"
    query_str = ("SELECT * FROM {source_table} "
                 "WHERE recordId in ") + record_id_str
    print(query_str)
    return query_str


if __name__ == "__main__":
    main()
