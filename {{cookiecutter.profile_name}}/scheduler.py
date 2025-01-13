#!/usr/bin/env python3
import os, sys
import logging, traceback

logging.basicConfig(
    level=logging.INFO,
    format="CLUSTER: %(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logging.error(
        "".join(
            [
                "Uncaught exception: ",
                *traceback.format_exception(exc_type, exc_value, exc_traceback),
            ]
        )
    )


# Install exception handler
sys.excepthook = handle_exception

## Beginning of the script

from subprocess import Popen, PIPE
import yaml


os.makedirs("cluster_log", exist_ok=True)


# let snakemake read job_properties
from snakemake.utils import read_job_properties

wrapper_directory = os.path.dirname(__file__)

jobscript = sys.argv[1]
job_properties = read_job_properties(jobscript)

# default paramters defined in cluster_spec (accessed via snakemake read_job_properties)
cluster_param = job_properties["cluster"]

if job_properties["type"] == "single":
    cluster_param["name"] = job_properties["rule"]
elif job_properties["type"] == "group":
    cluster_param["name"] = job_properties["groupid"]
else:
    raise NotImplementedError(
        f"Don't know what to do with job_properties['type']=={job_properties['type']}"
    )


# don't overwrite default parameters if defined in rule (or config file)
if ("threads" in job_properties) and ("threads" not in cluster_param):
    # logging
    logging.info(f"threads not in cluster_param, set to snakemake rule setting: [{job_properties['threads']}]")
    cluster_param["threads"] = job_properties["threads"]

for res in ["time_min", "mem_mb"]:
    if (res in job_properties["resources"]) and (res not in cluster_param):
        # logging
        logging.info(f"Resource {res} not in cluster_param, set to snakemake rule setting: [{job_properties['resources'][res]}]")
        cluster_param[res] = job_properties["resources"][res]

## Definie queue
queue_table_file = os.path.join(wrapper_directory, "queues.tsv")
if ("queue" not in cluster_param) and os.path.exists(queue_table_file):

    logging.info("Automatically choose best queue to submit")

    # load pandas only now to parse table
    import pandas as pd

    queue_table = pd.read_table(queue_table_file, index_col=0, comment="#")

    logging.debug(f"Parameters to constrain queues:\n {cluster_param}")

    # for each parameter in queu_params and table subset the queue_table
    for param in cluster_param:
        if param in queue_table.columns:

            logging.debug(f"Queue_table:\n{queue_table}")

            filter_criteria = f"{param} >= {cluster_param[param]}"
            logging.debug(f"Filter table with'{filter_criteria}'")
            queue_table = queue_table.query(filter_criteria)

    if queue_table.shape[0] == 0:
        logging.error(
            "No queue corresponding to the rule parameters are found. \n"
            "You need to adapt the snakemake resources!\n"
            f"Parameters to constrain queues:\n {cluster_param}\n"
            f"queue_table: {queue_table_file}\n"
        )
        exit(1)
    else:
        # choose queue with highest priority
        if ("priority" not in queue_table.columns) & (queue_table.shape[0] > 1):

            logging.info(
                f"'priority' not in queue table. I don't know how to prioritize. I choose the first."
            )

        else:
            # sort queue ascending by priority
            queue_table.sort_values("priority", inplace=True)

        cluster_param["queue"] = queue_table.index[0]
        logging.info(f"Choose queue: {cluster_param['queue']}")


# check which system you are on and load command command_options
key_mapping_file = os.path.join(wrapper_directory, "key_mapping.yaml")
command_options = yaml.load(open(key_mapping_file), Loader=yaml.BaseLoader)
system = command_options["system"]
command = command_options[system]["command"]

key_mapping = command_options[system]["key_mapping"]

def get_log_directive(job_properties, cluster_system):
    """
    Get log directive for cluster system
    
    Args:
        job_properties: job properties
        cluster_system: cluster system
    
    Returns:
        log_directive: log directive parameter for cluster system
    """
    # get system config
    system_config = command_options[cluster_system]
    # get log mode, it defined in key_mapping.yaml header
    log_mode = command_options.get("log_mode", "default")
    
    if log_mode == "defined" and "log" in job_properties:
        # Use rule defined log path
        log_files = job_properties["log"]
        if isinstance(log_files, list):
            # Multiple log files
            if len(log_files) >= 2:
                return system_config["log_defined"]["split"].format(
                    stdout=log_files[0],
                    stderr=log_files[1]
                )
            else:
                return system_config["log_defined"]["single"].format(
                    log=log_files[0]
                )
        else:
            # Single log file
            return system_config["log_defined"]["single"].format(
                log=log_files
            )
    elif log_mode == "default":
        # Use default log path
        os.makedirs("cluster_log", exist_ok=True)
        return system_config["log_default"]
    elif log_mode == "defined" and "log" not in job_properties:
        # Use default log path
        # TODO: Based on job_name to generate log path
        logging.warning("log not defined in job_properties, use default log path")
        os.makedirs("cluster_log", exist_ok=True)
        return system_config["log_default"]
    else:
        raise ValueError(f"Invalid log mode: {log_mode}")

# Add log directive to command
log_directive = get_log_directive(job_properties, system)
command = command_options[system]["command"].format(log_directive=log_directive)

# Add other parameters
for key in cluster_param:
    if key not in key_mapping:
        logging.warning(
            f"parameter '{key}' not in keymapping! It would be better if you add the key to the file: {key_mapping_file} \n I try without the key!"
        )
    else:
        command += " "
        command += key_mapping[key].format(cluster_param[key])

command += " {}".format(jobscript)

logging.info("submit command: " + command)

p = Popen(command.split(), stdout=PIPE, stderr=PIPE)
output, error = p.communicate()
if p.returncode != 0:


    error_message = "Job can't be submitted\n" + output.decode("utf-8") + error.decode("utf-8")

    logging.error(error_message)

    #give it to snakemake
    print(error_message)


    exit(p.returncode)
else:
    res = output.decode("utf-8")

    if system == "lsf":
        import re

        match = re.search(r"Job <(\d+)> is submitted", res)
        jobid = match.group(1)

    elif system == "pbs" or system == "pbspro":
        jobid = res.strip().split(".")[0]

    else:
        jobid = int(res.strip().split()[-1])

    print(jobid)
