import fnmatch
import os

from fabric.api import local, task
import tables as tb


def find_filenames(top_dir, filename_pattern):
    for path, _, file_list in os.walk(top_dir):
        for name in fnmatch.filter(file_list, filename_pattern):
            yield os.path.join(path, name)

def is_experiment_computed(results_file):
    try:
        return tb.is_hdf5_file(results_file)
    except (IOError, tb.HDF5ExtError):
        return False

@task
def compute_experiment(data_file, force=False):
    base_name = os.path.splitext(data_file)[0]
    log_file = base_name + '.log'
    results_file = base_name + '.hdf5'

    if force or not is_experiment_computed(results_file):
        local(
            "fsm_eigenvalue %s --results-file %s |& tee %s" % (data_file, results_file, log_file),
            shell='bash',
        )
    else:
        print "Experiment '%s' has already been computed, skipping." % data_file

@task
def compute_all_experiments(top_dir='.', force=False):
    for data_file in find_filenames(top_dir, '*.yaml'):
        compute_experiment(data_file, force=force)
