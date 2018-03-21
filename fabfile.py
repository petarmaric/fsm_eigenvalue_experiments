import fnmatch
import os
import shutil

from fabric.api import local, task
import tables as tb


ANALYSES_TYPES = {
    'fsm_modal_analysis': {
        'program_args_fmt': "%(results_file)s --report_file %(report_file)s",
        'report_file_ext': 'pdf',
        'variations': {
            '*/*.hdf5': [
                {},
            ],
            'barbero/*.hdf5': [
                {'a-max': 1000.0,},
                {'a-max': 1500.0,},
                {'t_b-min': 4.0, 't_b-max': 8.0,},
                {'t_b-min': 4.0, 't_b-max': 8.0, 'a-min': 2000.0,},
                {'t_b-min': 4.0, 't_b-max': 7.0, 'a-min': 200.0, 'a-max': 800.0,},
            ],
        },
    },
    'fsm_strip_length_analysis': {
        'program_args_fmt': "%(results_file)s --report_file %(report_file)s",
        'report_file_ext': 'pdf',
        'variations': {
            'barbero/*.hdf5': [
                {'t_b': 6.35, 'add-automatic-markers': '',},
            ],
        },
    },
    'fsm_strip_thickness_analysis': {
        'program_args_fmt': "%(results_file)s --report_file %(report_file)s",
        'report_file_ext': 'pdf',
        'variations': {
            'barbero/*.hdf5': [
                {'a': 2310, 'add-automatic-markers': '',},
            ],
        },
    },
}


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


@task
def clean_all_analyses_reports(top_dir='.', force_analysis_type=''):
    results_dirs = set(os.path.dirname(results_file) for results_file in find_filenames(top_dir, '*.hdf5'))
    for results_dir in results_dirs:
        for analysis_type in ANALYSES_TYPES:
            reports_dir = os.path.join(results_dir, analysis_type)
            if os.path.exists(reports_dir):
                print "Deleting the '%s' analyses reports directory..." % reports_dir
                shutil.rmtree(reports_dir)


@task
def run_single_analysis_type(results_file, analysis_type):
    reports_dir = os.path.join(os.path.dirname(results_file), analysis_type)
    reports_base_name = os.path.join(reports_dir, os.path.splitext(os.path.basename(results_file))[0])

    if not os.path.exists(reports_dir):
        os.mkdir(reports_dir)

    analysis_settings = ANALYSES_TYPES[analysis_type]
    for path_pattern, variation_group in analysis_settings['variations'].items():
        if not fnmatch.fnmatch(results_file, '*/' + path_pattern):
            continue

        for variation_dict in variation_group:
            sorted_variation_items = sorted(variation_dict.items())

            variation_filename_part = ','.join("%s=%s" % (k, v) for k, v in sorted_variation_items)
            report_file = "%s%s.%s" % (
                reports_base_name,
                '@' + variation_filename_part if variation_filename_part else '',
                analysis_settings['report_file_ext']
            )

            program_args = analysis_settings['program_args_fmt'] % locals()
            variation_program_args = ' '.join("--%s %s" % (k, v) for k, v in sorted_variation_items)
            local("%s %s %s" % (analysis_type, program_args, variation_program_args))

@task
def run_analyses(results_file, force_analysis_type=''):
    analyses_types = ANALYSES_TYPES if not force_analysis_type else (force_analysis_type,)
    for analysis_type in analyses_types:
        run_single_analysis_type(results_file, analysis_type)

@task
def run_analyses_on_all_experiments(top_dir='.', force_analysis_type=''):
    for results_file in find_filenames(top_dir, '*.hdf5'):
        run_analyses(results_file, force_analysis_type=force_analysis_type)
