import fnmatch
import os
import shutil

from fabric.api import local, task
import tables as tb


ANALYSES_TYPES = {
    'fsm_damage_analysis': {
        'program_args_fmt': "%(results_file)s --report_file %(report_file)s",
        'report_file_ext': 'pdf',
        'variations': {
            '*/*.hdf5': [
                {},
            ],
        },
    },
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
                {'t_b': 6.35, 'markers': 2310.00,},
                {'t_b': 6.35, 'add-automatic-markers': '',},
                {'t_b': 6.35, 'add-automatic-markers': '', 'a-min': 195.00, 'a-max': 198.00,}, # mode 2 to 3
                {'t_b': 6.35, 'add-automatic-markers': '', 'a-min': 276.00, 'a-max': 280.00,}, # mode 3 to 4
                {'t_b': 6.35, 'add-automatic-markers': '', 'a-min': 356.00, 'a-max': 362.00,}, # mode 4 to 5
                {'t_b': 6.35, 'add-automatic-markers': '', 'a-min': 436.00, 'a-max': 443.00,}, # mode 5 to 6
                {'t_b': 6.35, 'add-automatic-markers': '', 'a-min': 516.00, 'a-max': 524.00,}, # mode 6 to 7
                {'t_b': 6.35, 'add-automatic-markers': '', 'a-min': 596.00, 'a-max': 605.00,}, # mode 7 to 8
                {'t_b': 6.35, 'add-automatic-markers': '', 'a-min': 676.00, 'a-max': 685.00,}, # mode 8 to 9
                {'t_b': 6.35, 'add-automatic-markers': '', 'a-min': 756.00, 'a-max': 766.00,}, # mode 9 to 10
                {'t_b': 6.35, 'add-automatic-markers': '', 'a-min': 835.00, 'a-max': 847.00,}, # mode 10 to 11
                {'t_b': 6.35, 'add-automatic-markers': '', 'a-min': 900.00, 'a-max': 970.00,}, # mode 11 to 1
            ],
            'barbero_mode-transitions/*.hdf5': [
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
            'barbero/barbero-elastic.hdf5': [
                {'a': 198.0, 'add-automatic-markers': '',}, # mode  2 to  3, as per 'barbero' elastic model
                {'a': 279.5, 'add-automatic-markers': '',}, # mode  3 to  4, as per 'barbero' elastic model
                {'a': 361.0, 'add-automatic-markers': '',}, # mode  4 to  5, as per 'barbero' elastic model
                {'a': 442.0, 'add-automatic-markers': '',}, # mode  5 to  6, as per 'barbero' elastic model
                {'a': 523.0, 'add-automatic-markers': '',}, # mode  6 to  7, as per 'barbero' elastic model
                {'a': 603.5, 'add-automatic-markers': '',}, # mode  7 to  8, as per 'barbero' elastic model
                {'a': 684.5, 'add-automatic-markers': '',}, # mode  8 to  9, as per 'barbero' elastic model
                {'a': 765.0, 'add-automatic-markers': '',}, # mode  9 to 10, as per 'barbero' elastic model
                {'a': 846.0, 'add-automatic-markers': '',}, # mode 10 to 11, as per 'barbero' elastic model
                {'a': 968.5, 'add-automatic-markers': '',}, # mode 11 to  1, as per 'barbero' elastic model
            ],
            'barbero/barbero-viscoelastic.hdf5': [
                {'a': 196.0, 'add-automatic-markers': '',}, # mode  2 to  3, as per 'barbero' viscoelastic model
                {'a': 276.5, 'add-automatic-markers': '',}, # mode  3 to  4, as per 'barbero' viscoelastic model
                {'a': 357.0, 'add-automatic-markers': '',}, # mode  4 to  5, as per 'barbero' viscoelastic model
                {'a': 437.5, 'add-automatic-markers': '',}, # mode  5 to  6, as per 'barbero' viscoelastic model
                {'a': 517.5, 'add-automatic-markers': '',}, # mode  6 to  7, as per 'barbero' viscoelastic model
                {'a': 597.5, 'add-automatic-markers': '',}, # mode  7 to  8, as per 'barbero' viscoelastic model
                {'a': 677.5, 'add-automatic-markers': '',}, # mode  8 to  9, as per 'barbero' viscoelastic model
                {'a': 757.0, 'add-automatic-markers': '',}, # mode  9 to 10, as per 'barbero' viscoelastic model
                {'a': 837.0, 'add-automatic-markers': '',}, # mode 10 to 11, as per 'barbero' viscoelastic model
                {'a': 903.0, 'add-automatic-markers': '',}, # mode 11 to  1, as per 'barbero' viscoelastic model
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
