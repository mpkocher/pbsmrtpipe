def my_nproc_func(nproc, resolved_opts, my_report_attribute_id):
    return nproc / 2

di_list_model = ['$max_nproc', '$ropts', '$inputs.0.attr_x', my_nproc_func]
