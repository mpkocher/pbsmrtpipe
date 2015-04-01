def add_chunked_bindings(task_id, bs, nchunks):
    """Add Chunked bindings for Filtering """
    b = []
    for out, in_ in bs:
        # Re-Route the fundamental outputs of the fundamental task
        if out == 'pbsmrtpipe.tasks.filter.0':
            b.append(("pbsmrtpipe.tasks.gather_fofn.0", in_))
        elif out == 'pbsmrtpipe.tasks.filter.1':
            b.append(("pbsmrtpipe.tasks.gather_csv.0", in_))
        elif (out.startswith(task_id + '.') or in_.startswith(task_id + '.')):
            # remove original task
            pass
        else:
            b.append((out, in_))

    b.append(("pbsmrtpipe.tasks.input_xml_to_fofn.0", "pbsmrtpipe.tasks.scatter_movie_fofn.0"))

    for i in xrange(nchunks):
        b.append(("pbsmrtpipe.tasks.scatter_movie_fofn.{i}".format(i=i), "{x}.{i}.0".format(i=i, x=task_id)))
        b.append(("{x}.{i}.1".format(i=i, x=task_id), "pbsmrtpipe.tasks.gather_csv.{i}".format(i=i)))
        b.append(("{x}.{i}.0".format(i=i, x=task_id), "pbsmrtpipe.tasks.gather_fofn.{i}".format(i=i)))

    return b
