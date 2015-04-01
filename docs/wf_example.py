from pbsmrtpipe.pb_pipelines import register_pipeline


@register_pipeline("MyWorkflowId")
def my_workflow():
    # return a list of bindings + explicit Entry points
    # this could also be serialized to XML or JSON.
    # For purposes of discussion, this workflow exposes
    # two entry points by ids 'entry_01' (movie fofn) and 'entry_02' (reference)
    return ""
